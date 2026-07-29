package kafkax

import (
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"
	"go.opentelemetry.io/otel"
)

// Имя инструментационной библиотеки — общий scope для трейсов и метрик обеих
// ролей. Роль различается атрибутами спанов и именами метрик, а не scope'ом:
// приложение, создающее и продюсера, и консьюмера, не должно видеть два
// разных instrumentation scope для одной библиотеки.
const instrumentationName = "github.com/alfzs/kafkax/v2"

// instrumentRegistry накапливает ошибки регистрации инструментов OTel.
//
// Игнорировать их нельзя: metric.Meter не обещает вернуть работоспособный
// инструмент вместе с ошибкой, и nil-инструмент упал бы паникой на горячем
// пути, а не в конструкторе. Собираются все разом, чтобы не чинить по одному
// за перезапуск.
type instrumentRegistry struct {
	errs []error
}

// record возвращает инструмент и запоминает ошибку, если она была.
func record[T any](r *instrumentRegistry, name string, inst T, err error) T {
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("registering %s: %w", name, err))
	}

	return inst
}

func (r *instrumentRegistry) err() error {
	return errors.Join(r.errs...)
}

// telemetry — трейсер kotel и хуки, которые его питают.
//
// Ни перенос контекста через заголовки, ни имена спанов не пишутся здесь
// вручную: kotel делает и то, и другое по семантическим соглашениям OTel и
// обновляется вместе с ними.
type telemetry struct {
	tracer *kotel.Tracer
	hooks  []kgo.Hook
}

// newTelemetry настраивает kotel для одной роли.
//
// group передаётся только консьюмером: kotel добавляет messaging.kafka.
// consumer.group в спаны receive/process, и для продюсера это поле бессмысленно.
func newTelemetry(clientID, group string) telemetry {
	tracerOpts := []kotel.TracerOpt{
		kotel.TracerProvider(otel.GetTracerProvider()),
		kotel.TracerPropagator(otel.GetTextMapPropagator()),
		kotel.ClientID(clientID),
	}

	if group != "" {
		tracerOpts = append(tracerOpts, kotel.ConsumerGroup(group))
	}

	tracer := kotel.NewTracer(tracerOpts...)
	meter := kotel.NewMeter(kotel.MeterProvider(otel.GetMeterProvider()))

	return telemetry{
		tracer: tracer,
		hooks:  kotel.NewKotel(kotel.WithTracer(tracer), kotel.WithMeter(meter)).Hooks(),
	}
}
