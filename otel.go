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

// Границы бакетов гистограмм длительности, в секундах.
//
// Задавать их явно обязательно. Умолчание OTel SDK —
// [0 5 10 25 50 75 100 250 500 750 1000 2500 5000 7500 10000] — подобрано под
// миллисекунды, а обе гистограммы пакета объявлены с WithUnit("s"): с ним
// вообще всё, кроме десятисекундного хвоста, ложится в первый бакет, и
// гистограмма перестаёт отвечать на единственный вопрос, ради которого
// заводится, — какова доля запросов быстрее X.
//
// Обе сетки логарифмические с шагом ~2.5×: на такой сетке относительная
// погрешность интерполированного квантиля примерно одинакова по всему
// диапазону. Число бакетов держится в районе полутора десятков — каждый бакет
// это отдельный временной ряд на инструмент, топик и статус.
var (
	// producerDurationBuckets покрывают SendMessage: от локального брокера
	// (десятки микросекунд) до Producer.MessageTimeout, чьё умолчание — 30s.
	// Верхняя граница совпадает с ним не случайно: превышение бюджета видно
	// как переполнение последнего бакета, а не теряется в +Inf.
	producerDurationBuckets = []float64{
		0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30,
	}

	// consumerDurationBuckets покрывают обработку целиком, вместе с повторами и
	// паузами между ними, поэтому хвост длиннее продюсерского: при
	// HandlerRetryDelay=1s и десятке повторов честная длительность — десятки
	// секунд, и она не должна сливаться с «зависло навсегда».
	consumerDurationBuckets = []float64{
		0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300,
	}
)

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
