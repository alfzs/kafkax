package kafkax

import (
	"errors"
	"fmt"
	"maps"
	"runtime/debug"
	"sync"
	"sync/atomic"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// instrumentationName — scope доменных МЕТРИК пакета, общий для обеих ролей.
// Роль различается именами метрик, а не scope'ом: приложение, создающее и
// продюсера, и консьюмера, не должно видеть два разных instrumentation scope
// для одной библиотеки.
//
// Спаны сюда не относятся: их создаёт kotel под собственным scope
// github.com/twmb/franz-go/plugin/kotel, и подменить его пакет не может.
// Транспортные метрики kotel — там же.
const instrumentationName = "github.com/alfzs/kafkax/v3"

// instrumentationModule — путь модуля, по которому в build info ищется версия
// пакета для scope метрик.
const instrumentationModule = "github.com/alfzs/kafkax/v3"

// meterOptions собирает опции scope доменных метрик.
//
// Версия читается из build info, а не хардкодится константой: константу забыли
// бы обновить на первом же релизе, и scope врал бы уверенно. Отсутствие версии
// (сборка без модуля, go run по файлам) не ошибка — scope просто останется без
// неё, как было до этой функции.
//
// WithSchemaURL здесь намеренно нет. Он объявляет соответствие конкретной
// версии семантических соглашений, а доменные метрики пакета от messaging
// semantic conventions осознанно отклоняются (topic/status вместо
// messaging.destination.name — см. «Что осознанно не делаем» в
// docs/audit/03-observability.md). Схемо-осведомлённый backend, поверив
// объявлению, начал бы переименовывать атрибуты по правилам чужой схемы.
func meterOptions() []metric.MeterOption {
	return meterOptionsFor(moduleVersion(debug.ReadBuildInfo()))
}

// meterOptionsFor — тело meterOptions при уже известной версии.
//
// Чтение build info вынесено в вызывающего не ради красоты: build info
// тестового бинаря принадлежит модулю пакета и всегда непустое, так что обе
// ветки — «версия есть» и «версии нет» — из теста недостижимы, пока
// debug.ReadBuildInfo зашит внутрь. Проверить на них нечего, и потеря ветки с
// пустой строкой прошла бы молча: scope метрик уехал бы в экспорт с
// instrumentation.version="".
func meterOptionsFor(version string) []metric.MeterOption {
	opts := []metric.MeterOption{}

	if version == "" {
		return opts
	}

	return append(opts, metric.WithInstrumentationVersion(version))
}

// moduleVersion достаёт версию модуля пакета из build info.
//
// info и ok — ровно то, что вернул debug.ReadBuildInfo; параметрами, а не
// вызовом внутри, по той же причине, что и у meterOptionsFor. Пакет бывает и
// главным модулем (его собственные тесты и примеры), и зависимостью
// (единственный способ, которым его видит приложение), поэтому ищется в обоих
// местах.
func moduleVersion(info *debug.BuildInfo, ok bool) string {
	if !ok {
		return ""
	}

	if info.Main.Path == instrumentationModule {
		return info.Main.Version
	}

	for _, dep := range info.Deps {
		if dep.Path == instrumentationModule {
			return dep.Version
		}
	}

	return ""
}

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

// noStatus — пустой status в ключе кэша опций: инструмент, у которого из
// атрибутов только topic (handler.retries, fetch.errors, messages.sent/failed).
const noStatus = ""

// producerOptsLimit — потолок кэша опций продюсера.
//
// У консьюмера множество топиков замкнуто зарегистрированными обработчиками, у
// продюсера же Topic приезжает в каждом PublishRequest и не ограничен ничем.
// Кэш без потолка был бы утечкой ровно того класса, о котором пакет уже
// предупреждает в метриках: приложение, подставляющее в топик пользовательский
// ввод, наливало бы карту без конца. За потолком опции собираются на месте —
// так же, как до появления кэша, то есть худший случай не хуже прежнего.
//
// 256 записей — это 128 реальных топиков на продюсер (по одной записи на
// topic-only и по одной на каждый из двух статусов дают три на топик, так что
// с запасом), десятки байт на запись. Сервис, пишущий в большее число топиков,
// метрику с атрибутом topic перегрузил задолго до этого кэша.
const producerOptsLimit = 256

// metricOpts — заранее собранные опции OTel для одного набора атрибутов.
//
// Хранятся именно слайсы, а не attribute.Set: metric.WithAttributes строит
// множество заново на каждый вызов (аллокация слайса, сортировка, дедуп,
// боксирование опции в интерфейс), а вариадик `opts ...metric.AddOption`
// аллоцирует ещё и сам себя — даже если опция уже готова. Готовый слайс,
// переданный как opts..., не стоит ничего.
//
// Одно и то же значение лежит в обоих полях: metric.WithAttributeSet
// возвращает MeasurementOption, годный и для Add, и для Record.
type metricOpts struct {
	add    []metric.AddOption
	record []metric.RecordOption
}

// optKey — набор атрибутов доменной метрики целиком: он определяется парой
// (topic, status) и ничем больше. Пустой status — см. noStatus.
type optKey struct {
	topic  string
	status string
}

func newMetricOpts(key optKey) *metricOpts {
	attrs := make([]attribute.KeyValue, 1, 2)
	attrs[0] = attribute.String("topic", key.topic)

	if key.status != noStatus {
		attrs = append(attrs, attribute.String("status", key.status))
	}

	opt := metric.WithAttributeSet(attribute.NewSet(attrs...))

	return &metricOpts{
		add:    []metric.AddOption{opt},
		record: []metric.RecordOption{opt},
	}
}

// optsCache — кэш готовых опций метрик по паре (topic, status).
//
// Карта неизменяема после публикации и читается через atomic.Pointer: путь
// сообщения не берёт ни одного замка, а редкая вставка копирует карту целиком
// под mu. Промах по прогретому кэшу на горячем пути не случается вовсе, так
// что copy-on-write платится один раз за топик и никогда — за сообщение.
type optsCache struct {
	entries atomic.Pointer[map[optKey]*metricOpts]

	// mu защищает копирование карты при вставке. Читателям не нужен: они
	// работают со снимком, который после публикации никто не меняет.
	mu sync.Mutex

	// limit — граница памяти, а не оптимизация; см. producerOptsLimit. Ноль
	// означает «расти только через warm», то есть только по явному списку
	// топиков от приложения.
	limit int
}

func newOptsCache(limit int) *optsCache {
	c := &optsCache{limit: limit}
	empty := make(map[optKey]*metricOpts)
	c.entries.Store(&empty)

	return c
}

// get возвращает опции для набора атрибутов.
func (c *optsCache) get(topic, status string) *metricOpts {
	key := optKey{topic: topic, status: status}
	if opts, ok := (*c.entries.Load())[key]; ok {
		return opts
	}

	return c.miss(key)
}

// miss обслуживает промах: добавляет ключ в кэш, если тот ещё не упёрся в
// потолок, и в любом случае возвращает годные опции.
func (c *optsCache) miss(key optKey) *metricOpts {
	c.mu.Lock()
	defer c.mu.Unlock()

	current := *c.entries.Load()

	// Ключ мог появиться, пока мы ждали мьютекс: без повторной проверки два
	// промаха подряд разложили бы в карту две разные записи для одного набора
	// атрибутов.
	if opts, ok := current[key]; ok {
		return opts
	}

	opts := newMetricOpts(key)
	if len(current) >= c.limit {
		return opts
	}

	next := make(map[optKey]*metricOpts, len(current)+1)
	maps.Copy(next, current)
	next[key] = opts
	c.entries.Store(&next)

	return opts
}

// warm заранее укладывает в кэш все наборы атрибутов топика: topic-only и по
// одному на каждый статус.
//
// Потолок здесь намеренно не действует. Прогрев вызывается из AddHandler, где
// топик приходит из кода приложения, а не из данных: сколько обработчиков
// зарегистрировано, столько записей и будет. После прогрева путь сообщения
// находит готовые опции с первой попытки и ветку вставки не исполняет вовсе.
func (c *optsCache) warm(topic string, statuses ...string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	current := *c.entries.Load()
	next := make(map[optKey]*metricOpts, len(current)+len(statuses)+1)
	maps.Copy(next, current)

	for _, status := range append([]string{noStatus}, statuses...) {
		key := optKey{topic: topic, status: status}
		if _, ok := next[key]; !ok {
			next[key] = newMetricOpts(key)
		}
	}

	c.entries.Store(&next)
}

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
	tracer := kotel.NewTracer(tracerOpts(clientID, group)...)
	meter := kotel.NewMeter(kotel.MeterProvider(otel.GetMeterProvider()))

	return telemetry{
		tracer: tracer,
		hooks:  kotel.NewKotel(kotel.WithTracer(tracer), kotel.WithMeter(meter)).Hooks(),
	}
}

// tracerOpts собирает опции трейсера kotel для одной роли.
//
// Отдельная функция ради проверяемости: у kotel.Tracer все поля неэкспортные, а
// ConsumerGroup("") записывает в поле ту же пустую строку, что там и была, —
// то есть по готовому трейсеру «опции не было» и «опция была с пустым
// значением» неразличимы, и состав списка проверяется до NewTracer.
//
// TracerProvider и TracerPropagator повторяют то, что kotel подставил бы сам:
// оба его умолчания — те же глобали OTel, прочитанные в тот же момент. Опции
// оставлены явными, потому что зависимость от глобального состояния лучше
// видеть в коде, чем узнавать из чужого README; наблюдаемого поведения они не
// меняют, и теста на них поэтому нет.
func tracerOpts(clientID, group string) []kotel.TracerOpt {
	opts := []kotel.TracerOpt{
		kotel.TracerProvider(otel.GetTracerProvider()),
		kotel.TracerPropagator(otel.GetTextMapPropagator()),
		kotel.ClientID(clientID),
	}

	// Группа — консьюмерский атрибут: kotel кладёт её в спаны receive/process,
	// и у продюсера она означала бы принадлежность к группе, которой нет.
	if group != "" {
		opts = append(opts, kotel.ConsumerGroup(group))
	}

	return opts
}
