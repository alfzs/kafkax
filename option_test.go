package kafkax

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Тесты разделения данных и поведения: что опция применяется, что неприменимая
// к роли опция отвергается, и что сведения о заданном поведении не исчезают из
// журнала вместе с полями Config.

// optionLogRecord — запись журнала, разобранная из JSON.
type optionLogRecord struct {
	Msg     string         `json:"msg"`
	Options map[string]any `json:"options"`
}

// optionLogger отдаёт логгер, пишущий JSON в буфер, и функцию разбора записей.
//
// JSON, а не текст: проверяется вложенная группа «options» по ключам, и разбор
// текстового формата регулярками проверял бы форматирование slog вместо
// состава группы.
func optionLogger(t *testing.T) (Option, func() []optionLogRecord) {
	t.Helper()

	buf := &syncBuffer{}
	logger := slog.New(slog.NewJSONHandler(buf, nil))

	return WithLogger(logger), func() []optionLogRecord {
		t.Helper()

		var out []optionLogRecord

		for line := range strings.Lines(buf.String()) {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}

			var rec optionLogRecord
			if err := json.Unmarshal([]byte(line), &rec); err != nil {
				t.Fatalf("разбор записи журнала %q: %v", line, err)
			}

			out = append(out, rec)
		}

		return out
	}
}

// optionSummary достаёт группу «options» из записи с данным сообщением.
func optionSummary(t *testing.T, records []optionLogRecord, msg string) map[string]any {
	t.Helper()

	for _, rec := range records {
		if rec.Msg == msg {
			return rec.Options
		}
	}

	t.Fatalf("в журнале нет записи %q: %v", msg, records)

	return nil
}

// TestPanicAndSkipHooksRejectedByProducer — консьюмерская опция, переданная
// продюсеру, отвергается, а не применяется в никуда.
//
// Это и есть цена решения «один тип Option на оба конструктора»: компилятор
// такую пару не ловит, поэтому её обязан ловить конструктор. Молчаливый пропуск
// был бы худшим из исходов: WithSkipHook в NewProducer выглядит настроенной
// выдачей отравленных сообщений в DLQ, а не делает ничего, и разница вскрылась
// бы не при старте, а когда сообщение потребовалось бы спасать.
//
// Проверяются обе опции и текст ошибки: сентинела мало, потому что читателю
// лога нужно имя конструктора, который надо убрать, а не «option is not
// applicable».
func TestPanicAndSkipHooksRejectedByProducer(t *testing.T) {
	t.Parallel()

	const wantRoles = "applies to the consumer, but was passed to the producer constructor"

	// Имя подтеста — оно же ожидаемое имя конструктора в тексте ошибки: это
	// литерал теста, а не значение, вычитанное из проверяемого кода.
	tests := []struct {
		name string
		opt  Option
	}{
		{
			name: "WithPanicHook",
			opt:  WithPanicHook(func(context.Context, PanicSite, any, []byte) {}),
		},
		{
			name: "WithSkipHook",
			opt:  WithSkipHook(func(context.Context, IncomingMessage, error) error { return nil }),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p, err := NewProducer(testConfig(t), WithLogger(testLogger(t)), tt.opt)
			if err == nil {
				if closeErr := p.Close(); closeErr != nil {
					t.Logf("Close: %v", closeErr)
				}

				t.Fatalf("NewProducer с %s прошёл успешно: опция применена в никуда", tt.name)
			}

			if !errors.Is(err, ErrInapplicableOption) {
				t.Fatalf("NewProducer = %v, ожидался ErrInapplicableOption", err)
			}

			if !strings.Contains(err.Error(), tt.name) {
				t.Errorf("в тексте ошибки нет имени опции %q: %v", tt.name, err)
			}

			// Роли названы обе и в правильном порядке. Проверять вхождение
			// одного слова «consumer» мало: перепутанные местами роли дали бы
			// «applies to the producer, but was passed to the consumer», и
			// такая проверка прошла бы.
			if !strings.Contains(err.Error(), wantRoles) {
				t.Errorf("в тексте ошибки нет %q: %v", wantRoles, err)
			}
		})
	}
}

// TestConsumerAcceptsEveryOption — консьюмеру применимы все пять опций.
//
// Обратная половина предыдущего теста. Без неё маска ролей, выставленная в
// roleProducer у любой опции, прошла бы за успех: отказ проверялся бы, а
// принятие — нет.
func TestConsumerAcceptsEveryOption(t *testing.T) {
	t.Parallel()

	_ = mustConsumer(t, testConfig(t),
		WithLogger(testLogger(t)),
		WithTLSConfig(&tls.Config{MinVersion: tls.VersionTLS13}),
		WithExtraOpts(kgo.ClientID("consumer-accepts-all")),
		WithPanicHook(func(context.Context, PanicSite, any, []byte) {}),
		WithSkipHook(func(context.Context, IncomingMessage, error) error { return nil }),
	)
}

// TestProducerAcceptsCommonOptions — общие опции продюсеру применимы.
//
// Симметрично предыдущему: маска, суженная до roleConsumer, сделала бы
// WithLogger, WithTLSConfig и WithExtraOpts непригодными для продюсера, и
// поймать это отказными тестами нельзя.
func TestProducerAcceptsCommonOptions(t *testing.T) {
	t.Parallel()

	_ = mustProducer(t, testConfig(t),
		WithTLSConfig(&tls.Config{MinVersion: tls.VersionTLS13}),
		WithExtraOpts(kgo.ClientID("producer-accepts-common")),
	)
}

// TestNilOptionRejected — nil в списке опций отвергается.
//
// Типовой источник — вызов, вернувший nil вместо Option: `opts =
// append(opts, tlsOption(cfg))`, где tlsOption отдаёт nil при выключенном TLS.
// Пропустить такой элемент молча значило бы потерять настройку без следа, а
// применить — уронить конструктор паникой по nil-интерфейсу.
func TestNilOptionRejected(t *testing.T) {
	t.Parallel()

	p, err := NewProducer(testConfig(t), WithLogger(testLogger(t)), nil)
	if err == nil {
		if closeErr := p.Close(); closeErr != nil {
			t.Logf("Close: %v", closeErr)
		}

		t.Fatal("NewProducer с nil-опцией прошёл успешно")
	}

	if !errors.Is(err, ErrNilOption) {
		t.Fatalf("NewProducer = %v, ожидался ErrNilOption", err)
	}
}

// TestOptionErrorsCollectedTogether — отказы опций собираются все разом.
//
// Та же причина, что у Config.Validate: иначе набор опций чинится по одной за
// перезапуск. Проверяются обе половины — что разбор не останавливается на
// первом отказе и что второй отказ действительно доезжает до вызывающего.
func TestOptionErrorsCollectedTogether(t *testing.T) {
	t.Parallel()

	_, err := newBehavior(roleProducer,
		WithSkipHook(func(context.Context, IncomingMessage, error) error { return nil }),
		nil,
		WithPanicHook(func(context.Context, PanicSite, any, []byte) {}),
	)
	if err == nil {
		t.Fatal("newBehavior не отверг ни одной из трёх негодных опций")
	}

	got := err.Error()
	for _, want := range []string{"WithSkipHook", "WithPanicHook", "must not be nil"} {
		if !strings.Contains(got, want) {
			t.Errorf("в ошибке нет %q — разбор оборвался на первом отказе:\n%s", want, got)
		}
	}
}

// TestOptionErrorPrecedesConfigValidation — негодная опция отвергается раньше
// проверки полей.
//
// Порядок важен для читателя ошибки: неприменимая опция — ошибка вызова, и
// смешивать её со списком претензий к конфигурации, у которого документирован
// разбор через Unwrap() []error, значило бы отдать в этот список то, что к
// полям отношения не имеет.
func TestOptionErrorPrecedesConfigValidation(t *testing.T) {
	t.Parallel()

	// Конфигурация заведомо негодная: без брокеров и ClientID.
	_, err := NewProducer(Config{}, WithSkipHook(func(context.Context, IncomingMessage, error) error { return nil }))
	if !errors.Is(err, ErrInapplicableOption) {
		t.Fatalf("NewProducer = %v, ожидался ErrInapplicableOption", err)
	}

	if errors.Is(err, ErrInvalidConfig) {
		t.Errorf("отказ опции приехал агрегатом валидации конфигурации: %v", err)
	}
}

// TestWithExtraOptsAccumulates — повторный WithExtraOpts продолжает список, а
// не заменяет его.
//
// Единственная опция с таким поведением, и оно намеренное: WithExtraOpts — это
// и есть список, и «последний вызов победил» терял бы опции, добавленные из
// другого места сборки конфигурации. Проверяется по действующему значению
// опции клиента, а не по длине среза: срез мог бы накопиться и не доехать до
// kgo.NewClient.
func TestWithExtraOptsAccumulates(t *testing.T) {
	t.Parallel()

	const (
		linger   = 20 * time.Millisecond
		clientID = "extra-opts-accumulated"
	)

	cfg := testConfig(t)

	opts, err := cfg.producerOpts(testBehavior(t,
		WithExtraOpts(kgo.ProducerLinger(linger)),
		WithExtraOpts(kgo.ClientID(clientID)),
	))
	if err != nil {
		t.Fatalf("producerOpts: %v", err)
	}

	client := optsClient(t, opts)

	if got := client.OptValue(kgo.ClientID); got != clientID {
		t.Errorf("ClientID = %v, want %q: второй WithExtraOpts не применён", got, clientID)
	}

	if got, ok := client.OptValue(kgo.ProducerLinger).(time.Duration); !ok || got != linger {
		t.Errorf("ProducerLinger = %v, want %s: первый WithExtraOpts затёрт вторым", got, linger)
	}
}

// TestWithLoggerLastWins — повторная опция побеждает прежним значением
// последней.
//
// Правило общее для всех опций, кроме WithExtraOpts, и объявлено в godoc
// Option. Без проверки «первый победил» выглядел бы так же: логгер задан, лог
// пишется — просто не туда.
func TestWithLoggerLastWins(t *testing.T) {
	t.Parallel()

	first := slog.New(slog.DiscardHandler)
	second := slog.New(slog.DiscardHandler)

	b, err := newBehavior(roleProducer, WithLogger(first), WithLogger(second))
	if err != nil {
		t.Fatalf("newBehavior: %v", err)
	}

	if b.logger != second {
		t.Error("победил первый WithLogger, а не последний")
	}
}

// TestProducerLogsOptionSummary — продюсер печатает сводку опций при создании.
//
// Сюда переехали признаки, которые до разделения печатала Config.LogValue
// (tls_config_set, extra_opts). Требование к переезду было ровно одно: сведения
// не должны исчезнуть молча, — и без этой проверки они исчезли бы именно так,
// потому что ни один другой тест на состав записи не смотрит.
//
// Ключей хуков у продюсера быть не должно: задать их ему нельзя, и
// panic_hook_set=false читалось бы как «настройка есть, но выключена» вместо
// «настройки не существует».
func TestProducerLogsOptionSummary(t *testing.T) {
	t.Parallel()

	logOpt, records := optionLogger(t)

	_ = mustProducer(t, testConfig(t),
		logOpt,
		WithTLSConfig(&tls.Config{MinVersion: tls.VersionTLS13}),
		WithExtraOpts(kgo.ClientID("summary-a"), kgo.ClientID("summary-b")),
	)

	got := optionSummary(t, records(), "Kafka producer created")

	if got["tls_config_set"] != true {
		t.Errorf("tls_config_set = %v, want true", got["tls_config_set"])
	}

	if got["extra_opts"] != float64(2) {
		t.Errorf("extra_opts = %v, want 2", got["extra_opts"])
	}

	for _, key := range []string{"panic_hook_set", "skip_hook_set"} {
		if _, ok := got[key]; ok {
			t.Errorf("в сводке продюсера есть %q: опции, которой у него нет, в логе быть не должно", key)
		}
	}
}

// TestConsumerLogsOptionSummary — консьюмер печатает сводку, включая оба хука.
//
// Наличие SkipHook объясняет поведение, которого не видно больше нигде: с ним
// отравленное сообщение пропускается, без него партиция встаёт. Ровно за этим
// признак и заводился в Config.LogValue, и ровно поэтому он обязан пережить
// переезд.
func TestConsumerLogsOptionSummary(t *testing.T) {
	t.Parallel()

	logOpt, records := optionLogger(t)

	_ = mustConsumer(t, testConfig(t),
		logOpt,
		WithPanicHook(func(context.Context, PanicSite, any, []byte) {}),
		WithSkipHook(func(context.Context, IncomingMessage, error) error { return nil }),
	)

	got := optionSummary(t, records(), "Kafka consumer created")

	want := map[string]any{
		"tls_config_set": false,
		"extra_opts":     float64(0),
		"panic_hook_set": true,
		"skip_hook_set":  true,
	}

	for key, value := range want {
		if got[key] != value {
			t.Errorf("%s = %v, want %v", key, got[key], value)
		}
	}
}

// TestConsumerSummaryReportsMissingHooks — незаданный хук отражается как false,
// а не пропадает из записи.
//
// Признак нужен в обе стороны: «хук не задан» объясняет вставшую партицию так
// же, как «задан» объясняет пропуск. Сводка, печатающая только заданное,
// оставила бы дежурного без ответа именно в аварийном случае.
func TestConsumerSummaryReportsMissingHooks(t *testing.T) {
	t.Parallel()

	logOpt, records := optionLogger(t)

	_ = mustConsumer(t, testConfig(t), logOpt)

	got := optionSummary(t, records(), "Kafka consumer created")

	for _, key := range []string{"panic_hook_set", "skip_hook_set"} {
		value, ok := got[key]
		if !ok {
			t.Errorf("ключа %q нет в сводке: «хук не задан» — тоже сведение", key)

			continue
		}

		if value != false {
			t.Errorf("%s = %v, want false", key, value)
		}
	}
}

// TestValidateSeesTLSOption — Config.Validate судит о том же соединении, что
// соберётся, и потому принимает опции.
//
// Соседний TestConfigValidateSASLPlaintext проверяет то же самое на
// внутренних validateProducer/validateConsumer. Здесь проверяется публичный
// вход: без приёма опций Validate отвергала бы полностью рабочую конфигурацию
// с mTLS из памяти, и приложение, вызывающее её на старте перед
// конструкторами, падало бы там, где конструкторы проходят.
func TestValidateSeesTLSOption(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t)
	cfg.SASL = SASL{Mechanism: SASLMechanismPlain, Username: "u", Password: "p"}
	cfg.Consumer.Group = testGroup

	if err := cfg.Validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Validate без TLS = %v, ожидался ErrInvalidConfig: PLAIN шлёт пароль открытым текстом", err)
	}

	if err := cfg.Validate(WithTLSConfig(&tls.Config{MinVersion: tls.VersionTLS13})); err != nil {
		t.Fatalf("Validate с WithTLSConfig = %v, ожидался nil", err)
	}
}

// TestValidateAcceptsConsumerOnlyOptions — Validate принимает обе роли сразу.
//
// Она проверяет и продюсерскую секцию, и консьюмерскую, поэтому отвергать
// WithSkipHook ей не за что: приложение, создающее из одного Config и продюсер,
// и консьюмер, передаёт сюда весь свой набор опций целиком.
func TestValidateAcceptsConsumerOnlyOptions(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t)
	cfg.Consumer.Group = testGroup

	err := cfg.Validate(WithSkipHook(func(context.Context, IncomingMessage, error) error { return nil }))
	if err != nil {
		t.Fatalf("Validate с WithSkipHook = %v, ожидался nil", err)
	}
}

// TestValidateRejectsNilOption — негодная опция отвергается и публичным входом.
func TestValidateRejectsNilOption(t *testing.T) {
	t.Parallel()

	if err := testConfig(t).Validate(nil); !errors.Is(err, ErrNilOption) {
		t.Fatalf("Validate(nil) = %v, ожидался ErrNilOption", err)
	}
}

// TestRoleString — имена ролей в сообщениях об ошибке.
//
// Строки уходят пользователю в текст ErrInapplicableOption, и перепутанные
// между собой они отправляют читателя убирать не ту опцию. Значение roleAny
// через публичный вход не выводится (Validate принимает всё и не отказывает),
// но именно поэтому его и стоит зафиксировать здесь: первая же опция, роли
// которой сузятся, начнёт его печатать.
func TestRoleString(t *testing.T) {
	t.Parallel()

	tests := map[role]string{
		roleProducer: "producer",
		roleConsumer: "consumer",
		roleAny:      "producer or consumer",
		// Значение вне набора: молча выдать пустую строку хуже, чем показать
		// число — по нему хотя бы видно, что маска собрана неверно.
		role(8): "role(8)",
	}

	for r, want := range tests {
		if got := r.String(); got != want {
			t.Errorf("role(%d).String() = %q, want %q", uint8(r), got, want)
		}
	}
}
