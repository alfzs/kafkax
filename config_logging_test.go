package kafkax

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

// Тесты логгера библиотеки: порог логов franz-go и выбор базового логгера.
//
// Класс дефекта здесь один на все проверки — отказ, который ничем себя не
// обнаруживает. Отброшенная запись franz-go неотличима от «клиенту нечего было
// сказать», и заметить пропажу можно только в разборе инцидента, то есть ровно
// тогда, когда на неё рассчитывают. Отсюда форма ассертов: запись прогоняется
// сквозь всю цепочку в буфер, а проверяется её присутствие там, а не значения,
// вычисленные по дороге.

// logCanary — сообщение, которое невозможно спутать с чужой записью в буфере:
// его присутствие означает, что запись дошла до вложенного хендлера, а
// отсутствие — что её отбросили по дороге.
const logCanary = "kafkax-log-canary"

// bufferLogger — логгер приложения, пишущий в буфер и ничего не отсекающий
// сам: единственным фильтром в этих тестах должна быть проверяемая обёртка.
func bufferLogger(buf *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// TestKafkaLoggerDeliversRecordsThatPassTheThreshold — запись, прошедшая порог,
// доходит до вложенного хендлера.
//
// Класс дефекта — фильтр, отфильтровывающий всё. minLevelHandler.Handle
// состоит из одной строки «отдать запись дальше», и её пропажа (return nil без
// вызова inner) не роняла ни одного теста: логи franz-go исчезали целиком, а
// TestKafkaLogLevelGatesFranzGoLogs продолжал видеть верный kgo.LogLevel,
// потому что тот выводится из Enabled и до Handle не доходит (находка Д3,
// docs/audit/09-mutation-sweep.md). Отказ при этом абсолютно тихий: молчащий
// клиент выглядит как исправный.
//
// Порог логгера приложения здесь всегда Debug: проверяется обёртка, а
// взаимодействие двух порогов — предмет TestKafkaLogLevelGatesFranzGoLogs.
func TestKafkaLoggerDeliversRecordsThatPassTheThreshold(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		level  string
		record slog.Level
		want   bool
	}{
		{name: "debug пропускает debug", level: KafkaLogDebug, record: slog.LevelDebug, want: true},
		{name: "пустой порог означает info", level: "", record: slog.LevelInfo, want: true},
		{name: "info отсекает debug", level: KafkaLogInfo, record: slog.LevelDebug},
		{name: "warn пропускает warn", level: KafkaLogWarn, record: slog.LevelWarn, want: true},
		{name: "warn отсекает info", level: KafkaLogWarn, record: slog.LevelInfo},
		// Ветка "error" по покрытию не исполнялась ни разу, и подмена её на
		// slog.LevelInfo оставляла набор зелёным — то есть порог, ради которого
		// поле и заводили, мог перестать действовать незамеченным.
		{name: "error пропускает error", level: KafkaLogError, record: slog.LevelError, want: true},
		{name: "error отсекает warn", level: KafkaLogError, record: slog.LevelWarn},
		{name: "none молчит даже об ошибках", level: KafkaLogNone, record: slog.LevelError},
		// Неопознанное значение сюда доезжает только у того, кто собрал клиента
		// в обход Validate. Порог по умолчанию выбран именно для этого случая:
		// опечатка в KAFKAX_KAFKA_LOG_LEVEL не должна ни ронять процесс, ни
		// тихо выключать логи клиента у того, кто о ней не знает.
		{name: "опечатка мимо Validate даёт info", level: "verbose", record: slog.LevelInfo, want: true},
		{name: "опечатка мимо Validate отсекает debug", level: "verbose", record: slog.LevelDebug},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer

			cfg := testConfig(t)
			cfg.KafkaLogLevel = tc.level

			cfg.kafkaLogger(bufferLogger(&buf)).Log(t.Context(), tc.record, logCanary)

			if got := strings.Contains(buf.String(), logCanary); got != tc.want {
				t.Fatalf("запись уровня %v при kafka_log_level=%q дошла до хендлера: %t, ожидалось %t",
					tc.record, tc.level, got, tc.want)
			}
		})
	}
}

// TestKafkaLoggerKeepsThresholdAcrossWithAttrsAndGroup — порог переживает
// .With(attr) и .WithGroup(...).
//
// Обе функции строят НОВЫЙ minLevelHandler, и потерять в нём порог — правка в
// один идентификатор: нулевое значение slog.Level равно Info, поэтому
// конфигурация с none или error молча превратилась бы в обычный info-поток.
// По профилю покрытия ни WithAttrs, ни WithGroup не исполнялись ни разу — то
// есть дефект внесли бы, не увидев ни красного теста, ни непокрытой строки.
//
// Вызовы не гипотетические: kslog навешивает на переданный логгер собственную
// группу и атрибуты клиента, так что в бою обёртка всегда пересобранная.
func TestKafkaLoggerKeepsThresholdAcrossWithAttrsAndGroup(t *testing.T) {
	t.Parallel()

	const attrCanary = "kafkax-attr-canary"

	cases := []struct {
		name string
		wrap func(*slog.Logger) *slog.Logger
		// wantAttr — подстрока, которую обязана нести прошедшая запись.
		// Заодно доказывает, что вложенному хендлеру достались и сами
		// атрибуты: обёртка, потерявшая их, отличается от исправной только
		// содержимым записи.
		wantAttr string
	}{
		{
			name:     "With(attr)",
			wrap:     func(l *slog.Logger) *slog.Logger { return l.With(slog.String("client", attrCanary)) },
			wantAttr: "client=" + attrCanary,
		},
		{
			name: "WithGroup",
			wrap: func(l *slog.Logger) *slog.Logger { return l.WithGroup("kgo") },
		},
		{
			name: "WithGroup + With(attr)",
			wrap: func(l *slog.Logger) *slog.Logger {
				return l.WithGroup("kgo").With(slog.String("client", attrCanary))
			},
			wantAttr: "kgo.client=" + attrCanary,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer

			cfg := testConfig(t)
			cfg.KafkaLogLevel = KafkaLogWarn

			logger := tc.wrap(cfg.kafkaLogger(bufferLogger(&buf)))

			logger.Info(logCanary)

			if strings.Contains(buf.String(), logCanary) {
				t.Fatalf("после %s порог warn перестал действовать — info дошёл до хендлера:\n%s",
					tc.name, buf.String())
			}

			logger.Warn(logCanary)

			got := buf.String()
			if !strings.Contains(got, logCanary) {
				t.Fatalf("после %s запись уровня warn не дошла до хендлера:\n%s", tc.name, got)
			}

			if tc.wantAttr != "" && !strings.Contains(got, tc.wantAttr) {
				t.Errorf("после %s в записи нет %q — вложенный хендлер не получил атрибуты:\n%s",
					tc.name, tc.wantAttr, got)
			}
		})
	}
}

// TestConfigLoggerFallsBackToSlogDefault — без WithLogger библиотека берёт
// slog.Default().
//
// Ветка по покрытию не исполнялась ни разу: все тесты пакета передают логгер
// явно. Снятие fallback компилируется и роняет первую же запись паникой по
// nil-логгеру — притом что godoc WithLogger обещает ровно обратное, а
// конструктор без опций это самый обычный способ поднять клиент из
// конфигурации окружения.
//
//nolint:paralleltest // подменяет глобальный slog.Default(): параллельный сосед писал бы в тот же буфер
func TestConfigLoggerFallsBackToSlogDefault(t *testing.T) {
	var buf bytes.Buffer

	prev := slog.Default()

	t.Cleanup(func() { slog.SetDefault(prev) })
	slog.SetDefault(bufferLogger(&buf))

	// Пустой набор опций — то же, что конструктор без WithLogger.
	b, err := newBehavior(roleProducer)
	if err != nil {
		t.Fatalf("newBehavior: %v", err)
	}

	componentLogger(b.logger, "producer").Info(logCanary)

	got := buf.String()
	if !strings.Contains(got, logCanary) {
		t.Fatalf("запись не дошла до slog.Default():\n%s", got)
	}

	if !strings.Contains(got, "component=producer") {
		t.Errorf("в записи нет component=producer — логгер собран без пометки компонента:\n%s", got)
	}
}
