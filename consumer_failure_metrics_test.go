package kafkax

import (
	"errors"
	"fmt"
	"log/slog"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kslog"
	"go.opentelemetry.io/otel/attribute"
)

// Метрики отказов, которых раньше не существовало: проваленный коммит, потеря
// партиций, исчерпание бюджета дренажа, потеря данных на брокере.
//
// Общее у всех четырёх — до этих счётчиков они жили только в логе. Каждое из
// событий означает потерю или дублирование данных, то есть ровно то, ради чего
// дежурного и будят; строка в логе поднимает алерт только там, где логи
// индексируются и по ним построены правила, а метрики есть везде.

const (
	consMetricCommitErrors   = "kafkax.consumer.commit.errors"
	consMetricPartitionsLost = "kafkax.consumer.partitions.lost"
	consMetricDrainTimeouts  = "kafkax.consumer.drain.timeouts"
)

// TestFetchErrorReasonClassifiesDataLoss — потеря данных отличима от обычного
// сбоя фетча.
//
// Класс дефекта: два разных инцидента под одним сигналом. `*kgo.ErrDataLoss`
// franz-go инжектит тогда, когда обнаружил безвозвратно пропущенные записи и
// сам сдвинул позицию; `OffsetOutOfRange` означает, что запрошенного оффсета на
// брокере больше нет — усечение по retention, откат лидера, восстановление из
// бэкапа. Оба сводятся к дыре в потоке, и оба выглядели в `fetch.errors` ровно
// как «брокер недоступен», который чинится сам.
//
// Проверяется классификатор напрямую, а не через кластер: kfake ни одну из
// этих ошибок не выписывает, а подмена ответа брокера доказывала бы поведение
// kfake, а не пакета.
func TestFetchErrorReasonClassifiesDataLoss(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "data loss",
			err:  &kgo.ErrDataLoss{Topic: "t", Partition: 0, ConsumedTo: 100, ResetTo: 200},
			want: fetchReasonDataLoss,
		},
		{
			// Обёртка обязана распознаваться: до вызывающего ошибка доезжает
			// через несколько слоёв franz-go, и сравнение по типу верхнего
			// уровня промахнулось бы.
			name: "data loss wrapped",
			err:  fmt.Errorf("fetching: %w", &kgo.ErrDataLoss{Topic: "t"}),
			want: fetchReasonDataLoss,
		},
		{
			name: "offset out of range",
			err:  kerr.OffsetOutOfRange,
			want: fetchReasonOffsetOutOfRange,
		},
		{
			// Другой код брокера — обычный сбой: лидер сменился, данные на
			// месте.
			name: "other broker code",
			err:  kerr.NotLeaderForPartition,
			want: fetchReasonFetch,
		},
		{
			name: "plain error",
			err:  errors.New("dial tcp: connection refused"),
			want: fetchReasonFetch,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := fetchErrorReason(tc.err); got != tc.want {
				t.Fatalf("fetchErrorReason(%v) = %q, want %q", tc.err, got, tc.want)
			}
		})
	}
}

// TestDataLossCountedUnderOwnReason — классификация доезжает до метрики.
//
// Предыдущий тест проверяет функцию, этот — что её результат становится
// атрибутом, по которому можно построить правило. Без него классификатор мог бы
// остаться чистой функцией, которую никто не зовёт.
//
//nolint:paralleltest // captureMetrics подменяет глобальный MeterProvider
func TestDataLossCountedUnderOwnReason(t *testing.T) {
	const topic = "kafkax-data-loss-topic"

	rec := captureMetrics(t)

	brokers := newFakeCluster(t, 1, topic)
	c := mustConsumer(t, testConfig(t, brokers...))

	topicAttr := attribute.String("topic", topic)

	c.reportFetchError(topic, 0, &kgo.ErrDataLoss{Topic: topic, ConsumedTo: 10, ResetTo: 90})
	c.reportFetchError(topic, 1, errors.New("dial tcp: connection refused"))

	if got := rec.sum(consMetricFetchErrors, topicAttr,
		attribute.String("reason", fetchReasonDataLoss)); got != 1 {
		t.Fatalf("fetch.errors{reason=%s} = %d, want 1", fetchReasonDataLoss, got)
	}

	if got := rec.sum(consMetricFetchErrors, topicAttr,
		attribute.String("reason", fetchReasonFetch)); got != 1 {
		t.Fatalf("fetch.errors{reason=%s} = %d, want 1", fetchReasonFetch, got)
	}

	// Общая сумма не изменилась: разбор добавил измерение, а не задвоил учёт.
	if got := rec.sum(consMetricFetchErrors, topicAttr); got != 2 {
		t.Fatalf("fetch.errors = %d, want 2", got)
	}
}

// TestPartitionsLostCountsPartitions — потеря партиций считается партициями, а
// не событиями.
//
// Одно событие уносит столько партиций, сколько было назначено экземпляру, и
// разница между «потеряли одну из тридцати» и «потеряли все тридцать» —
// разница между рядовым ребалансом и отвалившимся координатором. Счётчик
// событий её стирает.
//
//nolint:paralleltest // captureMetrics подменяет глобальный MeterProvider
func TestPartitionsLostCountsPartitions(t *testing.T) {
	const topic = "kafkax-lost-metric-topic"

	rec := captureMetrics(t)

	brokers := newFakeCluster(t, 3, topic)
	c := mustConsumer(t, testConfig(t, brokers...))

	// Колбэк зовётся напрямую: заставить kfake разорвать сессию так, чтобы
	// onPartitionsLost пришёл ровно с тремя партициями, — это проверка kfake,
	// а не пакета. Что сам колбэк доходит до кода на реальном отзыве, покрывает
	// TestPartitionsLostDoesNotCommit.
	c.onPartitionsLost(t.Context(), nil, map[string][]int32{topic: {0, 1, 2}})

	if got := rec.sum(consMetricPartitionsLost); got != 3 {
		t.Fatalf("partitions.lost = %d, want 3: считаются события, а не партиции", got)
	}
}

// TestKafkaLogLevelGatesFranzGoLogs — порог логов franz-go отвязан от уровня
// логгера приложения.
//
// Класс дефекта: одна ручка на две несвязанные задачи. kslog отображает уровни
// один в один, поэтому приложение, поднятое с LevelDebug на время разбора
// инцидента, получало запись franz-go на каждый produce, fetch и metadata —
// включая «fetch stripped partitions» на каждом цикле опроса. Разбирать
// инцидент в таком потоке нельзя, а обходной путь через ExtraOpts нигде не
// описан.
//
// Проверяется kgo.LogLevel, а не число записей в журнале, и это точнее по
// существу: kslog выводит уровень клиента из Logger.Enabled, и franz-go при
// пороге выше Debug не станет даже собирать сообщение. Считать записи здесь
// значило бы мерить последствие вместо причины — и путать логи клиента с
// отладочными записями самого пакета, которые идут в тот же логгер.
func TestKafkaLogLevelGatesFranzGoLogs(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		level  string
		appMin slog.Level
		want   kgo.LogLevel
	}{
		{name: "empty means info", level: "", appMin: slog.LevelDebug, want: kgo.LogLevelInfo},
		{name: "none silences", level: KafkaLogNone, appMin: slog.LevelDebug, want: kgo.LogLevelNone},
		{name: "debug lets it through", level: KafkaLogDebug, appMin: slog.LevelDebug, want: kgo.LogLevelDebug},
		{name: "warn raises the floor", level: KafkaLogWarn, appMin: slog.LevelDebug, want: kgo.LogLevelWarn},
		{
			// Только ужесточение: debug здесь не включит отладку у логгера,
			// настроенного на Warn. Действующий порог — строгий из двух, иначе
			// поле конфигурации молча повышало бы многословность приложения.
			name:   "app logger still wins",
			level:  "debug",
			appMin: slog.LevelWarn,
			want:   kgo.LogLevelWarn,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			app := slog.New(slog.NewTextHandler(t.Output(), &slog.HandlerOptions{Level: tc.appMin}))

			cfg := testConfig(t)
			cfg.KafkaLogLevel = tc.level

			if got := kslog.New(cfg.kafkaLogger(app)).Level(); got != tc.want {
				t.Fatalf("kgo.LogLevel = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestKafkaLogLevelRejectsUnknownValue — опечатка в пороге отвергается
// валидацией, а не молча превращается в умолчание.
func TestKafkaLogLevelRejectsUnknownValue(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t)
	cfg.KafkaLogLevel = "verbose"

	err := cfg.Validate()
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Validate = %v, ожидался ErrInvalidConfig", err)
	}
}
