package integration

import (
	"log/slog"
	"slices"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/alfzs/kafkax/v2"
)

// Записи, из которых складывается усечённая тема.
//
// Значения говорящие: по упавшему тесту сразу видно, дошло ли до обработчика
// то, чего в теме уже нет (gone), или то, что усечение пережило (kept).
var (
	// committedValues — первая партия. Её группа читает и коммитит, после чего
	// закоммиченный оффсет указывает сразу за ней.
	committedValues = []string{"committed-1", "committed-2", "committed-3"}
	// goneValues — записи, которые администратор удалит. Именно они и есть
	// «дыра»: закоммиченный оффсет группы указывает на первую из них.
	goneValues = []string{"gone-1", "gone-2", "gone-3"}
	// keptValues — хвост, переживший удаление.
	keptValues = []string{"kept-1", "kept-2"}
)

// outOfRangeMarker — часть текста, которым franz-go отмечает выход за границы
// лога. Подстрока, а не полное сообщение: у franz-go две формулировки на две
// ветки сброса, а имя протокольной ошибки одно и меняться ему незачем.
const outOfRangeMarker = "OFFSET_OUT_OF_RANGE"

// TestTruncationBelowCommittedOffset — усечение темы под ногами у группы:
// закоммиченного оффсета в логе брокера больше нет.
//
// Класс дефекта — молчаливая потеря данных. Консьюмер закоммитил оффсет N,
// администратор (или retention) удалил записи вплоть до M > N, и записи между N
// и M до обработчика не дойдут никогда. Для потребителя пакета это худший
// возможный исход: поток выглядит здоровым, обработчик работает, лаг нулевой —
// а часть сообщений просто не существовала для приложения. Именно ради этого
// случая в reportFetchError разбираются два класса ошибок (*kgo.ErrDataLoss и
// kerr.OffsetOutOfRange), попадающие в атрибут reason метрики
// kafkax.consumer.fetch.errors и одноимённой записи лога.
//
// Тест фиксирует фактическое поведение, и оно оказалось не тем, на которое
// рассчитан разбор: НИ ОДНОЙ ошибки фетча наверх не приезжает, reason не
// выставляется, эпизод не считается. Обрабатывает ситуацию franz-go, и
// целиком у себя: получив OFFSET_OUT_OF_RANGE от лидера, он не оставляет
// ошибку в фетче, а ставит партиции загрузку нового оффсета из
// ConsumeResetOffset — а его kafkax задаёт всегда, подставляя туда
// Consumer.InitialOffset и не выставляя NoResetOffset никогда. До
// consumer_worker.go, где fetches.EachError раздаёт ошибки в reportFetchError,
// не доходит ничего.
//
// Поэтому ассерты такие. Положительная часть — не «консьюмер что-то получил»
// (это зеленело бы и на теме без усечения), а два независимых свидетельства
// того, что путь исполнился: собственная запись franz-go с именем протокольной
// ошибки и разное поведение при earliest и latest. Первое доказывает, что
// брокер действительно ответил OFFSET_OUT_OF_RANGE, второе — что позиция
// сброшена именно на Consumer.InitialOffset, а не куда-то ещё: при earliest
// хвост доезжает целиком, при latest не доезжает вовсе.
//
// Отрицательная часть — что о потере не сообщено ничем. Это не придирка к
// формулировке, а зафиксированная граница наблюдаемости: алерт «данные
// пропали» по kafkax.consumer.fetch.errors на усечении темы НЕ сработает, и
// строить его нужно иначе — например, на разрыве между закоммиченным оффсетом
// группы и log start offset. Проверяется отсутствие записи, а не отсутствие
// счётчика, потому что в reportFetchError они идут подряд и безусловно: нет
// записи — не было и добавления к метрике. Если однажды franz-go начнёт
// отдавать эту ошибку наверх, тест покраснеет, и это ровно тот момент, когда
// утверждение выше пора переписывать.
//
// *kgo.ErrDataLoss здесь не воспроизводится в принципе: franz-go рождает его
// при валидации leader epoch, когда конец эпохи оказался НИЖЕ позиции курсора.
// Для этого нужна нечистая смена лидера с откатом хвоста лога, то есть кластер
// минимум из двух брокеров с потерей реплики. Односерверный контейнер этого
// класса не даёт, и подменять его чем-то похожим бессмысленно — проверялась бы
// подмена.
func TestTruncationBelowCommittedOffset(t *testing.T) {
	t.Parallel()

	// earliest: сброс на начало лога. Удалённые записи пропущены, хвост,
	// переживший усечение, вычитан. Это «мягкий» исход — теряется только дыра.
	t.Run("earliest", func(t *testing.T) {
		t.Parallel()

		cfg := testConfig(t)
		topic := primeTruncatedTopic(t, cfg)

		spy := newLogSpy(t)
		cfg.Logger = slog.New(spy)
		cfg.Consumer.InitialOffset = kafkax.OffsetEarliest
		// Порог поднят с Warn до Info на один этот сценарий: о сбросе franz-go
		// сообщает сам, и его запись — единственное прямое свидетельство того,
		// что брокер ответил именно выходом за границы лога. Без неё зелёный
		// тест не отличить от теста, в котором усечения не случилось.
		cfg.KafkaLogLevel = kafkax.KafkaLogInfo

		resumed := &collector{}
		startConsumer(t, cfg, topic, resumed)

		await(t, "консьюмер вычитал хвост, переживший усечение", func() bool {
			return resumed.has(keptValues[len(keptValues)-1])
		})

		if !spy.contains(outOfRangeMarker) {
			t.Fatalf("franz-go не сообщил о выходе за границы лога: усечение не задело "+
				"консьюмера, и остальные ассерты ничего не проверяют; лог: %v", spy.snapshot())
		}

		for _, value := range goneValues {
			if resumed.has(value) {
				t.Fatalf("обработчику досталось %q, которого в теме уже нет: %v",
					value, resumed.snapshot())
			}
		}

		// Хвост дошёл целиком, а не только последняя запись: сброс на начало
		// лога обязан отдать всё, что усечение пережило.
		for _, value := range keptValues {
			if !resumed.has(value) {
				t.Fatalf("хвост %q до обработчика не дошёл: %v", value, resumed.snapshot())
			}
		}

		requireNoFetchErrorReported(t, spy)
	})

	// latest: сброс на конец лога. Записи, пережившие усечение, тоже теряются —
	// консьюмер перескакивает через весь неотставший хвост. Ровно этот исход
	// описан в комментарии к fetchReasonOffsetOutOfRange, и он тем опаснее, что
	// в теме данные ЕСТЬ, а группа их не увидит.
	t.Run("latest", func(t *testing.T) {
		t.Parallel()

		cfg := testConfig(t)
		topic := primeTruncatedTopic(t, cfg)

		spy := newLogSpy(t)
		cfg.Logger = slog.New(spy)
		cfg.Consumer.InitialOffset = kafkax.OffsetLatest
		cfg.KafkaLogLevel = kafkax.KafkaLogInfo

		producer, err := kafkax.NewProducer(cfg)
		if err != nil {
			t.Fatalf("NewProducer: %v", err)
		}

		t.Cleanup(func() { _ = producer.Close() })

		resumed := &collector{}
		startConsumer(t, cfg, topic, resumed)

		// Маркер отправляется на каждой попытке, а не однажды: «конец лога»
		// вычисляется в момент сброса, и единственный маркер, посланный до
		// него, оказался бы ровно перед новой позицией — тест зависел бы от
		// того, кто успел раньше. Повторная отправка снимает гонку: рано или
		// поздно маркер ложится ЗА точкой сброса, и вот его-то консьюмер
		// обязан отдать.
		await(t, "консьюмер получил маркер, отправленный после сброса", func() bool {
			if err := producer.SendMessage(t.Context(), kafkax.PublishRequest{
				Topic: topic,
				Value: []byte("marker"),
			}); err != nil {
				t.Errorf("SendMessage(marker): %v", err)

				return true
			}

			return resumed.has("marker")
		})

		if !spy.contains(outOfRangeMarker) {
			t.Fatalf("franz-go не сообщил о выходе за границы лога: усечение не задело "+
				"консьюмера, и остальные ассерты ничего не проверяют; лог: %v", spy.snapshot())
		}

		// Маркер доказал, что консьюмер читает; значит отсутствие хвоста — это
		// перескок, а не «не успел». Хвост лежит ниже любого возможного «конца
		// лога» на момент сброса, поэтому дойти он не мог никак.
		for _, value := range slices.Concat(goneValues, keptValues) {
			if resumed.has(value) {
				t.Fatalf("при latest обработчику досталось %q: %v", value, resumed.snapshot())
			}
		}

		requireNoFetchErrorReported(t, spy)
	})
}

// primeTruncatedTopic готовит тему, в которой закоммиченный группой оффсет
// заведомо ниже начала лога, и возвращает её имя.
//
// Порядок важен: сначала группа коммитит оффсет за первой партией, и только
// потом дописываются записи, часть которых удаляется. Иначе удалять было бы
// нечего — удаление ровно до закоммиченного оффсета оставляет позицию годной,
// и никакого выхода за границы не случится.
func primeTruncatedTopic(t *testing.T, cfg kafkax.Config) string {
	t.Helper()

	topic := newTopic(t, 1)

	producer, err := kafkax.NewProducer(cfg)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}

	t.Cleanup(func() { _ = producer.Close() })

	send := func(values []string) {
		t.Helper()

		for _, value := range values {
			if err := producer.SendMessage(t.Context(), kafkax.PublishRequest{
				Topic: topic,
				Value: []byte(value),
			}); err != nil {
				t.Fatalf("SendMessage(%s): %v", value, err)
			}
		}
	}

	send(committedValues)

	primer := &collector{}
	consumer := startConsumer(t, cfg, topic, primer)

	await(t, "группа вычитала первую партию", func() bool {
		return primer.count() >= len(committedValues)
	})

	// Stop коммитит отмеченное синхронно: полагаться на тикер автокоммита
	// нельзя, иначе удаление могло бы обогнать коммит.
	if err := consumer.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	admin := newAdmin(t)

	fetched, err := admin.FetchOffsets(t.Context(), cfg.Consumer.Group)
	if err != nil {
		t.Fatalf("FetchOffsets: %v", err)
	}

	committed, ok := fetched.Lookup(topic, 0)
	if !ok || committed.Err != nil {
		t.Fatalf("закоммиченный оффсет %s/0 не прочитан: ok=%v err=%v", topic, ok, committed.Err)
	}

	send(goneValues)
	send(keptValues)

	// Удаляем ровно goneValues: начало лога встаёт за ними, а закоммиченный
	// оффсет остаётся указывать на первую удалённую запись.
	logStart := committed.At + int64(len(goneValues))

	var request kadm.Offsets

	request.AddOffset(topic, 0, logStart, -1)

	deleted, err := admin.DeleteRecords(t.Context(), request)
	if err != nil {
		t.Fatalf("DeleteRecords: %v", err)
	}

	result, ok := deleted.Lookup(topic, 0)
	if !ok || result.Err != nil {
		t.Fatalf("удаление записей %s/0: ok=%v err=%v", topic, ok, result.Err)
	}

	// Предпосылка сценария проверяется, а не предполагается: если брокер удалил
	// не то, что мы просили, дальнейшие ассерты позеленели бы вхолостую.
	if result.LowWatermark != logStart {
		t.Fatalf("начало лога %d, want %d", result.LowWatermark, logStart)
	}

	if committed.At >= result.LowWatermark {
		t.Fatalf("закоммиченный оффсет %d не ниже начала лога %d: усечения не случилось",
			committed.At, result.LowWatermark)
	}

	return topic
}

// requireNoFetchErrorReported требует, чтобы об усечении не было сообщено ни
// записью «Partition fetch error», ни любым reason из тех, что означают потерю
// данных.
func requireNoFetchErrorReported(t *testing.T, spy *logSpy) {
	t.Helper()

	for _, entry := range spy.snapshot() {
		if entry.message == "Partition fetch error" {
			t.Fatalf("franz-go отдал ошибку фетча наверх (reason=%q) — поведение "+
				"изменилось, утверждение теста о молчаливом сбросе пора переписывать",
				entry.attrs["reason"])
		}

		if reason := entry.attrs["reason"]; reason == "data_loss" || reason == "offset_out_of_range" {
			t.Fatalf("в логе появился reason=%q — поведение изменилось", reason)
		}
	}
}
