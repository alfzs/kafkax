package kafkax

// consumer_metrics.go — инструменты OTel домена консьюмера, замкнутые множества
// значений их атрибутов и учёт ошибок фетча.
//
// Граница с соседями. Здесь объявляются инструменты и всё, что определяет
// кардинальность их меток; вызывают их горячие пути из consumer_worker.go и
// consumer_rebalance.go. Ошибки фетча попали сюда потому, что их дедуп
// существует ровно ради счётчика и уровня записи, а не ради логики потребления.

import (
	"context"
	"errors"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Значения атрибута status у метрик kafkax.consumer.messages.processed и
// kafkax.consumer.message.duration.
const (
	// consumerStatusSuccess — обработчик вернул nil, запись отмечена к коммиту.
	consumerStatusSuccess = "success"
	// consumerStatusError — обработчик исчерпал повторы и не справился.
	// Запись НЕ отмечена: at-least-once держится именно на этом.
	consumerStatusError = "error"
	// consumerStatusSkipped — запись сознательно пропущена и отмечена к
	// коммиту: обработчик исчерпал повторы, а OnMessageSkipped вернул nil,
	// то есть принял её на себя.
	//
	// Это единственный статус, при котором коммит сдвигается за запись, так и
	// не обработанную успешно. Ненулевой рост — повод смотреть в OnMessageSkipped.
	consumerStatusSkipped = "skipped"
	// consumerStatusCancelled — отмена контекста застала паузу между повторами:
	// вердикта нет, оффсет НЕ отмечен, партицию останавливать не за что.
	// Сообщение приедет снова.
	//
	// Отдельно от skipped, хотя оба означают «успеха не было»: коммит здесь не
	// двигается, записи в DLQ под это событие нет, и происходит оно на каждом
	// штатном завершении процесса. Свалив их в одно значение, дашборд DLQ
	// завышал бы счёт на каждом деплое. Длительность под этим статусом не
	// пишется: мерить нечего, обработка не закончилась.
	consumerStatusCancelled = "cancelled"
	// consumerStatusDropped — запись прочитана из очереди отравленной партиции
	// и выброшена не глядя. Оффсет не отмечен, поэтому она приедет снова.
	//
	// Считается, чтобы масштаб отравления был виден: за одной остановившейся
	// партицией стоит не одно сообщение, а весь буфер, набранный до паузы.
	consumerStatusDropped = "dropped"
)

// consumerStatuses — то же множество списком, для прогрева кэша опций метрик.
//
// Держится рядом с константами намеренно: забытый здесь статус не сломает
// метрику, но вернёт ей аллокации на горячем пути — молча, и заметно это будет
// только по бенчмарку.
var consumerStatuses = []string{
	consumerStatusSuccess,
	consumerStatusError,
	consumerStatusSkipped,
	consumerStatusCancelled,
	consumerStatusDropped,
}

// consumerMetrics — собственные метрики домена консьюмера.
//
// Транспортный уровень измеряет kotel, и дублировать его здесь нечем — но
// измеряет он меньше, чем можно подумать: в kotel v1.7.0 только счётчики
// (соединения, разрывы, ошибки и байты чтения/записи, записи и байты
// produce/fetch) и ни одной гистограммы. Латентности запросов к брокеру и
// распределения размеров батчей не меряет никто. Если она понадобится, её
// придётся заводить здесь, а не искать в kotel.
type consumerMetrics struct {
	processed        metric.Int64Counter
	duration         metric.Float64Histogram
	retries          metric.Int64Counter
	fetchErrors      metric.Int64Counter
	groupErrors      metric.Int64Counter
	commitErrors     metric.Int64Counter
	partitionsLost   metric.Int64Counter
	drainTimeouts    metric.Int64Counter
	workersActive    metric.Int64UpDownCounter
	partitionsPaused metric.Int64UpDownCounter
	panics           metric.Int64Counter
}

// Значения атрибута phase у kafkax.consumer.commit.errors и
// kafkax.consumer.drain.timeouts. Множество замкнутое: фаза — это ветка кода,
// а не входные данные.
const (
	// phaseRevoke — отзыв партиций на ребалансе.
	phaseRevoke = "revoke"
	// phaseShutdown — остановка консьюмера.
	phaseShutdown = "shutdown"
	// phasePollLoop — цикл опроса не вышел за отведённый ему бюджет.
	phasePollLoop = "poll_loop"
	// phaseWorkers — партиционные воркеры не дренировались за бюджет и были
	// отменены жёстко.
	phaseWorkers = "workers"
)

// newConsumerMetrics регистрирует инструменты, собирая все ошибки разом.
//
// Ни у одной метрики нет атрибута partition: он умножает кардинальность на
// число партиций, а диагностическая ценность нулевая — привязку к партиции
// даёт спан обработки, где она есть и без метрик.
func newConsumerMetrics(meter metric.Meter) (consumerMetrics, error) {
	var reg instrumentRegistry

	processedName := "kafkax.consumer.messages.processed"
	processed, err := meter.Int64Counter(processedName,
		metric.WithDescription("Messages that reached a terminal outcome, by topic and status"),
		metric.WithUnit("{message}"))
	m := consumerMetrics{processed: record(&reg, processedName, processed, err)}

	// Единица — секунды, а не миллисекунды: Milliseconds() усекает до целого,
	// и все длительности быстрее миллисекунды попадали бы в нулевую корзину.
	// Seconds() усечения не делает.
	durationName := "kafkax.consumer.message.duration"
	duration, err := meter.Float64Histogram(durationName,
		metric.WithDescription("End-to-end handler time including all retries and retry delays"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(consumerDurationBuckets...))
	m.duration = record(&reg, durationName, duration, err)

	retriesName := "kafkax.consumer.handler.retries"
	retries, err := meter.Int64Counter(retriesName,
		metric.WithDescription("Handler invocations that failed and were retried"),
		metric.WithUnit("{retry}"))
	m.retries = record(&reg, retriesName, retries, err)

	// Считаются эпизоды, а не опросы: неретраибельную ошибку партиции franz-go
	// возвращает на каждом опросе заново, и счётчик всех вхождений мерил бы
	// частоту опроса, а не масштаб проблемы. Инкремент делается на переходе
	// «партиция была здорова → сломалась» и на смену текста ошибки.
	fetchErrorsName := "kafkax.consumer.fetch.errors"
	fetchErrors, err := meter.Int64Counter(fetchErrorsName,
		metric.WithDescription("Partition-level fetch error episodes, counted on state change"),
		metric.WithUnit("{episode}"))
	m.fetchErrors = record(&reg, fetchErrorsName, fetchErrors, err)

	// Отказ уровня группы — не то же самое, что отказ партиции: сообщений нет
	// вообще, и алерт на него нужен свой. franz-go подкидывает такую ошибку
	// синтетическим фетчем с пустым топиком и партицией 0, поэтому в
	// fetch.errors она выглядела как поломка несуществующей партиции.
	// Атрибутов нет намеренно: топика у этой ошибки не существует.
	groupErrorsName := "kafkax.consumer.group.errors"
	groupErrors, err := meter.Int64Counter(groupErrorsName,
		metric.WithDescription("Group session error episodes, counted on state change"),
		metric.WithUnit("{episode}"))
	m.groupErrors = record(&reg, groupErrorsName, groupErrors, err)

	// Проваленный коммит — самый частый источник дубликатов, и до этого
	// счётчика он существовал только в логе. Атрибут phase разделяет два случая
	// с разной ценой: на revoke партиция уходит другому участнику, и он
	// перечитает хвост немедленно; на shutdown хвост перечитает следующий
	// экземпляр после старта.
	commitErrorsName := "kafkax.consumer.commit.errors"
	commitErrors, err := meter.Int64Counter(commitErrorsName,
		metric.WithDescription("Offset commits that failed, by phase"),
		metric.WithUnit("{commit}"))
	m.commitErrors = record(&reg, commitErrorsName, commitErrors, err)

	// Потеря партиций — это отзыв БЕЗ возможности закоммитить: сессия группы
	// уже разорвана. Считаются партиции, а не события: одно событие уносит
	// столько партиций, сколько было назначено, и «потеряли одну из тридцати»
	// от «потеряли все тридцать» иначе не отличить.
	lostName := "kafkax.consumer.partitions.lost"
	lost, err := meter.Int64Counter(lostName,
		metric.WithDescription("Partitions lost without a chance to commit"),
		metric.WithUnit("{partition}"))
	m.partitionsLost = record(&reg, lostName, lost, err)

	// Исчерпание бюджета мягкой остановки. Каждое такое событие означает
	// оборванную обработку и, скорее всего, дубликаты после перезапуска —
	// сигнал, что GracefulTimeout мал для реального времени обработки.
	drainName := "kafkax.consumer.drain.timeouts"
	drain, err := meter.Int64Counter(drainName,
		metric.WithDescription("Graceful drain budgets exhausted, by phase"),
		metric.WithUnit("{timeout}"))
	m.drainTimeouts = record(&reg, drainName, drain, err)

	workersName := "kafkax.consumer.workers.active"
	workers, err := meter.Int64UpDownCounter(workersName,
		metric.WithDescription("Partition workers currently running"),
		metric.WithUnit("{worker}"))
	m.workersActive = record(&reg, workersName, workers, err)

	// Продолжающийся сигнал для штатного исхода политики отравленного
	// сообщения. Разового лога мало: приостановленная партиция — это состояние,
	// и алерт «стоит хотя бы одна» строится только по гейджу. Разовое событие к
	// моменту дежурства уже уехало из окна.
	pausedName := "kafkax.consumer.partitions.paused"
	paused, err := meter.Int64UpDownCounter(pausedName,
		metric.WithDescription("Partitions currently paused at an uncommitted offset"),
		metric.WithUnit("{partition}"))
	m.partitionsPaused = record(&reg, pausedName, paused, err)

	panicsName := "kafkax.consumer.panics"
	panicsCounter, err := meter.Int64Counter(panicsName,
		metric.WithDescription("Panics recovered inside kafkax consumer goroutines, by site"),
		metric.WithUnit("{panic}"))
	m.panics = record(&reg, panicsName, panicsCounter, err)

	return m, reg.err()
}

// reportFetchError фиксирует ошибку фетча — партиционную или групповую.
//
// Сообщается только смена состояния. Ретраибельные ошибки franz-go гасит сам,
// а неретраибельные оставляет в фетче: курсор снова становится годным, и
// следующий опрос приносит ту же ошибку. Бэкоффа в этой ветке нет, поэтому
// безусловный лог давал бы поток записей Error с частотой опроса — при
// MaxWait=500ms это минимум две в секунду на партицию, а при мгновенном ответе
// брокера на порядки больше. Счётчик при этом мерил бы частоту опроса вместо
// масштаба проблемы.
func (c *KafkaConsumer) reportFetchError(topic string, partition int32, err error) {
	if errors.Is(err, context.Canceled) || errors.Is(err, kgo.ErrClientClosed) {
		return
	}

	// Отказ уровня группы franz-go подкидывает синтетическим фетчем с пустым
	// топиком и партицией 0. Без разбора он выглядел бы как поломка
	// несуществующей партиции 0 топика "" — притом что это худший из отказов
	// консьюмера: сообщений нет вообще, ни по одной партиции.
	if groupErr, isGroupErr := errors.AsType[*kgo.ErrGroupSession](err); isGroupErr {
		// Дедуп и лог смотрят на одну и ту же ошибку — распакованную. Иначе
		// смена обёртки вокруг неизменившейся причины считалась бы новым
		// эпизодом.
		if c.firstReport(workerKey{}, groupErr) {
			c.metrics.groupErrors.Add(context.WithoutCancel(c.lifeCtx), 1)
			c.logger.Error("Consumer group session error", slog.Any("error", groupErr))
		}

		return
	}

	key := workerKey{topic: topic, partition: partition}
	if !c.firstReport(key, err) {
		return
	}

	// Разбор отделяет два класса, которые означают РЕАЛЬНУЮ потерю данных, от
	// обычного сбоя фетча. Он стоит две проверки на эпизод, а не на опрос:
	// дедуп по смене состояния уже отсеял повторы, и сюда доходит только первая
	// ошибка эпизода. Поэтому же атрибуты собираются на месте, а не берутся из
	// кэша opts: горячего пути здесь нет.
	reason := fetchErrorReason(err)

	c.metrics.fetchErrors.Add(context.WithoutCancel(c.lifeCtx), 1,
		metric.WithAttributes(
			attribute.String("topic", topic),
			attribute.String("reason", reason)))

	// Потеря данных — это Error по любому счёту; обычный сбой фетча тоже
	// остаётся Error, потому что неретраибельная ошибка партиции требует
	// вмешательства. Различает их атрибут reason, по нему и строится алерт:
	// «брокер недоступен» чинится сам, «данные пропали» — нет.
	c.logger.Error("Partition fetch error",
		slog.String("topic", topic),
		slog.Int("partition", int(partition)),
		slog.String("reason", reason),
		slog.Any("error", err))
}

// Значения атрибута reason в записи о партиционной ошибке фетча.
const (
	// fetchReasonDataLoss — franz-go обнаружил безвозвратно пропущенные записи
	// и сам сбросил позицию. Данные потеряны на стороне брокера: усечение по
	// retention, откат лидера, восстановление из бэкапа.
	fetchReasonDataLoss = "data_loss"
	// fetchReasonOffsetOutOfRange — запрошенного оффсета на брокере больше нет,
	// позиция сбрасывается на Consumer.InitialOffset. При earliest это
	// перечитывание с начала, при latest — молчаливый перескок через весь
	// неотставший хвост.
	fetchReasonOffsetOutOfRange = "offset_out_of_range"
	// fetchReasonFetch — всё остальное: недоступный брокер, смена лидера,
	// таймаут запроса. Данные на месте, отказ временный или требует починки
	// кластера, но ничего не пропало.
	fetchReasonFetch = "fetch"
)

// fetchErrorReason классифицирует ошибку фетча.
//
// Смысл разбора в том, что по одному лишь тексту ошибки алерт «мы потеряли
// данные» не построишь, а от «брокер недоступен» он отличается ценой: второе
// чинится само, первое требует решения, что делать с дырой в потоке.
func fetchErrorReason(err error) string {
	// AsType, а не Is: kgo.ErrDataLoss — структура с полями (топик, партиция,
	// потерянный и восстановленный оффсеты), а не сентинел, и сравнивать с ней
	// нечего.
	// Значение именованное, а не `_`: у *kgo.ErrDataLoss есть Error(), то есть
	// это ошибка, и линтер справедливо не даёт молча выбросить её в пустоту.
	if lost, isLoss := errors.AsType[*kgo.ErrDataLoss](err); isLoss && lost != nil {
		return fetchReasonDataLoss
	}

	// Сравнение по коду, а не по значению: kerr.OffsetOutOfRange — экземпляр
	// *kerr.Error, а брокер присылает свой; равенство указателей здесь ничего
	// не значит.
	if kerrErr, ok := errors.AsType[*kerr.Error](err); ok &&
		kerrErr.Code == kerr.OffsetOutOfRange.Code {
		return fetchReasonOffsetOutOfRange
	}

	return fetchReasonFetch
}

// firstReport сообщает, изменилось ли состояние ошибки на key.
//
// Сравнение по тексту, а не по значению: ошибки franz-go — указатели на
// структуры, свежие на каждом фетче, и == сравнивал бы адреса. Текст же
// содержит и код ошибки, и её параметры, поэтому смена причины отказа
// распознаётся как новый эпизод.
func (c *KafkaConsumer) firstReport(key workerKey, err error) bool {
	text := err.Error()

	c.pausedMu.Lock()
	defer c.pausedMu.Unlock()

	if c.lastFetchErr[key] == text {
		return false
	}

	c.lastFetchErr[key] = text

	return true
}

// clearFetchError снимает отметку об ошибке: партиция снова отдаёт записи.
//
// Без сброса повторение той же ошибки после выздоровления не было бы сообщено
// вовсе — дедуп превратился бы в «сообщаем один раз за жизнь процесса».
func (c *KafkaConsumer) clearFetchError(key workerKey) {
	c.pausedMu.Lock()
	defer c.pausedMu.Unlock()

	delete(c.lastFetchErr, key)
}
