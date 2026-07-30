package kafkax

// consumer_worker.go — партиционная машинерия консьюмера: цикл опроса,
// раздача батчей по воркерам и обработка одной записи вплоть до отметки
// оффсета.
//
// Граница с соседями. Публичный API и жизненный цикл целиком в consumer.go;
// сюда попадает только то, что живёт между опросом клиента и вызовом
// ConsumerHandler. Остановку воркеров и снятие их с карты ведёт
// consumer_rebalance.go — здесь воркеры лишь создаются и работают. Инструменты
// OTel и их атрибуты объявлены в consumer_metrics.go, здесь они только
// вызываются.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// workerKey — адрес партиционного воркера. Структура сравнима, поэтому годится
// ключом карты без конкатенации строк на горячем пути.
type workerKey struct {
	topic     string
	partition int32
}

// partitionWorker — очередь и горутина одной топик-партиции. Одна горутина на
// партицию даёт параллелизм между партициями и строгий порядок внутри.
type partitionWorker struct {
	// records — батчи из одного опроса. Буфер ёмкостью
	// Consumer.MessageQueueSize определяет, насколько цикл опроса может
	// обгонять обработку; при переполнении опрос блокируется, и это и есть
	// backpressure.
	records chan []*kgo.Record
	// done закрывается горутиной воркера при выходе.
	done chan struct{}
	// cancel обрывает обработку жёстко — когда мягкая остановка не уложилась
	// в бюджет и партицию пора отпустить, чем бы воркер ни был занят.
	cancel context.CancelFunc
	// stopOnce защищает records от повторного закрытия: остановить воркер
	// могут и колбэк ребаланса, и Stop.
	stopOnce sync.Once

	// poisoned означает, что запись в этой партиции не удалось ни обработать,
	// ни отдать в OnMessageSkipped, и её оффсет не отмечен.
	//
	// Флаг обязателен, а не декоративен: MarkCommitRecords отмечает оффсет, а
	// не сообщение, поэтому отметка любой ПОСЛЕДУЮЩЕЙ записи сдвинула бы
	// коммит за проваленную и потеряла бы её. Отказ отмечать одну запись
	// сохраняет at-least-once только вместе с отказом отмечать всё, что за
	// ней.
	//
	// Читается и пишется исключительно горутиной воркера, поэтому без
	// синхронизации.
	poisoned bool
}

// stop закрывает очередь; воркер дообработает буфер и выйдет сам.
func (w *partitionWorker) stop() {
	w.stopOnce.Do(func() { close(w.records) })
}

// runPollLoop — единственный читатель клиента и единственный писатель в очереди
// воркеров.
//
// Перехват паники здесь — страховка, а не лечение: все чужие вызовы на этом
// пути уже под собственными recover. Но именно эта горутина владеет картой
// воркеров и гейтом BlockRebalanceOnPoll, и её падение уронило бы процесс
// целиком, поэтому непойманного пути отсюда быть не должно. Консьюмер после
// такого перестаёт потреблять: закрытие loopDone разблокирует Stop, но сам
// Stop обязан позвать вызывающий.
func (c *KafkaConsumer) runPollLoop(ctx context.Context, client *kgo.Client) {
	// Порядок регистрации обратен порядку исполнения: сначала перехват паники,
	// и только затем сигнал о завершении — иначе Stop пошёл бы закрывать
	// клиента, пока паника ещё разматывается.
	defer close(c.loopDone)
	defer func() {
		if r := recover(); r != nil {
			c.panics.report(context.WithoutCancel(ctx), PanicSitePollLoop, r, debug.Stack())
		}
	}()

	for c.pollOnce(ctx, client) {
	}
}

// pollOnce обрабатывает результат одного опроса. false означает, что цикл
// закончился.
//
// Отдельная функция ради defer: AllowRebalance обязателен на каждой итерации, а
// в теле цикла есть два ранних выхода и возможная паника, которые его миновали
// бы. Без него следующий ребаланс — включая тот, что инициирует закрытие
// клиента, — повис бы навсегда.
func (c *KafkaConsumer) pollOnce(ctx context.Context, client *kgo.Client) bool {
	defer client.AllowRebalance()

	fetches := client.PollRecords(ctx, c.config.Consumer.MaxPollRecords)

	// Оба условия проверяются до разбора ошибок: закрытие клиента и отмена
	// контекста приезжают синтетическим фетчем с ошибкой в нулевой
	// партиции, и принимать их за отказ брокера не за чем.
	if fetches.IsClientClosed() {
		return false
	}

	if err := fetches.Err0(); errors.Is(err, context.Canceled) {
		return false
	}

	// Обход ошибок вынесен наружу цикла по записям намеренно: партиция с
	// фатальной ошибкой и без записей приезжает отдельным пустым фетчем и
	// изнутри обхода Records не видна вовсе.
	fetches.EachError(c.reportFetchError)

	fetches.EachPartition(func(ftp kgo.FetchTopicPartition) {
		c.dispatch(ctx, client, ftp)
	})

	return true
}

// dispatch отдаёт батч воркеру партиции.
//
// Отправка блокирующая и без таймаута намеренно: батч, выброшенный из-за
// переполнения очереди, был бы перепрыгнут коммитом следующего, и сообщения
// потерялись бы молча. Блокировка тормозит опрос, а не теряет данные.
func (c *KafkaConsumer) dispatch(ctx context.Context, client *kgo.Client, ftp kgo.FetchTopicPartition) {
	if len(ftp.Records) == 0 {
		return
	}

	key := workerKey{topic: ftp.Topic, partition: ftp.Partition}

	// Партиция отдала записи — значит, выздоровела. Отметку о последней
	// сообщённой ошибке снимаем здесь, а не в обходе ошибок: тот про успех
	// ничего не знает.
	c.clearFetchError(key)

	worker := c.worker(client, key)

	select {
	case worker.records <- ftp.Records:
	// Воркер мог умереть (паника) или уйти по отмене: без этой ветки опрос
	// встал бы навсегда на партиции, которую некому читать.
	case <-worker.done:
		c.abandonDeadWorker(ctx, client, key, ftp.Records)
	case <-ctx.Done():
	}
}

// abandonDeadWorker обрабатывает батч партиции, воркер которой уже не примет
// его никогда.
//
// Молча выбросить батч — худший из возможных исходов, и до этой функции всё
// работало именно так. Запись воркера из карты снимает только awaitWorkers, то
// есть ребаланс или Stop; до тех пор dispatch на каждом опросе снова находит
// мёртвого воркера, снова выбирает ветку done и снова выбрасывает записи.
// Оффсет не движется, ошибок нет, трафика нет — партиция вычитывается по кругу
// до конца жизни процесса, и отличить это от «в топик просто не пишут» снаружи
// нечем.
//
// Партиция поэтому выводится из выборки, как отравленная: лаг растёт и виден в
// мониторинге, гейдж partitions.paused поднят, записи учтены как dropped.
// Пересоздавать воркера на той же партиции — второй возможный путь — здесь
// отвергнут: умер он от паники в собственном теле, и та же паника повторилась
// бы на новом воркере, превратив отказ в цикл смертей. Пауза снимется сама,
// когда партицию переназначат и она получит свежего воркера (resumePartition).
//
// Вызывается из горутины цикла опроса. Читать поля воркера здесь нельзя, а
// трогать набор пауз можно: pausePartition держит его под pausedMu.
func (c *KafkaConsumer) abandonDeadWorker(
	ctx context.Context, client *kgo.Client, key workerKey, records []*kgo.Record,
) {
	dropCtx := context.WithoutCancel(ctx)
	for range records {
		c.countMessage(dropCtx, key.topic, consumerStatusDropped)
	}

	// Лог один на паузу, а не на батч: партиция могла отдать ещё не один
	// буферизованный батч до того, как пауза дойдёт до брокера.
	if !c.pausePartition(client, key) {
		return
	}

	c.logger.Error("Partition worker is dead; partition paused at uncommitted offset",
		slog.String("topic", key.topic),
		slog.Int("partition", int(key.partition)),
		slog.Int("dropped", len(records)))
}

// worker возвращает воркера партиции, создавая его при необходимости.
//
// Обычно воркер уже создан колбэком назначения; ленивое создание закрывает
// окно, в котором фетч приезжает раньше колбэка, и стоит один незанятый захват
// мьютекса.
func (c *KafkaConsumer) worker(client *kgo.Client, key workerKey) *partitionWorker {
	c.workersMu.Lock()
	defer c.workersMu.Unlock()

	if w, ok := c.workers[key]; ok {
		return w
	}

	ctx, cancel := context.WithCancel(c.lifeCtx)
	w := &partitionWorker{
		records: make(chan []*kgo.Record, c.config.Consumer.MessageQueueSize),
		done:    make(chan struct{}),
		cancel:  cancel,
	}
	c.workers[key] = w

	c.metrics.workersActive.Add(ctx, 1)

	// Свежий воркер читает партицию с непрокоммиченного оффсета, поэтому пауза,
	// если она была, снимается именно здесь — и только здесь. Вызов под
	// workersMu: он берёт другой мьютекс и карту воркеров не трогает, а
	// вынесенный за замок открыл бы окно, в котором воркер уже в карте, но
	// партиция ещё не в выборке.
	c.resumePartition(client, key)

	go c.runPartitionWorker(ctx, client, key, w)

	return w
}

// runPartitionWorker последовательно обрабатывает батчи одной партиции.
func (c *KafkaConsumer) runPartitionWorker(
	ctx context.Context, client *kgo.Client, key workerKey, w *partitionWorker,
) {
	logger := c.logger.With(slog.String("topic", key.topic), slog.Int("partition", int(key.partition)))

	// Порядок регистрации обратен порядку исполнения: сначала перехват паники,
	// потом освобождение ресурсов, и только затем сигнал о завершении.
	defer close(w.done)
	defer w.cancel()
	defer c.metrics.workersActive.Add(context.WithoutCancel(ctx), -1)
	defer func() {
		if r := recover(); r != nil {
			// Паника здесь — не в обработчике, а в самом воркере: без
			// перехвата она уронила бы процесс, потому что чужая горутина
			// вызывающим кодом не ловится.
			c.panics.report(context.WithoutCancel(ctx), PanicSitePartitionWorker, r, debug.Stack(),
				slog.String("topic", key.topic),
				slog.Int("partition", int(key.partition)))
		}
	}()

	logger.Debug("Partition worker started")

	for {
		// Отмена читается наравне с очередью, а не только внутри батча:
		// простаивающий воркер иначе не увидел бы её вовсе и висел бы на
		// приёме до тех пор, пока кто-нибудь не закроет канал.
		var batch []*kgo.Record

		select {
		case b, ok := <-w.records:
			if !ok {
				return
			}

			batch = b
		case <-ctx.Done():
			return
		}

		for _, rec := range batch {
			// Проверка внутри батча, а не только на приёме: жёсткая отмена
			// должна обрывать разбор уже полученного буфера, иначе Stop по
			// истечении бюджета ждал бы обработки всей очереди.
			if ctx.Err() != nil {
				return
			}

			// Отравленная партиция: записи вычитываются из очереди, но не
			// обрабатываются. Выбрасывать их безопасно ровно потому, что
			// оффсет не отмечен — после ребаланса или перезапуска они приедут
			// снова. А продолжать читать канал обязательно: перестань воркер
			// это делать, dispatch упёрся бы в полную очередь и заблокировал
			// общий цикл опроса, остановив заодно и здоровые партиции.
			if w.poisoned {
				c.countMessage(context.WithoutCancel(ctx), rec.Topic, consumerStatusDropped)

				continue
			}

			c.processRecord(ctx, client, rec, key, w, logger)
		}
	}
}

// recordLogger — логгер одной записи, обогащаемый лениво.
//
// offset и trace_id нужны только сообщениям об отказе, а на happy path не
// пишется ни строки: два Logger.With клонируют хэндлер вместе с его
// предформатированными атрибутами, а TraceID().String() кодирует шестнадцать
// байт в hex — и всё это на каждое сообщение впустую. Обогащение поэтому
// откладывается до первого обращения и запоминается: путь отказа логирует по
// несколько раз, и платить за клонирование каждый раз незачем.
//
// Экземпляр живёт в пределах одного processRecord и не покидает горутину
// воркера, поэтому кэш без синхронизации.
type recordLogger struct {
	base   *slog.Logger
	offset int64

	// span проставляется уже после создания: обогащённый логгер нужен и
	// перехватчику паник, зарегистрированному раньше, чем спан появился.
	span trace.Span

	cached *slog.Logger
}

// get возвращает обогащённый логгер, строя его при первом обращении.
func (l *recordLogger) get() *slog.Logger {
	if l.cached != nil {
		return l.cached
	}

	log := l.base.With(slog.Int64("offset", l.offset))

	// Проверка на nil не формальность: сюда попадает и паника обвязки,
	// случившаяся до старта спана.
	if l.span != nil {
		if sc := l.span.SpanContext(); sc.IsValid() {
			log = log.With(slog.String("trace_id", sc.TraceID().String()))
		}
	}

	l.cached = log

	return log
}

// processRecord проводит одну запись через трейсинг, обработчик и отметку
// к коммиту.
func (c *KafkaConsumer) processRecord(
	ctx context.Context, client *kgo.Client, rec *kgo.Record,
	key workerKey, w *partitionWorker, logger *slog.Logger,
) {
	log := &recordLogger{base: logger, offset: rec.Offset}

	defer func() {
		r := recover()
		if r == nil {
			return
		}

		// Отдельный перехват вокруг обвязки: паника в трейсинге или в
		// метриках не должна уносить воркера вместе с очередью.
		c.panics.report(ctx, PanicSiteProcessMessage, r, debug.Stack(), recordAttrs(rec)...)

		// Отравление здесь обязательно. Штатный возврат из processRecord
		// оставил бы запись без отметки, но не остановил бы партицию — и
		// первая же успешная запись за ней сдвинула бы коммит через
		// необработанную. Паника обвязки поэтому трактуется ровно как
		// исчерпание повторов.
		//
		// Метрика исхода намеренно не пишется: упасть могла как раз она, и
		// повторный вызов того же инструмента увёл бы панику из-под этого
		// recover. Машиночитаемый след даёт kafkax.consumer.panics с
		// site=process_message.
		c.poison(client, key, w, log, fmt.Errorf("panic in message processing: %v", r))
	}()

	// Trace context из заголовков записи kotel уже извлёк на хуке фетча,
	// поэтому ручного propagator-carrier здесь нет.
	spanCtx, span := c.telemetry.tracer.WithProcessSpan(rec)
	defer span.End()

	log.span = span

	// Контекст спана построен от rec.Context, у которого нет отмены. Обработчику
	// нужен отменяемый контекст воркера, поэтому спан переносится в него, а не
	// наоборот.
	msgCtx := trace.ContextWithSpan(ctx, span)

	// Baggage переносится отдельной строкой ровно потому, что основой остаётся
	// контекст воркера: результат propagator.Extract kotel кладёт в spanCtx, и
	// вместе с ним терялось бы всё, чем отправитель разметил запрос, —
	// tenant_id, request_id и прочее. Сквозная корреляция обрывалась бы на
	// границе пакета, хотя продюсер заголовок baggage честно пишет.
	//
	// Len() перед переносом не микрооптимизация: без baggage в заголовках
	// (propagator по умолчанию его не ставит) ContextWithBaggage клал бы в
	// контекст пустое значение на каждое сообщение.
	if bag := baggage.FromContext(spanCtx); bag.Len() > 0 {
		msgCtx = baggage.ContextWithBaggage(msgCtx, bag)
	}

	handler, ok := c.handler(rec.Topic)
	if !ok {
		// Возможно только при рассинхроне подписки и карты обработчиков.
		// Оффсет не отмечается: сообщение вернётся, а не исчезнет.
		c.countMessage(msgCtx, rec.Topic, consumerStatusError)
		c.poison(client, key, w, log, errors.New("no handler registered"),
			slog.String("reason", "subscription and handler map are out of sync"))

		return
	}

	msg := newIncomingMessage(rec)

	start := time.Now()

	decided, attempts, err := c.runHandler(msgCtx, handler, msg, span, log)
	if !decided {
		// Отмена застала паузу между попытками: вердикта нет, длительность
		// мерить нечего. Оффсет не отмечается — сообщение приедет снова, —
		// поэтому и статус не skipped: там коммит двигается, здесь нет.
		c.countMessage(context.WithoutCancel(msgCtx), rec.Topic, consumerStatusCancelled)

		return
	}

	status := consumerStatusSuccess
	if err != nil {
		status = c.resolveFailure(msgCtx, client, msg, key, w, err, attempts, log)
	}

	// Длительность включает все попытки и все паузы между ними: измеряется
	// задержка сообщения, а не одного вызова обработчика.
	c.recordOutcome(msgCtx, rec.Topic, status, time.Since(start))

	if status == consumerStatusError {
		// Неотмеченная запись — и есть гарантия at-least-once: коммит не
		// сдвинется за неё, и после перезапуска или ребаланса она приедет
		// снова. Партиция при этом уже отравлена в resolveFailure, иначе
		// отметка следующей записи сдвинула бы коммит за эту.
		return
	}

	// Жёсткая отмена означает, что бюджет ожидания воркера исчерпан и партиция
	// либо уже отдана другому участнику группы, либо будет отдана сейчас.
	// Отметка в этот момент — не безобидное опоздание: abandonAssignment
	// обнуляет g.uncommitted, но MarkCommitRecords пересоздаёт карту
	// (kgo/consumer_group.go), и следующий тик автокоммита отправит оффсет уже
	// под новой валидной генерацией. Владение партицией Kafka в OffsetCommit не
	// проверяет — коммит будет принят и откатит прогресс нового владельца
	// назад, то есть заставит группу перечитать чужой хвост.
	//
	// Цена отказа — повторная доставка ровно этой записи: обработчик её
	// отработал, но оффсет за неё не отмечен. Дубликат дешевле отката.
	if ctx.Err() != nil {
		log.get().Warn("Offset not marked: worker was hard-cancelled after the message was processed",
			slog.String("status", status))

		return
	}

	// MarkCommitRecords без AutoCommitMarks() был бы no-op, а сдвинуть оффсет
	// назад он не умеет — порядок отметок внутри партиции гарантирован тем,
	// что воркер один.
	client.MarkCommitRecords(rec)
}

// resolveFailure решает судьбу сообщения, которое обработчик не осилил за все
// попытки: отдать его OnMessageSkipped или остановить партицию.
//
// Возвращает статус для метрик. consumerStatusError означает, что оффсет
// отмечать нельзя и партиция отравлена; consumerStatusSkipped — что хук забрал
// сообщение и коммит может двигаться дальше.
//
// Уровень лога выбирается здесь и только здесь — это и есть причина, по которой
// runHandler на исчерпании повторов молчит. Исход знает лишь resolveFailure: при
// работающем OnMessageSkipped отказ обработчика — штатное событие с Warn, и
// Error из глубины стека давал бы постоянный фон Error на исправной работе,
// то есть ровно ту причину, по которой на Error перестают реагировать.
// Отравление партиции, наоборот, всегда Error — но одной записью, а не тремя на
// трёх уровнях стека.
func (c *KafkaConsumer) resolveFailure(
	ctx context.Context,
	client *kgo.Client,
	msg IncomingMessage,
	key workerKey,
	w *partitionWorker,
	cause error,
	attempts int,
	log *recordLogger,
) string {
	if c.config.OnMessageSkipped == nil {
		c.poison(client, key, w, log, cause,
			slog.Int("attempts", attempts),
			slog.String("reason", "no OnMessageSkipped hook is configured"))

		return consumerStatusError
	}

	if hookErr := c.callSkipHook(ctx, msg, cause); hookErr != nil {
		c.poison(client, key, w, log, cause,
			slog.Int("attempts", attempts),
			slog.String("reason", "OnMessageSkipped refused the message"),
			slog.Any("hook_error", hookErr))

		return consumerStatusError
	}

	log.get().Warn("Message skipped after exhausting retries",
		slog.Int("attempts", attempts),
		slog.Any("error", cause))

	return consumerStatusSkipped
}

// callSkipHook вызывает OnMessageSkipped под собственным recover.
//
// Хук — чужой код, исполняемый в горутине воркера уже после того, как recover
// вокруг обработчика отработал: его собственная паника прошла бы мимо и уронила
// процесс. Паника трактуется как отказ забрать сообщение — иначе упавший хук
// молча разрешил бы сдвинуть коммит.
func (c *KafkaConsumer) callSkipHook(ctx context.Context, msg IncomingMessage, cause error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			c.panics.report(ctx, PanicSiteMessageSkipped, r, debug.Stack(),
				slog.String("topic", msg.Topic),
				slog.Int("partition", int(msg.Partition)),
				slog.Int64("offset", msg.Offset))

			// Значение recover() доносится так же, как в callHandler. Без
			// него ошибка сообщает только «хук упал»: сам текст паники
			// уходит в отдельную запись репортера, а в hook_error, который
			// resolveFailure кладёт рядом с решением отравить партицию,
			// приезжает голый сентинел, одинаковый на любую причину.
			err = fmt.Errorf("on message skipped: %w: %v", ErrHandlerPanic, r)
		}
	}()

	return c.config.OnMessageSkipped(ctx, msg, cause)
}

// poison останавливает партицию на неотмеченном оффсете.
//
// Флага poisoned мало: без паузы клиент продолжил бы тянуть записи, которые
// воркер обязан выбрасывать, и партиция крутила бы трафик, который всё равно
// приедет заново. PauseFetchPartitions прекращает выборку, не отдавая
// партицию: назначение остаётся за нами, лаг растёт и виден в мониторинге.
//
// Пауза снимается вместе со сменой воркера — см. resumePartition. Набор пауз в
// franz-go принадлежит клиенту, а не назначению, и сам по себе ребаланс его не
// трогает.
//
// extra — атрибуты, уточняющие причину отказа: число попыток, ветка
// resolveFailure, ошибка хука. Запись здесь единственная на весь отказ, поэтому
// всё, что о нём известно, обязано приехать в неё, а не в отдельные строки
// уровнем выше.
func (c *KafkaConsumer) poison(
	client *kgo.Client, key workerKey, w *partitionWorker, log *recordLogger,
	cause error, extra ...slog.Attr,
) {
	w.poisoned = true

	args := make([]any, 0, len(extra)+1)
	args = append(args, slog.Any("error", cause))

	for _, attr := range extra {
		args = append(args, attr)
	}

	log.get().Error("Partition paused at uncommitted offset; the message will be redelivered "+
		"after rebalance or restart", args...)

	c.pausePartition(client, key)
}

// pausePartition выводит партицию из выборки и поднимает гейдж пауз.
//
// Возвращает true, если партиция ставится на паузу впервые: гейдж считает
// партиции, а не отравленные сообщения. Повторный вызов на той же партиции
// возможен — воркер выбрасывает записи, но обвязка вокруг processRecord может
// упасть и на выброшенной, а мёртвому воркеру батчи приезжают снова и снова, —
// и без этой проверки счётчик уехал бы вверх на каждой записи и никогда не
// вернулся.
//
// Вызывается из двух горутин: воркерной (через poison) и цикла опроса (через
// abandonDeadWorker), поэтому набор пауз живёт под pausedMu.
func (c *KafkaConsumer) pausePartition(client *kgo.Client, key workerKey) bool {
	client.PauseFetchPartitions(map[string][]int32{key.topic: {key.partition}})

	c.pausedMu.Lock()
	defer c.pausedMu.Unlock()

	if _, already := c.paused[key]; already {
		return false
	}

	c.paused[key] = struct{}{}
	c.metrics.partitionsPaused.Add(context.WithoutCancel(c.lifeCtx), 1)

	return true
}

// resumePartition возвращает партицию в выборку, если та была приостановлена.
//
// Вызывается ровно при появлении нового воркера — и это единственный момент,
// когда снимать паузу правильно. Новый воркер означает, что партиция будет
// прочитана заново с непрокоммиченного оффсета: отравившее её сообщение
// приедет снова, ради чего пауза и ставилась.
//
// Привязка к воркеру, а не к списку assigned, — не стилистический выбор.
// Балансировщик franz-go по умолчанию кооперативный, и onPartitionsAssigned
// получает только вновь добавленные партиции: партиция, пережившая ребаланс за
// тем же экземпляром, в этот список не попадает. Резюмировать по нему значило
// бы оставить её на паузе навсегда, если воркера всё-таки пересоздали, и
// наоборот — снять паузу с партиции, чей отравленный воркер жив и продолжает
// выбрасывать записи.
func (c *KafkaConsumer) resumePartition(client *kgo.Client, key workerKey) {
	c.pausedMu.Lock()

	_, wasPaused := c.paused[key]
	if wasPaused {
		delete(c.paused, key)
		c.metrics.partitionsPaused.Add(context.WithoutCancel(c.lifeCtx), -1)
	}

	// Журнал ошибок фетча чистится вместе с паузой: партиция начинает жизнь
	// заново, и следующая её поломка обязана быть сообщена как новая.
	delete(c.lastFetchErr, key)

	c.pausedMu.Unlock()

	if !wasPaused {
		return
	}

	client.ResumeFetchPartitions(map[string][]int32{key.topic: {key.partition}})
	c.logger.Info("Partition resumed after reassignment",
		slog.String("topic", key.topic),
		slog.Int("partition", int(key.partition)))
}

// recordOutcome записывает исход обработки целиком: длительность и счётчик.
//
// Отдельный метод, а не две строки в processRecord: это единственная точка,
// где на каждое сообщение трогаются оба инструмента разом, и её цена
// измеряется бенчмарком.
func (c *KafkaConsumer) recordOutcome(ctx context.Context, topic, status string, elapsed time.Duration) {
	// Один поиск в кэше на оба инструмента: набор атрибутов у них общий.
	opts := c.opts.get(topic, status)

	c.metrics.duration.Record(ctx, elapsed.Seconds(), opts.record...)
	c.metrics.processed.Add(ctx, 1, opts.add...)
}

// countMessage инкрементирует счётчик исходов обработки.
func (c *KafkaConsumer) countMessage(ctx context.Context, topic, status string) {
	c.metrics.processed.Add(ctx, 1, c.opts.get(topic, status).add...)
}

// runHandler вызывает обработчик с повторами.
//
// Первый результат — «вердикт получен»: false означает, что отмена контекста
// прервала паузу между попытками и исход сообщения неизвестен. Второй — число
// сделанных вызовов обработчика: судьбу сообщения решает resolveFailure, и это
// единственный способ довести до его записи в журнале, сколько попыток стоило
// сообщение.
func (c *KafkaConsumer) runHandler(
	ctx context.Context, handler ConsumerHandler, msg IncomingMessage, span trace.Span, log *recordLogger,
) (bool, int, error) {
	maxRetries := c.config.Consumer.HandlerMaxRetries

	for attempt := 0; ; attempt++ {
		err := c.callHandler(ctx, handler, msg, attempt)
		if err == nil {
			return true, attempt + 1, nil
		}

		// Отрицательное значение (-1) означает «повторять бесконечно», ноль —
		// «без повторов»: attempt считает уже сделанные повторы, а не вызовы.
		if maxRetries >= 0 && attempt >= maxRetries {
			// Единственный RecordError на отказ. Паника обработчика в спан
			// отсюда попадает тоже — callHandler спан не трогает намеренно:
			// иначе паника на последней попытке приезжала бы в трассировку
			// дважды и читалась бы как две разные аварии.
			//
			// Лога здесь нет: уровень выбирает resolveFailure, который один
			// знает, штатный это исход или отравление партиции.
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())

			return true, attempt + 1, err
		}

		log.get().Warn("Handler failed, retrying",
			slog.Int("attempt", attempt+1),
			slog.Int("max_retries", maxRetries),
			slog.Any("error", err))
		c.metrics.retries.Add(ctx, 1, c.opts.get(msg.Topic, noStatus).add...)

		if !waitRetryDelay(ctx, c.config.Consumer.HandlerRetryDelay) {
			return false, attempt + 1, err
		}
	}
}

// callHandler вызывает обработчик под recover.
//
// Паника превращается в обычную ошибку, чтобы сообщение прошло штатный путь
// повторов, а воркер остался жив: до этого паника обработчика убивала воркера
// и осиротевшая очередь целиком перепрыгивалась коммитом следующего воркера.
// Плата — детерминированная паника повторяется HandlerMaxRetries раз.
//
// attempt нужен ровно для того, чтобы эта плата не стала неограниченной. Полный
// рапорт — стек в лог, инкремент kafkax.consumer.panics, вызов OnPanic — пишется
// только на первой попытке. При HandlerMaxRetries=-1 (конфигурация, которую
// doc.go прямо рекомендует) повторы не кончаются никогда, и рапорт на каждой
// давал бы бесконечный поток Error со стеком и счётчик, растущий линейно во
// времени: «одно сообщение крутится сутки» стало бы неотличимо от «упало N
// разных». Сами повторы при этом не молчат — каждый пишет Warn с текстом
// паники, а спан получает её от runHandler на исчерпании.
func (c *KafkaConsumer) callHandler(
	ctx context.Context, handler ConsumerHandler, msg IncomingMessage, attempt int,
) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %v", ErrHandlerPanic, r)

			if attempt == 0 {
				c.panics.report(ctx, PanicSiteHandler, r, debug.Stack(),
					slog.String("topic", msg.Topic),
					slog.Int("partition", int(msg.Partition)),
					slog.Int64("offset", msg.Offset))
			}
		}
	}()

	return handler.ProcessMessage(ctx, msg)
}

// waitRetryDelay выдерживает паузу; false означает отмену контекста.
func waitRetryDelay(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// newIncomingMessage переводит запись franz-go в сообщение публичного API.
func newIncomingMessage(rec *kgo.Record) IncomingMessage {
	return IncomingMessage{
		Topic:     rec.Topic,
		Partition: rec.Partition,
		Offset:    rec.Offset,
		Key:       rec.Key,
		Value:     rec.Value,
		Headers:   fromRecordHeaders(rec.Headers),
		Timestamp: rec.Timestamp,
	}
}

// recordAttrs — координаты записи для логов.
func recordAttrs(rec *kgo.Record) []slog.Attr {
	return []slog.Attr{
		slog.String("topic", rec.Topic),
		slog.Int("partition", int(rec.Partition)),
		slog.Int64("offset", rec.Offset),
	}
}
