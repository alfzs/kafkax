package integration

// failure_test.go — отказоустойчивость против настоящего брокера: изоляция
// отравленной партиции, хук пропуска, дренаж на остановке и перезапуск брокера
// под нагрузкой.
//
// Граница с соседями. rebalance_test.go отвечает за семантику группы, когда всё
// исправно, а участников больше одного. Здесь исправно не всё: ломается либо
// сообщение — обработчик его не осилил, — либо сам брокер. Общее у сценариев
// одно: доказательством служит состояние в брокере (закоммиченный оффсет,
// содержимое лога темы), а не то, что приехало обработчику. Доставка одинаково
// молчит и про удержанное сообщение, и про потерянное, а чинятся эти два случая
// противоположным образом.

import (
	"context"
	"errors"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/alfzs/kafkax/v3"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// errPoison — отказ обработчика, воспроизводимый на каждой попытке. Именно
// стабильный отказ, а не случайный: сценарии ниже проверяют, что происходит с
// сообщением, которое не обработается никогда, а не с тем, которое повезёт со
// второго раза.
var errPoison = errors.New("обработчик не осилил сообщение")

// TestPoisonedPartitionKeepsNeighboursRunning проверяет изоляцию отказа по
// партициям: сообщение, которое обработчик не осилил, обязано остановить свою
// партицию и не тронуть соседние.
//
// Класс дефекта — общий на консьюмера тормоз. Любая попытка «придержать»
// отказавшую партицию через общий цикл опроса, общую очередь или общую отметку
// коммита превращает одно неисправное сообщение в остановку всей темы, и
// снаружи это выглядит как «сервис перестал читать Kafka» без единой строчки о
// причине. Против kfake изоляция уже проверена, но там нет ни настоящей паузы
// выборки (PauseFetchPartitions доходит до брокера), ни __consumer_offsets, по
// которому только и видно, докуда группа реально доехала.
//
// Ассерт поэтому стоит на оффсетах в брокере, а не на одной доставке.
// Партиция, в которую просто ничего не написали, и партиция, вставшая на
// непрокоммиченном оффсете, по доставке неотличимы, а чинятся по-разному:
// соседняя обязана быть закоммичена до конца, отравленная — ровно на записи
// перед отравленной, и свежий консьюмер обязан получить отравленную запись
// снова. Последнее и есть at-least-once: пауза удерживает сообщение, а не
// хоронит его.
//
// Ключевая волна — та, что отправлена в соседнюю партицию УЖЕ ПОСЛЕ
// подтверждённой остановки отравленной. Порядок обработки двух партиций ничем
// не задан, и на записях, лежащих в теме с самого начала, «сосед жив» и «сосед
// успел отработать раньше, чем всё встало» неотличимы: тест зеленел бы и на
// консьюмере, который от одного отказа умирает целиком. Отсюда же и повторная
// проверка отравленного оффсета после этой волны: коммит соседней партиции не
// имеет права утащить за собой чужую позицию.
func TestPoisonedPartitionKeepsNeighboursRunning(t *testing.T) {
	t.Parallel()

	const poison = "p0-poison"

	topic := newTopic(t, 2)
	cfg := testConfig(t)

	produceToPartition(t, topic, 0, "p0-ok", poison, "p0-after")
	produceToPartition(t, topic, 1, "p1-first", "p1-second")

	failPoison := func(msg kafkax.IncomingMessage) error {
		if string(msg.Value) == poison {
			return errPoison
		}

		return nil
	}

	first := &collector{fn: failPoison}
	consumer := startConsumer(t, cfg, topic, first)

	admin := newAdmin(t)
	group := cfg.Consumer.Group

	await(t, "отравленное сообщение дошло до обработчика", func() bool {
		return first.has(poison)
	})

	await(t, "отравленная партиция встала перед отравленной записью", func() bool {
		return committedOffset(t, admin, group, topic, 0) == 1
	})

	// Запись за отравленной приехала тем же батчем — все три записи легли в
	// партицию до старта консьюмера — и выброшена без обработки. Обработать её
	// значило бы отметить её оффсет, а отметка оффсета — это отметка позиции, а
	// не сообщения: коммит перепрыгнул бы отравленную запись и потерял её молча.
	if first.has("p0-after") {
		t.Fatalf("обработана запись за отравленной: %v", first.snapshot())
	}

	await(t, "оффсет соседней партиции доехал до брокера", func() bool {
		return committedOffset(t, admin, group, topic, 1) == 2
	})

	// Соседняя партиция нагружается, когда отравленная уже стоит: до этого
	// момента её работа ничего не доказывает.
	produceToPartition(t, topic, 1, "p1-after-poison")

	await(t, "соседняя партиция работает при вставшей отравленной", func() bool {
		return first.has("p1-after-poison") && committedOffset(t, admin, group, topic, 1) == 3
	})

	if got := committedOffset(t, admin, group, topic, 0); got != 1 {
		t.Fatalf("отравленная партиция уехала на оффсет %d, want 1: "+
			"прогресс соседней партиции утащил за собой чужую позицию", got)
	}

	if err := consumer.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	second := &collector{fn: failPoison}
	startConsumer(t, cfg, topic, second)

	await(t, "после перезапуска отравленное сообщение приехало заново", func() bool {
		return second.has(poison)
	})

	// Соседняя партиция закоммичена до конца, поэтому перечитывать ей нечего.
	// Без этой проверки тест зеленел бы и на консьюмере, который после
	// перезапуска читает тему с начала.
	for _, value := range []string{"p1-first", "p1-second", "p1-after-poison"} {
		if second.has(value) {
			t.Fatalf("соседняя партиция перечитана заново: %v", second.snapshot())
		}
	}
}

// TestSkipHookAdvancesOffset проверяет вторую половину политики повторов: с
// заданным OnMessageSkipped, вернувшим nil, отравленное сообщение перестаёт
// быть отравленным.
//
// Класс дефекта — асимметрия двух веток разрешения отказа. Ветка без хука
// обязана НЕ двигать оффсет, ветка с хуком — обязана его двинуть, и обе они
// сходятся в одной точке (отметка записи к коммиту). Ошибка в любую сторону
// молчалива: не двинули — конвейер встал, хотя дежурному обещали DLQ; двинули
// не то — потеряли соседнее сообщение. Это рекомендованная doc.go
// конфигурация «прогресс важнее отдельного сообщения», и против настоящего
// брокера, где коммит проходит весь путь до __consumer_offsets, она не
// проверялась ни разу.
//
// Доказательством служит именно оффсет в брокере плюс поведение свежего
// консьюмера: «сообщение больше не приезжает» само по себе может означать и
// потерю, и паузу, и незакоммиченный прогресс, который вскроется на следующем
// ребалансе.
func TestSkipHookAdvancesOffset(t *testing.T) {
	t.Parallel()

	const poison = "poison"

	topic := newTopic(t, 1)
	cfg := testConfig(t)

	var (
		skipMu sync.Mutex
		skips  []string
	)

	skipHook := kafkax.WithSkipHook(func(_ context.Context, msg kafkax.IncomingMessage, _ error) error {
		skipMu.Lock()
		defer skipMu.Unlock()

		skips = append(skips, string(msg.Value))

		return nil
	})

	producer := openProducer(t, cfg)
	publishValues(t, producer, topic, "before", poison, "after")

	first := &collector{fn: func(msg kafkax.IncomingMessage) error {
		if string(msg.Value) == poison {
			return errPoison
		}

		return nil
	}}
	consumer := startConsumer(t, cfg, topic, first, skipHook)

	// Запись ЗА отравленной — единственное прямое свидетельство, что партиция
	// не встала: в теме одна партиция, и обработать «after» можно только
	// перешагнув через «poison».
	await(t, "партиция продолжила работу за отравленным сообщением", func() bool {
		return first.has("after")
	})

	skipMu.Lock()

	gotSkips := append([]string(nil), skips...)

	skipMu.Unlock()

	if len(gotSkips) != 1 || gotSkips[0] != poison {
		t.Fatalf("OnMessageSkipped получил %v, want [%s]", gotSkips, poison)
	}

	admin := newAdmin(t)

	await(t, "оффсет доехал до брокера за отравленным сообщением", func() bool {
		return committedOffset(t, admin, cfg.Consumer.Group, topic, 0) == 3
	})

	if err := consumer.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	second := &collector{}
	startConsumer(t, cfg, topic, second)

	publishValues(t, producer, topic, "marker")

	await(t, "свежий консьюмер получил маркер", func() bool {
		return second.has("marker")
	})

	if got := second.snapshot(); len(got) != 1 {
		t.Fatalf("свежий консьюмер получил %v, want только маркер: "+
			"пропущенное сообщение приехало заново", got)
	}
}

// TestStopDrainsInFlightMessage проверяет дренаж: сообщение, которое обработчик
// держит в момент Stop, обязано быть дообработано, а его оффсет — закоммичен.
//
// Класс дефекта — остановка, которая рвёт обработку посередине. Закрыть клиента
// раньше, чем воркеры дошли до отметки оффсета, значит на каждом штатном
// деплое переобрабатывать хвост батча; отменить контекст воркера вместо
// закрытия очереди — то же самое, но ещё и с оборванным обработчиком внутри
// чужой транзакции. Против kfake порядок фаз проверен, но финальный коммит там
// никуда не идёт: координатора нет, и «CommitMarkedOffsets вернул nil»
// означает лишь, что эмулятор принял вызов.
//
// Отсюда четыре ассерта, и ни один не про время: Stop не вернулся, пока
// обработчик держал сообщение; оффсет спрошен у настоящего координатора; свежий
// консьюмер сообщение не получил; Stop уложился в GracefulTimeout — потому что
// превышение бюджета переводит остановку на жёсткую отмену, при которой пакет
// намеренно НЕ отмечает оффсет.
//
// Проверяется контракт, а не конкретная фаза остановки, и против настоящего
// брокера это различие не умозрительное: у дренажа в Stop есть страховка,
// которой нет у kfake. CloseAllowingRebalance выводит участника из группы, а
// уход вызывает onPartitionsRevoked — тот же дренаж и тот же коммит, только
// изнутри закрытия клиента. Вырезанный stopAllWorkers этот тест поэтому
// переживает; жёсткий обрыв воркеров вместо дренажа — уже нет.
func TestStopDrainsInFlightMessage(t *testing.T) {
	t.Parallel()

	topic := newTopic(t, 1)
	cfg := testConfig(t)

	producer := openProducer(t, cfg)
	publishValues(t, producer, topic, "held")

	var releaseOnce sync.Once

	release := make(chan struct{})
	releaseHandler := func() { releaseOnce.Do(func() { close(release) }) }

	// Обработчик отпускается в любом исходе. Провалившийся ассерт иначе
	// оставил бы Stop висеть на недообработанном сообщении, и падение одного
	// теста стало бы зависанием всего пакета — включая t.Cleanup, который сам
	// зовёт Stop.
	t.Cleanup(releaseHandler)

	held := &collector{fn: func(kafkax.IncomingMessage) error {
		<-release

		return nil
	}}
	consumer := startConsumer(t, cfg, topic, held)

	await(t, "обработчик забрал сообщение", func() bool {
		return held.count() > 0
	})

	stopped := make(chan error, 1)
	started := time.Now()

	go func() { stopped <- consumer.Stop() }()

	// Отпустить обработчик до того, как Stop начался, значило бы проверить
	// остановку простаивающего консьюмера — то есть не проверить ничего.
	// Начало остановки наблюдаемо снаружи по коду ошибки AddHandler: до Stop
	// консьюмер отвечает ErrConsumerStarted, с началом остановки —
	// ErrConsumerClosed. Ни в одном из состояний вызов ничего не меняет.
	await(t, "Stop перевёл консьюмера в терминальное состояние", func() bool {
		return errors.Is(consumer.AddHandler(topic+"-probe", held), kafkax.ErrConsumerClosed)
	})

	// Прямое утверждение о дренаже: пока обработчик держит сообщение, Stop не
	// имеет права вернуться. Косвенные ассерты ниже поймали бы и обрыв, но
	// назвали бы его «финальный коммит не дошёл», а причина в этом случае
	// другая.
	select {
	case err := <-stopped:
		t.Fatalf("Stop вернулся (%v), не дождавшись обработчика: дренаж пропущен", err)
	default:
	}

	releaseHandler()

	var stopErr error

	select {
	case stopErr = <-stopped:
	case <-time.After(waitFor):
		t.Fatal("Stop не вернулся")
	}

	if stopErr != nil {
		t.Fatalf("Stop: %v", stopErr)
	}

	if elapsed := time.Since(started); elapsed >= cfg.GracefulTimeout {
		t.Fatalf("Stop занял %v при GracefulTimeout=%v: дренаж не уложился в мягкий бюджет",
			elapsed, cfg.GracefulTimeout)
	}

	// Оффсет спрашивается у брокера отдельно от проверки свежим консьюмером:
	// вторая молчит о том, дошёл ли финальный коммит до координатора или
	// сообщение просто ещё не переназначено.
	if got := committedOffset(t, newAdmin(t), cfg.Consumer.Group, topic, 0); got != 1 {
		t.Fatalf("закоммичен оффсет %d, want 1: финальный коммит не дошёл до координатора", got)
	}

	second := &collector{}
	startConsumer(t, cfg, topic, second)

	publishValues(t, producer, topic, "marker")

	await(t, "свежий консьюмер получил маркер", func() bool {
		return second.has("marker")
	})

	if got := second.snapshot(); len(got) != 1 {
		t.Fatalf("свежий консьюмер получил %v, want только маркер: "+
			"удержанное сообщение приехало заново", got)
	}
}

// TestBrokerRestartUnderLoad гасит брокер под работающим консьюмером и
// поднимает его заново.
//
// Класс дефекта — живой мертвец. Клиент, который прекрасно работает, пока
// брокер отвечает, после обрыва восстанавливает соединения и заново входит в
// группу, но партиции ему больше не назначены — либо назначены, а воркеры под
// них не пересозданы. Процесс при этом здоров по всем внешним признакам: он
// жив, heartbeat идёт, ошибок нет, лаг растёт. Ни один другой сценарий набора
// этого не воспроизводит: там брокер не исчезает никогда, а kfake не исчезает
// в принципе.
//
// # Почему брокер здесь свой, а не общий
//
// Не осторожность, а необходимость. Порт хоста, на который отображён 9093,
// docker раздаёт заново при каждом старте контейнера — проверяется одной
// парой `docker stop`/`docker start`. Остановив и подняв общий брокер, тест
// сменил бы ему адрес и сломал бы сразу двоих: себя, потому что консьюмер под
// тестом знает только тот seed-адрес, с которым его создали, и восстанавливаться
// ему стало бы некуда, — и весь остальной пакет, у которого shared.brokers
// остался прежним. Поэтому контейнер поднимается отдельный и с ЗАКРЕПЛЁННЫМ
// портом хоста: только так адрес переживает перезапуск.
//
// # Почему нет t.Parallel()
//
// Отдельный контейнер снимает главную опасность — соседние тесты больше не
// зависят от брокера, который здесь гасится, — но не снимает вторую. Это самый
// чувствительный к таймингам сценарий набора: между гашением и подъёмом
// брокера консьюмер обязан пережить истечение сессии, потерю партиций и
// повторный вход в группу. Параллельный запуск свёл бы его со всеми остальными
// тестами, которые в этот момент нагружают ту же машину и общий брокер, а
// голодание по CPU здесь выглядит ровно как невосстановившийся консьюмер.
// Последовательная фаза `go test` — единственный момент, когда параллельные
// тесты стоят на паузе, и этот тест намеренно занимает её целиком.
//
//nolint:paralleltest // намеренно последовательный, причина — в комментарии выше
func TestBrokerRestartUnderLoad(t *testing.T) {
	broker, seeds := dedicatedBroker(t)

	// Таймауты те же, что у остальных сценариев, — расходится только адрес.
	cfg := testConfig(t)
	cfg.Brokers = seeds

	topic := topicName(t)
	admin := newAdminAt(t, seeds)
	createTopic(t, admin, topic, 1)

	producer := openProducer(t, cfg)
	received := &collector{}
	startConsumer(t, cfg, topic, received)

	// Круг до аварии: без него «после перезапуска ничего не приехало»
	// неотличимо от «консьюмер не работал никогда».
	publishValues(t, producer, topic, "before")
	await(t, "консьюмер работает до аварии", func() bool {
		return received.has("before")
	})

	load := startLoad(t, producer, topic)

	stopBroker(t, broker)

	// Факт аварии подтверждается состоянием, а не возвратом docker stop:
	// брокер обязан именно перестать отвечать, иначе весь сценарий проверяет
	// перезапуск, которого не было.
	await(t, "брокер перестал отвечать", func() bool {
		return !brokerReachable(t, admin)
	})

	startBroker(t, broker)

	await(t, "брокер снова отвечает", func() bool {
		return brokerReachable(t, admin)
	})

	acked := load.stop()
	if acked == 0 {
		t.Fatal("нагрузка не подтвердила ни одной записи: проверять нечего")
	}

	// Маркер отправляется последним и последним же приезжает: партиция одна,
	// порядок внутри неё гарантирован, поэтому его получение означает, что
	// консьюмер дочитал всё, что лежит в теме до него. Это и есть
	// доказательство восстановления — без единого таймера.
	publishValues(t, producer, topic, "marker")
	await(t, "консьюмер восстановился и продолжил обработку", func() bool {
		return received.has("marker")
	})

	// at-least-once сверяется с тем, что брокер ДЕЙСТВИТЕЛЬНО сохранил, а не со
	// списком подтверждённых отправок. Реплика одна, а SIGTERM этот образ не
	// заканчивает — docker добивает контейнер SIGKILL'ом по истечении своего
	// бюджета, — поэтому подтверждённая, но не сброшенная на диск запись
	// законно исчезает вместе с брокером. Это свойство такой установки Kafka, а
	// не пакета. Утверждение теста ровно одно и оно проверяемо: ничего из
	// уцелевшего в логе консьюмер не потерял. Дубликаты при этом допустимы —
	// гарантия пакета at-least-once, а не exactly-once.
	stored := readTopic(t, seeds, topic)

	if len(stored) < 2 {
		t.Fatalf("в теме %d записей: перезапуск проверять не на чем", len(stored))
	}

	// Расхождение между подтверждёнными и уцелевшими — это цена аварии, а не
	// отказ, но именно она объясняет, почему сверка идёт с логом. В отказе
	// соседнего ассерта знать её порядок первым делом полезно.
	t.Logf("подтверждено отправок: %d (плюс before и marker), уцелело в теме: %d",
		acked, len(stored))

	for _, value := range stored {
		if !received.has(value) {
			t.Fatalf("запись %q лежит в теме, но консьюмер её не получил: "+
				"at-least-once нарушена", value)
		}
	}
}

// dedicatedBroker поднимает брокер, принадлежащий одному тесту, и закрепляет за
// ним порт хоста. Причины — в документации TestBrokerRestartUnderLoad.
//
// Пропуск при недоступном Docker повторяет решение brokers(): набор обязан быть
// запускаемым на машине без Docker, а в CI отсутствие Docker обязано быть
// отказом — там выставлен KAFKAX_INTEGRATION=required.
func dedicatedBroker(t *testing.T) (*tckafka.KafkaContainer, []string) {
	t.Helper()

	port := strconv.Itoa(freeHostPort(t))

	ctx, cancel := context.WithTimeout(context.Background(), startTimeout)
	defer cancel()

	broker, err := tckafka.Run(ctx, kafkaImage,
		tckafka.WithClusterID("kafkax-restart"),
		testcontainers.WithHostConfigModifier(func(hostConfig *container.HostConfig) {
			hostConfig.PortBindings = network.PortMap{
				network.MustParsePort("9093/tcp"): {{HostPort: port}},
			}
		}))

	if broker != nil {
		t.Cleanup(func() { terminateBroker(t, broker) })
	}

	if err != nil {
		if os.Getenv("KAFKAX_INTEGRATION") == "required" {
			t.Fatalf("отдельный брокер не поднялся, а KAFKAX_INTEGRATION=required: %v", err)
		}

		t.Skipf("Docker недоступен, сценарий с перезапуском пропущен: %v", err)
	}

	// Адрес именно закреплённый, а не спрошенный у контейнера: спрошенный
	// пришлось бы спрашивать заново после каждого подъёма, и тест перестал бы
	// проверять то, ради чего написан.
	return broker, []string{"127.0.0.1:" + port}
}

func stopBroker(t *testing.T, broker *tckafka.KafkaContainer) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	timeout := 10 * time.Second
	if err := broker.Stop(ctx, &timeout); err != nil {
		t.Fatalf("остановка брокера: %v", err)
	}
}

func startBroker(t *testing.T, broker *tckafka.KafkaContainer) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), startTimeout)
	defer cancel()

	if err := broker.Start(ctx); err != nil {
		t.Fatalf("подъём брокера: %v", err)
	}
}

func terminateBroker(t *testing.T, broker *tckafka.KafkaContainer) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	if err := broker.Terminate(ctx); err != nil {
		t.Errorf("не удалось погасить отдельный брокер: %v", err)
	}
}

// brokerReachable отвечает, отдаёт ли брокер метаданные.
//
// Собственный короткий бюджет обязателен: без него один запрос к мёртвому
// брокеру съел бы весь потолок await, и «брокер не поднялся» стало бы
// неотличимо от «await не успел спросить».
func brokerReachable(t *testing.T, admin *kadm.Client) bool {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err := admin.Metadata(ctx)

	return err == nil
}

// produceToPartition кладёт значения в ЗАДАННУЮ партицию.
//
// Партиция назначается вручную, а не выводится из ключа, потому что сценарий
// изоляции целиком построен на том, что лежит в соседней партиции, и «ключи,
// скорее всего, разъедутся» — не то основание, на котором строится ассерт.
// Публичный продюсер пакета выбора партиции не даёт намеренно (см.
// PublishRequest), поэтому здесь клиент сырой.
func produceToPartition(t *testing.T, topic string, partition int32, values ...string) {
	t.Helper()

	client := rawClient(t, brokers(t), kgo.RecordPartitioner(kgo.ManualPartitioner()))

	for _, value := range values {
		record := &kgo.Record{Topic: topic, Partition: partition, Value: []byte(value)}
		if err := client.ProduceSync(t.Context(), record).FirstErr(); err != nil {
			t.Fatalf("запись %q в %s/%d: %v", value, topic, partition, err)
		}
	}
}
