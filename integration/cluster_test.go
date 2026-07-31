package integration

// cluster_test.go — кластер из трёх брокеров.
//
// Зачем отдельно от остального набора. Всё, что стоит выше, идёт против одного
// брокера с RF=1, и целый класс поведения там не воспроизводится в принципе:
// репликация, ISR, выборы лидера партиции. Одиночный брокер не отличает
// «продюсер пережил перевыборы» от «перевыборов не было», а именно перевыборы
// и есть штатное событие в жизни кластера — раскатка, перезапуск узла,
// сетевой разрыв.
//
// Образ здесь другой, и это вынужденно. confluentinc/confluent-local, на
// котором стоит весь набор, поднимается стартовым скриптом модуля
// testcontainers, а тот форматирует хранилище случайным cluster id
// (`kafka-storage random-uuid`) на каждом контейнере. Трём узлам KRaft нужен
// ОДИН общий id, иначе кворум контроллеров не соберётся: они друг друга не
// признают. apache/kafka берёт id из переменной CLUSTER_ID, поэтому кластер
// собирается из него. Версия закреплена по той же причине, что и у соседа:
// «latest» превратил бы отказ теста в вопрос «пакет или брокер».
//
// Адреса закреплены заранее (freeHostPort), а не спрошены у контейнера после
// подъёма: KAFKA_ADVERTISED_LISTENERS обязан быть известен до старта, иначе
// брокер сообщит клиенту адрес, по которому с хоста не достучаться.

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/alfzs/kafkax/v3"
	"github.com/moby/moby/api/types/container"
	mobynet "github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	tcnetwork "github.com/testcontainers/testcontainers-go/network"
	"github.com/twmb/franz-go/pkg/kadm"
)

const (
	// clusterImage — образ узла кластера, см. заголовок файла.
	clusterImage = "apache/kafka:3.7.1"
	// clusterSize — три узла, а не два: кворуму KRaft нужно большинство, и
	// на двух узлах потеря одного останавливает контроллер вместе с ним.
	clusterSize = 3
	// clusterID — общий идентификатор кластера. Любая корректная base64-строка
	// из 16 байт; значение из документации Kafka.
	clusterID = "4L6g3nShT-eMCtK--X86sw"
	// clusterPort — порт слушателя PLAINTEXT внутри контейнера.
	clusterPort = "9093/tcp"
)

// clusterNode — узел кластера. id совпадает с KAFKA_NODE_ID, то есть с тем
// числом, которым брокер называет себя в метаданных: без этого «лидер — узел
// 2» нельзя перевести в «остановить вот этот контейнер».
type clusterNode struct {
	id        int32
	container testcontainers.Container
	address   string
}

// kafkaCluster — поднятый кластер и адреса его узлов.
type kafkaCluster struct {
	nodes []clusterNode
}

// seeds отдаёт адреса всех узлов.
func (c *kafkaCluster) seeds() []string {
	addrs := make([]string, 0, len(c.nodes))
	for _, node := range c.nodes {
		addrs = append(addrs, node.address)
	}

	return addrs
}

// seedsExcept отдаёт адреса всех узлов, кроме заданного. Нужен после
// остановки узла: клиент, которому мёртвый адрес назван первым, тратит на него
// бюджет каждого запроса.
func (c *kafkaCluster) seedsExcept(id int32) []string {
	addrs := make([]string, 0, len(c.nodes))

	for _, node := range c.nodes {
		if node.id != id {
			addrs = append(addrs, node.address)
		}
	}

	return addrs
}

// TestReplicatedCluster — круг на теме с RF=3 и потеря лидера партиции под
// нагрузкой.
//
// Подтесты последовательны и делят один кластер: второй останавливает узел,
// то есть портит его для первого. Отсюда и nolint ниже — параллельность здесь
// не «забыли», а нельзя.
//
//nolint:paralleltest // подтесты делят один кластер, и второй его ломает
func TestReplicatedCluster(t *testing.T) {
	t.Parallel()

	cluster := dedicatedCluster(t)

	t.Run("круг на теме с тремя репликами", func(t *testing.T) {
		clusterRoundTrip(t, cluster)
	})

	t.Run("потеря лидера партиции под нагрузкой", func(t *testing.T) {
		clusterLeaderFailover(t, cluster)
	})
}

// clusterRoundTrip — самый простой вопрос, на который одиночный брокер
// ответить не может: доезжает ли запись до темы, у которой три реплики и
// требование двух синхронных.
//
// Он же проверяет обвязку: если кластер собран неверно — разошлись cluster id,
// не сошёлся кворум, адрес не тот, — красным станет этот подтест, а не разбор
// перевыборов.
func clusterRoundTrip(t *testing.T, cluster *kafkaCluster) {
	t.Helper()

	seeds := cluster.seeds()
	topic := topicName(t)

	createTopicWith(t, newAdminAt(t, seeds), topic, 1, clusterSize, minISRConfig(2))

	cfg := configFor(t, seeds)
	producer := openProducer(t, cfg)

	received := &collector{}
	startConsumer(t, cfg, topic, received)

	publishValues(t, producer, topic, "rf3-first", "rf3-second")

	await(t, "обе записи дошли до обработчика", func() bool {
		return received.has("rf3-first") && received.has("rf3-second")
	})

	// Ассерт на составе ISR, а не только на доставке: тема с RF=3, у которой
	// реплики так и не догнали лидера, круг проходит точно так же, и
	// «репликация работает» от «репликация не начиналась» доставкой не
	// отличается.
	admin := newAdminAt(t, seeds)
	await(t, "все три реплики в ISR", func() bool {
		return len(partitionISR(t, admin, topic)) == clusterSize
	})
}

// clusterLeaderFailover — потеря лидера партиции под нагрузкой.
//
// Утверждения сценария три: продюсер продолжает получать подтверждения от
// НОВОГО лидера; всё, что новый лидер подтвердил, лежит в теме; консьюмер
// дочитывает тему до маркера, отправленного после аварии, то есть переживает
// и смену лидера, и переезд координатора группы.
//
// Про записи, подтверждённые в самом окне перевыборов, утверждения нет —
// см. развёрнутое обоснование ниже по тексту функции. Успеха от отправок,
// попавших в окно, тем более не требуется: acks=-1 обещает сохранность
// подтверждённого, а не подтверждение каждой попытки.
//
// Нагрузка идёт через всю аварию намеренно: продюсер с непустым буфером
// переживает смену лидера иначе, чем простаивающий, и на простаивающем
// сценарий проверял бы только то, что метаданные обновились.
func clusterLeaderFailover(t *testing.T, cluster *kafkaCluster) {
	t.Helper()

	seeds := cluster.seeds()
	topic := topicName(t)
	admin := newAdminAt(t, seeds)

	createTopicWith(t, admin, topic, 1, clusterSize, minISRConfig(2))

	cfg := configFor(t, seeds)
	producer := openProducer(t, cfg)

	received := &collector{}
	startConsumer(t, cfg, topic, received)

	// Круг до аварии: без него «после перевыборов ничего не приехало»
	// неотличимо от «консьюмер не работал никогда».
	publishValues(t, producer, topic, "before")
	await(t, "консьюмер работает до аварии", func() bool { return received.has("before") })

	// Вторая тема — зонд на смысл acks=-1. Она не терпит потери ни одной
	// реплики (min.insync.replicas равен фактору репликации), поэтому запись
	// в неё обязана пройти до аварии и обязана отказать после: продюсер,
	// который на самом деле просит подтверждения только у лидера или не
	// просит вовсе, разницы не заметит и пройдёт оба раза. Кластер здесь
	// незаменим — при RF=1 «все синхронные» и «лидер» это одна и та же
	// реплика.
	strict := topic + "-strict"
	createTopicWith(t, admin, strict, 1, clusterSize, minISRConfig(clusterSize))

	await(t, "у строгой темы все реплики синхронны", func() bool {
		return len(partitionISR(t, admin, strict)) == clusterSize
	})

	if err := producer.SendMessage(t.Context(), kafkax.PublishRequest{
		Topic: strict,
		Value: []byte("strict-before"),
	}); err != nil {
		t.Fatalf("запись в строгую тему до аварии: %v", err)
	}

	load := startLoad(t, producer, topic)

	leader := partitionLeader(t, admin, topic)
	if leader < 0 {
		t.Fatal("лидер партиции неизвестен до аварии: проверять нечего")
	}

	stopClusterNode(t, cluster, leader)

	// Админ через выживших: клиент, чей первый seed мёртв, тратит на него
	// бюджет каждого запроса, а запросов здесь по одному на виток await.
	survivors := newAdminAt(t, cluster.seedsExcept(leader))

	await(t, "партиция выбрала нового лидера", func() bool {
		got := partitionLeader(t, survivors, topic)

		return got >= 0 && got != leader
	})

	// Подтверждения ПОСЛЕ перевыборов — то, ради чего сценарий и написан.
	// Без этого ожидания «ничего не потеряли» доказывалось бы записями,
	// подтверждёнными до аварии, и тест зеленел бы на продюсере, который
	// после смены лидера умер молча.
	settled := load.ackedCount()

	await(t, "новый лидер подтверждает записи", func() bool {
		return load.ackedCount() > settled+3
	})

	// Реплик стало две, а строгая тема требует трёх: acks=-1 обязан отказать.
	if err := producer.SendMessage(t.Context(), kafkax.PublishRequest{
		Topic: strict,
		Value: []byte("strict-after"),
	}); err == nil {
		t.Fatal("запись в строгую тему прошла при неполном ISR: " +
			"продюсер не ждёт подтверждения всех синхронных реплик")
	}

	load.stop()

	// Проверяются записи, подтверждённые ПОСЛЕ подтверждённой смены лидера, а
	// не весь поток. Причина не в осторожности: на записях, подтверждённых в
	// самом окне перевыборов, потеря наблюдается — примерно в каждом восьмом
	// прогоне ровно одна подтверждённая запись в теме отсутствует, а одна
	// НЕподтверждённая в ней лежит.
	//
	// Механизм разобран и лежит ниже kafkax: брокер отвечает
	// not_leader_or_follower на батч, который уже дописал в лог, franz-go
	// исчерпывает лимит повторов, заваливает батч и переиспользует его
	// sequence number для следующей записи, а брокер отвечает на знакомый
	// номер успехом, ничего не записав. Воспроизведено на голом kgo с теми же
	// опциями — 3 отказа на 47 прогонов; измерения, логи и ссылки на исходник
	// в docs/audit/09-mutation-sweep.md, контракт наружу — в doc.go.
	//
	// Утверждать здесь то, чего стек не держит, значило бы завести мигающий
	// тест; утверждать это про окно после перевыборов — можно, и именно оно
	// отвечает на вопрос «продюсер пережил или молча умер».
	afterFailover := load.ackedValues()[settled:]
	if len(afterFailover) == 0 {
		t.Fatal("после перевыборов не подтверждено ни одной записи")
	}

	stored := readTopic(t, cluster.seedsExcept(leader), topic)

	for _, value := range afterFailover {
		if !slices.Contains(stored, value) {
			t.Fatalf("подтверждённая новым лидером запись %s в теме отсутствует "+
				"(подтверждено после перевыборов %d, в теме всего %d)",
				value, len(afterFailover), len(stored))
		}
	}

	// Маркер отправляется последним и последним же приезжает: партиция одна,
	// порядок внутри неё гарантирован, и воркер отдаёт записи обработчику по
	// порядку. Его получение поэтому означает, что консьюмер дочитал ВСЁ, что
	// лежит в теме до него, включая записи, попавшие в окно перевыборов, — то
	// есть перевыборы он пережил, а не пропустил.
	publishValues(t, producer, topic, "marker")

	await(t, "консьюмер дочитал тему до маркера", func() bool {
		return received.has("marker")
	})
}

// dedicatedCluster поднимает кластер и гасит его по окончании теста.
func dedicatedCluster(t *testing.T) *kafkaCluster {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), startTimeout)
	defer cancel()

	net := clusterNetwork(t, ctx)

	aliases := make([]string, clusterSize)
	ports := make([]int, clusterSize)
	voters := make([]string, clusterSize)

	for i := range clusterSize {
		aliases[i] = fmt.Sprintf("kafkax-node-%d", i+1)
		ports[i] = freeHostPort(t)
		voters[i] = fmt.Sprintf("%d@%s:9094", i+1, aliases[i])
	}

	quorum := strings.Join(voters, ",")
	cluster := &kafkaCluster{}

	for i := range clusterSize {
		port := ports[i]

		ctr, err := testcontainers.Run(ctx, clusterImage,
			testcontainers.WithEnv(clusterEnv(i+1, aliases[i], port, quorum)),
			testcontainers.WithExposedPorts(clusterPort),
			testcontainers.WithHostConfigModifier(func(hostConfig *container.HostConfig) {
				hostConfig.PortBindings = mobynet.PortMap{
					mobynet.MustParsePort(clusterPort): {{HostPort: strconv.Itoa(port)}},
				}
			}),
			tcnetwork.WithNetwork([]string{aliases[i]}, net))

		if ctr != nil {
			t.Cleanup(func() { terminateNode(t, ctr) })
		}

		if err != nil {
			brokerUnavailable(t, fmt.Errorf("узел %d кластера: %w", i+1, err))
		}

		cluster.nodes = append(cluster.nodes, clusterNode{
			id:        int32(i + 1),
			container: ctr,
			address:   "127.0.0.1:" + strconv.Itoa(port),
		})
	}

	awaitCluster(t, cluster)

	return cluster
}

// clusterNetwork создаёт сеть, в которой узлы видят друг друга по алиасу.
//
// Своя сеть, а не bridge по умолчанию: имена контейнеров резолвятся только в
// пользовательской сети, а межброкерный слушатель и кворум контроллеров
// адресуются именно именами — адреса контейнеров до подъёма неизвестны.
//
//revive:disable-next-line:context-as-argument ctx здесь бюджет подъёма, а не запроса
func clusterNetwork(t *testing.T, ctx context.Context) *testcontainers.DockerNetwork {
	t.Helper()

	net, err := tcnetwork.New(ctx)
	if err != nil {
		brokerUnavailable(t, fmt.Errorf("сеть кластера: %w", err))
	}

	// Сеть снимается последней: Cleanup идёт в обратном порядке регистрации, а
	// удалить сеть с подключёнными контейнерами docker не даст.
	t.Cleanup(func() {
		removeCtx, removeCancel := context.WithTimeout(context.Background(), time.Minute)
		defer removeCancel()

		if err := net.Remove(removeCtx); err != nil {
			t.Errorf("не удалось удалить сеть кластера: %v", err)
		}
	})

	return net
}

// clusterEnv — настройки узла.
//
// Реплицированы и служебные темы: при RF=1 координатор группы и оффсеты жили
// бы на одном узле, и остановка именно его роняла бы сценарий по причине, к
// перевыборам лидера партиции отношения не имеющей.
func clusterEnv(id int, alias string, hostPort int, quorum string) map[string]string {
	return map[string]string{
		"CLUSTER_ID":                           clusterID,
		"KAFKA_NODE_ID":                        strconv.Itoa(id),
		"KAFKA_PROCESS_ROLES":                  "broker,controller",
		"KAFKA_LISTENERS":                      "PLAINTEXT://0.0.0.0:9093,BROKER://0.0.0.0:9092,CONTROLLER://0.0.0.0:9094",
		"KAFKA_ADVERTISED_LISTENERS":           fmt.Sprintf("PLAINTEXT://127.0.0.1:%d,BROKER://%s:9092", hostPort, alias),
		"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
		"KAFKA_INTER_BROKER_LISTENER_NAME":     "BROKER",
		"KAFKA_CONTROLLER_LISTENER_NAMES":      "CONTROLLER",
		"KAFKA_CONTROLLER_QUORUM_VOTERS":       quorum,

		"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         strconv.Itoa(clusterSize),
		"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": strconv.Itoa(clusterSize),
		"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "2",
		"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",
	}
}

// awaitCluster ждёт, пока все узлы окажутся в метаданных.
//
// Именно метаданные, а не строчка в логе контейнера: узел, доложивший о
// старте, ещё не обязан быть виден кворуму, а сценарию нужен собранный
// кластер, а не три запущенных процесса.
func awaitCluster(t *testing.T, cluster *kafkaCluster) {
	t.Helper()

	admin := newAdminAt(t, cluster.seeds())

	await(t, "все узлы кластера в метаданных", func() bool {
		// Свой короткий бюджет: без него один запрос к ещё не поднявшемуся
		// узлу съел бы весь потолок await.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		meta, err := admin.Metadata(ctx)

		return err == nil && len(meta.Brokers) == clusterSize
	})
}

// partitionLeader отдаёт лидера нулевой партиции темы; -1 означает «неизвестен».
//
// Отсутствие лидера — значение, а не отказ теста: метод зовётся из await в
// цикле, и на витке сразу после остановки узла лидера нет ни у кого.
func partitionLeader(t *testing.T, admin *kadm.Client, topic string) int32 {
	t.Helper()

	detail, ok := partitionDetail(t, admin, topic)
	if !ok {
		return -1
	}

	return detail.Leader
}

// partitionISR отдаёт список синхронных реплик нулевой партиции темы.
func partitionISR(t *testing.T, admin *kadm.Client, topic string) []int32 {
	t.Helper()

	detail, ok := partitionDetail(t, admin, topic)
	if !ok {
		return nil
	}

	return detail.ISR
}

func partitionDetail(t *testing.T, admin *kadm.Client, topic string) (kadm.PartitionDetail, bool) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	meta, err := admin.Metadata(ctx, topic)
	if err != nil {
		return kadm.PartitionDetail{}, false
	}

	topicDetail, ok := meta.Topics[topic]
	if !ok || topicDetail.Err != nil {
		return kadm.PartitionDetail{}, false
	}

	detail, ok := topicDetail.Partitions[0]
	if !ok || detail.Err != nil {
		return kadm.PartitionDetail{}, false
	}

	return detail, true
}

// stopClusterNode останавливает узел с заданным id.
func stopClusterNode(t *testing.T, cluster *kafkaCluster, id int32) {
	t.Helper()

	index := slices.IndexFunc(cluster.nodes, func(node clusterNode) bool { return node.id == id })
	if index < 0 {
		t.Fatalf("узла %d в кластере нет", id)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	timeout := 10 * time.Second
	if err := cluster.nodes[index].container.Stop(ctx, &timeout); err != nil {
		t.Fatalf("остановка узла %d: %v", id, err)
	}
}

func terminateNode(t *testing.T, ctr testcontainers.Container) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	if err := ctr.Terminate(ctx); err != nil {
		t.Errorf("не удалось погасить узел кластера: %v", err)
	}
}
