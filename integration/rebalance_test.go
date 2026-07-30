package integration

// rebalance_test.go — семантика группы против настоящего координатора:
// кооперативное перераспределение партиций между экземплярами, уход участника
// из группы, продолжение с закоммиченного оффсета и сохранность потока при
// ребалансе на ходу.
//
// Граница с соседями. roundtrip_test.go отвечает за то, что круг
// «отправили — приняли — закоммитили» вообще работает, и заодно за исправность
// обвязки. Здесь всё держится на том, что участников больше одного, и все
// утверждения о назначении партиций берутся у координатора, а не выводятся из
// доставки.

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
)

// TestCooperativeRebalanceSplitsPartitionsBetweenInstances ловит дефекты
// кооперативного перераспределения партиций между двумя живыми экземплярами.
//
// Класс дефекта. Балансировщик franz-go по умолчанию — cooperative-sticky, и
// колбэк назначения получает только ДОБАВЛЕННЫЕ партиции, а не полный
// assignment. Любая логика, читающая этот список как «вот всё, что у меня
// есть», промахивается мимо партиций, оставшихся за экземпляром, — ровно на
// этом пакет уже обжигался (RF-KAFKA-03: пауза с отравленной партиции не
// снималась до конца жизни процесса). Отказы этого семейства тихие: ошибок
// нет, метрики зелёные, растёт только лаг. kfake второго раунда кооперативного
// протокола не строит и группу между двумя клиентами по-настоящему не делит,
// поэтому против него «партиции разъехались» и «второй присоединился и остался
// ни с чем» неотличимы.
//
// Почему ассерты такие. Распределение спрашивается у координатора через
// DescribeGroups, а не выводится из того, кто какие сообщения получил:
// доставка — следствие назначения, она отстаёт от него и на партиции без
// записей не наступает вовсе. Продолжение обработки у обоих проверяется
// отдельной волной, отправленной уже ПОСЛЕ подтверждённого сплита: до него
// любое распределение волны объясняется чем угодно, включая «первый ещё держит
// всё». Сохранность потока проверяется по объединению двух наборов, а не по
// каждому в отдельности: дубликат при ребалансе законен (гарантия
// at-least-once), пропажа — нет.
func TestCooperativeRebalanceSplitsPartitionsBetweenInstances(t *testing.T) {
	t.Parallel()

	const partitions = 4

	topic := newTopic(t, partitions)
	cfg := testConfig(t)
	admin := newAdmin(t)
	producer := openProducer(t, cfg)

	settled := waveValues("settled", 24)
	publishValues(t, producer, topic, settled...)

	ends := requireFedEveryPartition(t, admin, topic, nil)

	first := &collector{}
	startConsumer(t, cfg, topic, first)

	// Исходное состояние фиксируется явно: без него утверждение «партиции
	// разъехались» опиралось бы на предположение, что до второго экземпляра они
	// все были у первого.
	awaitAssignment(t, admin, cfg.Consumer.Group, topic, 1, partitions)

	await(t, "первый экземпляр обработал волну до ребаланса", func() bool {
		return len(missingValues(settled, first.snapshot())) == 0
	})

	second := &collector{}
	startConsumer(t, cfg, topic, second)

	split := awaitAssignment(t, admin, cfg.Consumer.Group, topic, 2, partitions)

	// Волна после сплита: обе стороны обязаны и получить свои партиции, и
	// продолжить обработку — до сих пор второй экземпляр мог не сделать ни
	// одного успешного цикла опроса.
	shared := waveValues("shared", 24)
	publishValues(t, producer, topic, shared...)
	requireFedEveryPartition(t, admin, topic, ends)

	await(t, "волна после ребаланса дошла целиком", func() bool {
		return len(missingValues(shared, first.snapshot(), second.snapshot())) == 0
	})

	firstShare := countWithPrefix(first.snapshot(), "shared")
	secondShare := countWithPrefix(second.snapshot(), "shared")

	if firstShare == 0 || secondShare == 0 {
		t.Fatalf("после ребаланса обработка идёт не у обоих: первый взял %d сообщений, второй %d "+
			"при назначении %v — партиции разъехались, а поток за ними не пошёл",
			firstShare, secondShare, split)
	}

	if lost := missingValues(settled, first.snapshot(), second.snapshot()); len(lost) > 0 {
		t.Fatalf("ребаланс потерял сообщения долевой волны: %v", lost)
	}
}

// TestLeavingInstanceHandsOffPartitionsWithoutRedelivery ловит потерю
// финального коммита при штатном уходе экземпляра из группы.
//
// Класс дефекта. Уход участника — это две разные обязанности: отдать партиции
// (иначе группа простаивает до истечения session timeout) и закоммитить всё
// обработанное (иначе преемник перечитает чужой хвост). Вторая ломается молча и
// именно в проде: дубликаты после каждого рестарта пода списывают на
// at-least-once, хотя по контракту пакета Stop обязан довести отмеченные
// оффсеты до координатора. Против kfake это не воспроизводится: оффсеты там
// живут в памяти эмулятора и не проходят ни через __consumer_offsets, ни через
// смену владельца партиции.
//
// Почему ассерты такие. CommitInterval поднят до минуты, чтобы фоновый тикер
// автокоммита не сделал работу за завершение: с умолчанием в секунду тест
// зеленел бы и на вырезанном финальном коммите, просто потому что успел бы
// сработать таймер. Переезд партиций спрашивается у координатора, а доставка
// после переезда проверяется отдельной волной — «второй что-то получил» само по
// себе совместимо с тем, что он читает свои прежние партиции. Отсутствие
// повторной доставки формулируется как пересечение снимка первого экземпляра со
// снимком второго: ровно те значения, за которые первый успел отчитаться, и
// никакие другие.
func TestLeavingInstanceHandsOffPartitionsWithoutRedelivery(t *testing.T) {
	t.Parallel()

	const partitions = 4

	topic := newTopic(t, partitions)
	cfg := testConfig(t)
	cfg.Consumer.CommitInterval = time.Minute

	admin := newAdmin(t)
	producer := openProducer(t, cfg)

	first := &collector{}
	leaving := startConsumer(t, cfg, topic, first)

	second := &collector{}
	startConsumer(t, cfg, topic, second)

	awaitAssignment(t, admin, cfg.Consumer.Group, topic, 2, partitions)

	before := waveValues("before", 24)
	publishValues(t, producer, topic, before...)

	ends := requireFedEveryPartition(t, admin, topic, nil)

	await(t, "волна до ухода обработана обоими экземплярами", func() bool {
		return len(missingValues(before, first.snapshot(), second.snapshot())) == 0
	})

	// Снимок берётся до Stop: именно за эти значения уходящий экземпляр
	// отчитался, и именно они не должны приехать преемнику.
	done := first.snapshot()
	if len(done) == 0 {
		t.Fatal("уходящий экземпляр не обработал ни одного сообщения: " +
			"проверять повторную доставку не на чем")
	}

	if err := leaving.Stop(); err != nil {
		t.Fatalf("Stop уходящего экземпляра: %v", err)
	}

	awaitAssignment(t, admin, cfg.Consumer.Group, topic, 1, partitions)

	after := waveValues("after", 24)
	publishValues(t, producer, topic, after...)
	requireFedEveryPartition(t, admin, topic, ends)

	await(t, "оставшийся экземпляр обработал волну в переехавших партициях", func() bool {
		return len(missingValues(after, second.snapshot())) == 0
	})

	if again := commonValues(done, second.snapshot()); len(again) > 0 {
		t.Fatalf("оставшийся экземпляр перечитал %d сообщений, обработанных ушедшим (%v): "+
			"финальный коммит Stop не дошёл до координатора", len(again), again)
	}
}

// TestFreshInstanceResumesFromCommittedOffset ловит расхождение между тем, что
// пакет считает обработанным, и тем, что о группе знает брокер.
//
// Класс дефекта. Всё, что стоит между MarkCommitRecords и записью в
// __consumer_offsets, — отметка не той записи, коммит с пустым бюджетом,
// проглоченная ошибка коммита — проявляется одинаково: процесс перезапустился и
// перечитал уже сделанное. В памяти kfake этот путь короче настоящего на
// координатора группы целиком, поэтому здесь он и проверяется.
//
// Почему ассерты такие. Оффсет читается у брокера через kadm.FetchOffsets и
// сверяется с точным числом, а не с «читать нечего»: отсутствие повторной
// доставки — косвенное следствие, совместимое и с коммитом не туда (оффсет
// уехал за необработанное — сообщения тоже не приедут, но они потеряны).
// Прямое число различает эти два исхода. Тема на одну партицию выбрана ради
// того же: при нескольких партициях оффсет распадается на набор и точное
// утверждение приходится заменять суммой. CommitInterval поднят до минуты,
// чтобы за коммит отвечало завершение, а не фоновый тикер.
func TestFreshInstanceResumesFromCommittedOffset(t *testing.T) {
	t.Parallel()

	const batch = 5

	topic := newTopic(t, 1)
	cfg := testConfig(t)
	cfg.Consumer.CommitInterval = time.Minute

	admin := newAdmin(t)
	producer := openProducer(t, cfg)

	processed := waveValues("processed", batch)
	publishValues(t, producer, topic, processed...)

	first := &collector{}
	stopped := startConsumer(t, cfg, topic, first)

	await(t, "первый экземпляр обработал первую партию", func() bool {
		return first.count() >= batch
	})

	if err := stopped.Stop(); err != nil {
		t.Fatalf("Stop первого экземпляра: %v", err)
	}

	// Без ожидания: CommitMarkedOffsets внутри Stop синхронен, и к моменту
	// возврата координатор уже подтвердил запись. Ожидание здесь скрыло бы
	// разницу между «коммит опоздал» и «коммита не было».
	if got := committedOffset(t, admin, cfg.Consumer.Group, topic, 0); got != batch {
		t.Fatalf("после Stop у группы закоммичен оффсет %d, want %d: "+
			"брокер не знает об обработанной партии", got, batch)
	}

	tail := waveValues("tail", batch)
	publishValues(t, producer, topic, tail...)

	second := &collector{}
	resumed := startConsumer(t, cfg, topic, second)

	await(t, "перезапущенный экземпляр обработал хвост", func() bool {
		return second.count() >= batch
	})

	// Порядок гарантирован: партиция одна, а значит и сравнение со срезом
	// законно — при чтении с начала здесь окажется "processed-000" первым
	// элементом, и отказ сразу назовёт причину.
	if got := second.snapshot(); !slices.Equal(got, tail) {
		t.Fatalf("перезапущенный экземпляр получил %v, want %v: чтение началось не с "+
			"закоммиченного места", got, tail)
	}

	if err := resumed.Stop(); err != nil {
		t.Fatalf("Stop перезапущенного экземпляра: %v", err)
	}

	if got := committedOffset(t, admin, cfg.Consumer.Group, topic, 0); got != 2*batch {
		t.Fatalf("после второго Stop у группы закоммичен оффсет %d, want %d", got, 2*batch)
	}
}

// TestNoMessageLostWhenRebalanceHitsLiveStream ловит потерю сообщений на стыке
// отзыва и назначения партиций, когда поток не останавливается.
//
// Класс дефекта. Ребаланс на живом потоке — это одновременная работа трёх
// путей: воркеры отдаваемых партиций дренируются, отмеченные оффсеты
// коммитятся, новый владелец начинает читать с зафиксированного места. Ошибка в
// порядке между ними — коммит раньше дренажа, дренаж без коммита, потерянная
// отметка последней записи батча — даёт ровно одну наблюдаемую примету:
// несколько сообщений не получил никто. Ни на остановленном потоке, ни на kfake
// это окно не открывается.
//
// Почему ассерты такие. Проверяется покрытие, а не совпадение: контракт пакета
// — at-least-once, и требовать отсутствия дубликатов значило бы проверять
// exactly-once, которого пакет не даёт и не обещает. Поэтому ассерт
// формулируется как «в объединении двух наборов нет пропаж», а повторы
// сознательно не считаются отказом. Третья волна отправляется уже после
// подтверждённого координатором сплита: без неё «второй экземпляр ничего не
// получил» было бы законным исходом — первый успевает вычитать весь поток
// раньше, чем кооперативный протокол доходит до второго раунда, и тест
// выродился бы в проверку одного экземпляра.
func TestNoMessageLostWhenRebalanceHitsLiveStream(t *testing.T) {
	t.Parallel()

	const (
		partitions = 4
		waveSize   = 100
	)

	topic := newTopic(t, partitions)
	cfg := testConfig(t)
	admin := newAdmin(t)
	producer := openProducer(t, cfg)

	first := &collector{}
	startConsumer(t, cfg, topic, first)

	head := waveValues("head", waveSize)
	publishValues(t, producer, topic, head...)

	ends := requireFedEveryPartition(t, admin, topic, nil)

	await(t, "первый экземпляр вошёл в поток", func() bool {
		return first.count() > 0
	})

	// Start не блокирует: к моменту возврата ребаланс только начат, и следующая
	// волна льётся ровно в то окно, ради которого тест написан.
	second := &collector{}
	startConsumer(t, cfg, topic, second)

	during := waveValues("during", waveSize)
	publishValues(t, producer, topic, during...)
	ends = requireFedEveryPartition(t, admin, topic, ends)

	awaitAssignment(t, admin, cfg.Consumer.Group, topic, 2, partitions)

	tail := waveValues("tail", waveSize)
	publishValues(t, producer, topic, tail...)
	requireFedEveryPartition(t, admin, topic, ends)

	stream := slices.Concat(head, during, tail)

	await(t, "весь поток получен хотя бы одним из экземпляров", func() bool {
		return len(missingValues(stream, first.snapshot(), second.snapshot())) == 0
	})

	if second.count() == 0 {
		t.Fatal("второй экземпляр не обработал ни одного сообщения: " +
			"ребаланс не состоялся, и поток проверен без него")
	}

	// Дубликаты законны, но их число — единственное, что отличает «ребаланс
	// прошёл дёшево» от «весь поток перечитан заново», и в отказе соседнего
	// ассерта это первое, что хочется знать.
	total := first.count() + second.count()
	t.Logf("поток из %d сообщений обработан %d раз(а): %d дубликатов",
		len(stream), total, total-len(stream))
}

// waveValues строит волну значений с общим префиксом.
//
// Порядковый номер дополнен нулями, чтобы лексикографический порядок совпадал с
// числовым: снимки обработчиков попадают в сообщения об отказе как есть, и
// «tail-10» между «tail-1» и «tail-2» читать невозможно.
func waveValues(prefix string, n int) []string {
	values := make([]string, n)
	for i := range values {
		values[i] = fmt.Sprintf("%s-%03d", prefix, i)
	}

	return values
}

// groupAssignment спрашивает координатора, как партиции темы разложены по
// участникам группы: идентификатор участника — его партиции.
//
// Нестабильная группа отдаётся пустым результатом, а не ошибкой: во время
// ребаланса это нормальное промежуточное состояние, и вызывающий его
// переспрашивает. Участник без единой партиции в результат не попадает — на нём
// и стоит проверка «все ли получили свою долю».
//
// Ошибка возвращается, а не валит тест на месте: первые обращения к только что
// созданной группе штатно отвечают COORDINATOR_NOT_AVAILABLE — координатор
// назначается лениво, при первом join. Отличить это от настоящей поломки можно
// только по тому, повторяется ли отказ до конца бюджета ожидания, а такой счёт
// ведёт вызывающий.
func groupAssignment(
	t *testing.T, admin *kadm.Client, group, topic string,
) (map[string][]int32, error) {
	t.Helper()

	described, err := admin.DescribeGroups(t.Context(), group)
	if err != nil {
		return nil, fmt.Errorf("DescribeGroups(%s): %w", group, err)
	}

	current, ok := described[group]
	if !ok || current.State != "Stable" {
		return nil, nil
	}

	if current.Err != nil {
		return nil, fmt.Errorf("описание группы %s: %w", group, current.Err)
	}

	assignment := make(map[string][]int32, len(current.Members))

	for _, member := range current.Members {
		consumer, ok := member.Assigned.AsConsumer()
		if !ok {
			continue
		}

		for _, assigned := range consumer.Topics {
			if assigned.Topic == topic && len(assigned.Partitions) > 0 {
				assignment[member.MemberID] = assigned.Partitions
			}
		}
	}

	return assignment, nil
}

// awaitAssignment ждёт, пока координатор разложит все партиции темы ровно по
// members участникам, и отдаёт получившееся распределение.
//
// Сумма партиций проверяется вместе с числом участников: при кооперативном
// протоколе между раундами существует стабильное состояние, в котором отданные
// партиции уже сняты с прежнего владельца, но новому ещё не выданы, и одна
// проверка без другой приняла бы его за конечное.
func awaitAssignment(
	t *testing.T, admin *kadm.Client, group, topic string, members int, partitions int32,
) map[string][]int32 {
	t.Helper()

	var (
		settled  map[string][]int32
		reported string
	)

	await(t, fmt.Sprintf("координатор разложил %d партиций темы %s по %d участникам группы",
		partitions, topic, members), func() bool {
		current, err := groupAssignment(t, admin, group, topic)
		if err != nil {
			// Повторяющийся отказ печатается один раз: опрос идёт двадцать раз в
			// секунду, и без дедупа причина утонула бы в собственном повторе.
			if text := err.Error(); text != reported {
				reported = text
				t.Logf("координатор пока не отвечает: %s", text)
			}

			return false
		}

		if len(current) != members {
			return false
		}

		var total int
		for _, owned := range current {
			total += len(owned)
		}

		if total != int(partitions) {
			return false
		}

		settled = current

		return true
	})

	return settled
}

// partitionEnds отдаёт конечные оффсеты всех партиций темы.
func partitionEnds(t *testing.T, admin *kadm.Client, topic string) map[int32]int64 {
	t.Helper()

	listed, err := admin.ListEndOffsets(t.Context(), topic)
	if err != nil {
		t.Fatalf("ListEndOffsets(%s): %v", topic, err)
	}

	ends := make(map[int32]int64)

	listed.Each(func(offset kadm.ListedOffset) {
		if offset.Err != nil {
			t.Errorf("конечный оффсет %s/%d: %v", offset.Topic, offset.Partition, offset.Err)

			return
		}

		ends[offset.Partition] = offset.Offset
	})

	return ends
}

// requireFedEveryPartition требует, чтобы очередная волна задела все партиции
// темы, и отдаёт новые конечные оффсеты для следующей проверки.
//
// Это проверка теста, а не пакета. Сценарии здесь держатся на том, что данные
// пришли в партиции обоих участников; промах волны мимо партиции превратил бы
// отказ в неправду — «второй экземпляр ничего не обработал» вместо «второму
// экземпляру нечего было обрабатывать». Партиционер детерминирован, а ключи
// фиксированы, поэтому промах означал бы ровно одно: набор ключей пора менять.
func requireFedEveryPartition(
	t *testing.T, admin *kadm.Client, topic string, before map[int32]int64,
) map[int32]int64 {
	t.Helper()

	after := partitionEnds(t, admin, topic)

	for partition, end := range after {
		if end <= before[partition] {
			t.Fatalf("волна не задела партицию %s/%d: конечный оффсет так и остался %d — "+
				"подберите другой набор ключей", topic, partition, end)
		}
	}

	return after
}

// missingValues возвращает значения из sent, не встретившиеся ни в одном из
// полученных наборов.
//
// Объединение, а не сверка каждого набора по отдельности: при ребалансе
// сообщение законно достаётся любому из участников, и единственное, что
// является отказом, — если оно не досталось никому.
func missingValues(sent []string, received ...[]string) []string {
	seen := make(map[string]struct{})

	for _, values := range received {
		for _, value := range values {
			seen[value] = struct{}{}
		}
	}

	var lost []string

	for _, value := range sent {
		if _, ok := seen[value]; !ok {
			lost = append(lost, value)
		}
	}

	return lost
}

// commonValues возвращает значения, встретившиеся в обоих наборах.
func commonValues(left, right []string) []string {
	inLeft := make(map[string]struct{}, len(left))
	for _, value := range left {
		inLeft[value] = struct{}{}
	}

	var both []string

	for _, value := range right {
		if _, ok := inLeft[value]; ok {
			both = append(both, value)
		}
	}

	return both
}

// countWithPrefix считает значения нужной волны в снимке обработчика.
func countWithPrefix(values []string, prefix string) int {
	var n int

	for _, value := range values {
		if strings.HasPrefix(value, prefix) {
			n++
		}
	}

	return n
}
