package kafkax

import (
	"fmt"
	"os"
	"strings"
)

// Отставленные ключи конфигурации.
//
// Мажорная версия даёт право сломать имя поля, и v3 этим правом пользуется:
// два консьюмерских ключа сменили не только имя, но и смысл значения —
// message_queue_size считал сообщения, а стал считать батчи; handler_max_retries
// понимал ноль как «повторять бесконечно», а стал понимать его как «не
// повторять». Одного переименования мало: cleanenv молча игнорирует и
// незнакомый yaml-ключ, и незнакомую переменную окружения, поэтому перенесённый
// из v1/v2 конфиг просто потерял бы настройку и взял умолчание. Разница между
// «потерял настройку» и «сменил единицу измерения» видна не по логам, а по
// потолку памяти в десятки гигабайт.
//
// Поэтому старые имена не исчезают, а становятся отказом на старте с текстом,
// который называет замену и суть изменения.

// Имена секций конфигурации в файле.
//
// Константами, а не литералом на каждую запись: секция — один и тот же
// идентификатор, повторённый полтора десятка раз, и он обязан совпадать с
// yaml-тегом соответствующего поля Config. Самоподтверждения здесь нет —
// проверяемая величина в записи не секция, а имя отставленного ключа, и живые
// ключи собираются рефлексией по тегам, а не из этих констант.
const (
	yamlSectionProducer = "producer"
	yamlSectionConsumer = "consumer"
	yamlSectionTLS      = "tls"
)

// retiredKey — ключ конфигурации, снятый с употребления.
//
// Хранится сразу в двух формах записи, потому что задать настройку можно двумя
// способами и оба надо перехватить: yamlPath — путь в файле («consumer»,
// «message_queue_size»), env — имя переменной окружения. Выводить второе из
// первого нельзя: у отставленного ключа нет поля в структуре, а значит нет и
// Go-пути, из которого envName собирает имя переменной.
type retiredKey struct {
	yamlPath []string
	env      string
	// change — что именно изменилось, а не факт устаревания. «Ключ устарел»
	// оставляет читателя с вопросом, на что менять и можно ли перенести
	// значение; здесь названы и замена, и смена смысла, и последствие переноса
	// значения как есть.
	change string
}

// retiredKeys — полный список отставленных ключей.
//
// Список, а не проверка на каждый ключ россыпью: обе точки перехвата (файл и
// окружение) обходят его целиком, поэтому новая запись закрывает сразу оба
// пути, и забыть одну из половин невозможно.
//
// Записи не удаляются вместе с выходом следующей мажорной версии: ценность
// отказа в том, что он встречает конфиг, пролежавший в репозитории годы.
var retiredKeys = []retiredKey{
	{
		yamlPath: []string{yamlSectionConsumer, "message_queue_size"},
		env:      "KAFKAX_CONSUMER_MESSAGE_QUEUE_SIZE",
		change: "renamed to consumer.message_queue_batches (env KAFKAX_CONSUMER_MESSAGE_QUEUE_BATCHES) " +
			"and the unit changed from messages to batches: v1 buffered single messages, v3 buffers whole " +
			"poll batches, so a value carried over as is raises the memory ceiling " +
			"(partitions x batches x max_partition_bytes) by up to max_poll_records times; " +
			"pick a batch count, the default is 100",
	},
	{
		yamlPath: []string{yamlSectionConsumer, "handler_max_retries"},
		env:      "KAFKAX_CONSUMER_HANDLER_MAX_RETRIES",
		change: "renamed to consumer.handler_retries (env KAFKAX_CONSUMER_HANDLER_RETRIES) " +
			"and the meaning of 0 is inverted: in v1 zero meant retry forever and the value counted " +
			"handler calls, in v3 zero means no retries at all, N means N retries on top of the first " +
			"call and -1 means retry forever",
	},

	// Ниже — ключи v1, исчезнувшие вместе с переходом на franz-go. Смысла они
	// не меняли, потому что их больше нет вовсе, и тем они опаснее: настройка,
	// сменившая смысл, хотя бы продолжает существовать, а исчезнувшая просто
	// перестаёт действовать, не оставляя следа. Первый из них — прямая дыра в
	// безопасности, остальные меняют поведение или не меняют ничего, и
	// разбираться, который тут который, должен не читатель, а этот список.
	{
		yamlPath: []string{"security_protocol"},
		env:      "KAFKAX_SECURITY_PROTOCOL",
		change: "retired without a replacement: v3 derives the protocol from the settings themselves " +
			"instead of naming it, so TLS is on when the tls section (or Config.TLSConfig) is filled " +
			"and SASL is on when sasl.mechanism is set. Carrying this key over is the dangerous case: " +
			"a config that said SASL_SSL but has no tls section connects in PLAINTEXT, and nothing " +
			"reports it. Fill the tls and sasl sections to state the same intent",
	},
	{
		yamlPath: []string{"clientid"},
		// Переменной окружения у этого ключа нет: имя KAFKAX_CLIENT_ID не
		// менялось между версиями, менялось только имя в файле.
		change: "renamed to client_id: v1 gave the field no yaml tag, so the key was the field name " +
			"lowercased. Carrying the old spelling over leaves ClientID empty, and since it is " +
			"required the load fails anyway — but with 'ClientID is required' instead of the reason",
	},
	{
		yamlPath: []string{yamlSectionConsumer, "enable_auto_commit"},
		env:      "KAFKAX_CONSUMER_ENABLE_AUTO_COMMIT",
		change: "retired without a replacement: v3 always commits marked offsets and marks a record " +
			"only after the handler returned nil (AutoCommitMarks), which is the at-least-once " +
			"guarantee of the package and not a tunable. consumer.commit_interval sets how often the " +
			"marked offsets are flushed",
	},
	{
		yamlPath: []string{yamlSectionProducer, "batch_timeout"},
		env:      "KAFKAX_PRODUCER_BATCH_TIMEOUT",
		change: "retired as a duplicate: it was librdkafka's queue.buffering.max.ms, which is the same " +
			"knob as linger.ms, and v1 exposed both under two names. v3 keeps one — producer.linger " +
			"(env KAFKAX_PRODUCER_LINGER). Mind the value: v1 defaulted batch_timeout to 1s and linger " +
			"to 0s, so which of the two won depended on which one you had filled in",
	},
	{
		yamlPath: []string{yamlSectionProducer, "batch_size"},
		env:      "KAFKAX_PRODUCER_BATCH_SIZE",
		change: "retired without a replacement: it capped a batch by RECORD COUNT " +
			"(librdkafka batch.num.messages), and franz-go caps a batch by size only. Use " +
			"producer.batch_bytes (env KAFKAX_PRODUCER_BATCH_BYTES), which v1 also had, and drop " +
			"the count",
	},
	{
		yamlPath: []string{yamlSectionConsumer, "max_poll_interval"},
		env:      "KAFKAX_CONSUMER_MAX_POLL_INTERVAL",
		change: "renamed to consumer.rebalance_timeout (env KAFKAX_CONSUMER_REBALANCE_TIMEOUT): both " +
			"bound how long the group waits for this member before evicting it, but v3 spends that " +
			"budget on the rebalance callback rather than on the gap between polls",
	},
	{
		yamlPath: []string{yamlSectionConsumer, "socket_timeout"},
		env:      "KAFKAX_CONSUMER_SOCKET_TIMEOUT",
		change: "renamed to dial_timeout (env KAFKAX_DIAL_TIMEOUT): the budget for establishing a " +
			"TCP/TLS connection to a broker. Timeouts of individual requests are franz-go's own and " +
			"are not exposed as one setting",
	},
	{
		yamlPath: []string{yamlSectionTLS, "identification_algorithm"},
		env:      "KAFKAX_TLS_IDENTIFICATION_ALGORITHM",
		change: "retired without a replacement: it existed to switch librdkafka's hostname " +
			"verification on and off. Go's TLS stack always verifies the hostname, and the only way " +
			"to opt out is tls.insecure_skip_verify, which the package warns about at startup. Use " +
			"tls.server_name when the broker's certificate names a host other than the one you dial",
	},
	{
		yamlPath: []string{yamlSectionConsumer, "read_timeout"},
		env:      "KAFKAX_CONSUMER_READ_TIMEOUT",
		change: "retired without a replacement: v1 polled one message at a time and needed a bound " +
			"on that call. v3 polls whole batches and blocks until a batch arrives or the consumer " +
			"stops; consumer.max_wait bounds how long the broker holds a fetch waiting for data",
	},
	{
		yamlPath: []string{yamlSectionConsumer, "read_error_backoff"},
		env:      "KAFKAX_CONSUMER_READ_ERROR_BACKOFF",
		change: "retired without a replacement: the pause after a failed poll belongs to franz-go " +
			"and is not exposed. consumer.handler_retry_delay is a different thing — the pause " +
			"between handler retries",
	},
	{
		yamlPath: []string{yamlSectionProducer, "message_queue_size"},
		env:      "KAFKAX_PRODUCER_MESSAGE_QUEUE_SIZE",
		change: "retired without a replacement: it sized v1's own per-tenant queue in front of the " +
			"client, and SendMessage blocked when that queue filled up. v3 has no queue of its own — " +
			"SendMessage calls franz-go directly, and the buffer that can fill up is the client's, " +
			"bounded by producer.max_buffered_records and producer.max_buffered_bytes. Note this key " +
			"also existed under consumer, where it meant something else again",
	},
	{
		yamlPath: []string{yamlSectionProducer, "inactive_worker_ttl"},
		env:      "KAFKAX_PRODUCER_INACTIVE_WORKER_TTL",
		change: "retired without a replacement: v1 kept a goroutine per tenant and had to reap idle " +
			"ones. v3 has no queue of its own in front of the client — SendMessage calls franz-go " +
			"directly, so there is nothing to reap",
	},
	{
		yamlPath: []string{yamlSectionProducer, "cleanup_worker_interval"},
		env:      "KAFKAX_PRODUCER_CLEANUP_WORKER_INTERVAL",
		change: "retired without a replacement: it set how often idle tenant workers were reaped, " +
			"and v3 has no such workers",
	},

	// В v1 обе воркерные настройки стояли и в секции продюсера, и в секции
	// консьюмера — одинаковые имена в разных секциях. Перехватывать надо обе,
	// иначе половина перенесённого конфига пройдёт молча.
	{
		yamlPath: []string{yamlSectionConsumer, "inactive_worker_ttl"},
		env:      "KAFKAX_CONSUMER_INACTIVE_WORKER_TTL",
		change: "retired without a replacement: v1 reaped a partition worker that had been idle for " +
			"this long. In v3 a worker lives exactly as long as the partition is assigned to this " +
			"consumer, so idleness is not a reason to stop one",
	},
	{
		yamlPath: []string{yamlSectionConsumer, "cleanup_worker_interval"},
		env:      "KAFKAX_CONSUMER_CLEANUP_WORKER_INTERVAL",
		change: "retired without a replacement: it set how often idle partition workers were reaped, " +
			"and v3 ties a worker's life to the partition assignment instead",
	},
}

// yamlKey — путь ключа в файле конфигурации, как его пишут в yaml.
func (k retiredKey) yamlKey() string {
	return strings.Join(k.yamlPath, ".")
}

// err собирает претензию к отставленному ключу. source — как ключ записан там,
// где его нашли: yaml-путь для файла, имя переменной для окружения. Разные
// формы записи в одном тексте только сбивали бы с толку — читатель правит тот
// источник, из которого пришёл ключ, а замену в обеих формах называет change.
func (k retiredKey) err(source string) error {
	return fmt.Errorf("%s is retired: %s", source, k.change)
}

// retiredEnvErrors ищет отставленные ключи в переменных окружения.
//
// Вызывается из commonErrors — единственного места, через которое проходят все
// три входа в валидацию (Validate, validateProducer, validateConsumer), а
// значит и оба конструктора. Обойти проверку, не обойдя валидацию целиком,
// нельзя.
//
// Секция ключа роли не играет: продюсерский конструктор ругается и на
// консьюмерский ключ. Переменная в окружении процесса — это утверждение
// оператора «настройка действует», и утверждение ложно независимо от того,
// какую роль процесс играет. Цена — сервис, которому досталась общая на всё
// развёртывание карта переменных, чинит её целиком, а не по частям; молчание
// стоило бы дороже, потому что второго повода узнать об отставленном ключе у
// него не будет.
//
// Значение переменной не разбирается: пустая KAFKAX_CONSUMER_MESSAGE_QUEUE_SIZE=
// — такое же заявление о настройке, как и заполненная.
func retiredEnvErrors() []error {
	var errs []error

	for _, key := range retiredKeys {
		// Пустое имя — «переменной у этого ключа не было»: менялось имя только
		// в файле. Пропуск здесь явный, потому что os.LookupEnv("") и так
		// вернул бы false, и половина проверки исчезла бы молча — ровно тот
		// способ отказа, от которого весь этот файл и защищает. Что пустое имя
		// стоит только там, где оно оправдано, сторожит
		// TestEveryRetiredKeyCoversBothForms.
		if key.env == "" {
			continue
		}

		if _, ok := os.LookupEnv(key.env); ok {
			errs = append(errs, key.err(key.env))
		}
	}

	return errs
}

// UnmarshalYAML перехватывает разбор Config из yaml, чтобы отставленный ключ в
// файле стал ошибкой загрузки, а не молча пропущенной строкой.
//
// Это единственная точка, мимо которой yaml пройти не может: cleanenv.ReadConfig
// разбирает файл декодером yaml, а декодер отдаёт узел этому методу, если он у
// типа есть. Проверять после разбора нечего — незнакомый ключ до Config не
// доезжает, в структуре его нет, и валидация видит поле с умолчанием, будто его
// и не задавали.
//
// Сигнатура — устаревшая форма yaml.Unmarshaler (та, что была основной в
// yaml.v2). Выбрана ради неё самой: она не упоминает типов библиотеки, поэтому
// gopkg.in/yaml.v3 не попадает в зависимости пакета и, через него, в граф
// модулей каждого потребителя. yaml.v3 эту форму поддерживает наравне с
// узловой (decode.go, obsoleteUnmarshaler), и это закреплено тестом, который
// гоняет настоящий декодер.
//
// Разбор идёт двумя проходами по одному узлу: сначала в карту — за именами
// ключей, которых в структуре нет, потом в саму структуру. Ошибка первого
// прохода игнорируется намеренно: узел, который не разобрался в карту (скаляр,
// список), не содержит и ключей, а внятную жалобу на него сформулирует второй
// проход.
//
// Поля, заданные до разбора, сохраняются: метод достраивает переданное
// значение, а не подменяет его нулевым, — иначе Logger или TLSConfig,
// выставленные до чтения файла, пропали бы.
func (c *Config) UnmarshalYAML(unmarshal func(any) error) error {
	var raw any
	if err := unmarshal(&raw); err == nil {
		if retired := newConfigError("config file", retiredYAMLErrors(raw)); retired != nil {
			return retired
		}
	}

	// Именованный тип без методов: разбор Config напрямую вызвал бы этот же
	// метод и ушёл в бесконечную рекурсию.
	type plainConfig Config

	plain := plainConfig(*c)
	if err := unmarshal(&plain); err != nil {
		return err
	}

	*c = Config(plain)

	return nil
}

// retiredYAMLErrors ищет отставленные ключи в разобранном дереве файла.
func retiredYAMLErrors(root any) []error {
	var errs []error

	for _, key := range retiredKeys {
		if yamlHasPath(root, key.yamlPath) {
			errs = append(errs, key.err(key.yamlKey()))
		}
	}

	return errs
}

// yamlHasPath отвечает, есть ли в дереве ключ по пути path. Значение не
// смотрится: наличие ключа и есть заявление о настройке, а «consumer:
// message_queue_size:» без значения — та же строка в файле, что и со значением.
func yamlHasPath(root any, path []string) bool {
	node := root

	for _, key := range path {
		child, ok := yamlChild(node, key)
		if !ok {
			return false
		}

		node = child
	}

	return true
}

// yamlChild достаёт значение по ключу из узла-отображения.
//
// Регистр не важен: декодер сопоставляет ключи файла с полями структуры
// регистронезависимо, поэтому «Message_Queue_Size» настроил бы поле наравне с
// «message_queue_size» — и отставленный ключ обязан ловиться так же.
//
// Разобраны обе формы отображения: yaml.v3 отдаёт map[string]any, yaml.v2 —
// map[any]any. Вторая ветка нужна не ради v2 как такового, а чтобы проверка не
// выродилась в тихое «ключа нет» на декодере, который отдаст другую карту.
func yamlChild(node any, key string) (any, bool) {
	switch m := node.(type) {
	case map[string]any:
		for k, v := range m {
			if strings.EqualFold(k, key) {
				return v, true
			}
		}
	case map[any]any:
		for k, v := range m {
			if s, ok := k.(string); ok && strings.EqualFold(s, key) {
				return v, true
			}
		}
	}

	return nil, false
}
