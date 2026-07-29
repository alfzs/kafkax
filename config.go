package kafkax

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Допустимые значения SASL.Mechanism. Сравнение регистронезависимое.
const (
	SASLMechanismPlain       = "PLAIN"
	SASLMechanismScramSHA256 = "SCRAM-SHA-256"
	SASLMechanismScramSHA512 = "SCRAM-SHA-512"
)

// Допустимые значения Consumer.InitialOffset.
const (
	OffsetEarliest = "earliest"
	OffsetLatest   = "latest"
)

// Допустимые значения Consumer.IsolationLevel.
const (
	IsolationReadCommitted   = "read_committed"
	IsolationReadUncommitted = "read_uncommitted"
)

// Допустимые значения Producer.CompressionType.
const (
	CompressionNone   = "none"
	CompressionGzip   = "gzip"
	CompressionSnappy = "snappy"
	CompressionLZ4    = "lz4"
	CompressionZstd   = "zstd"
)

// Config — конфигурация клиента Kafka.
//
// Отдельного поля с протоколом безопасности здесь нет: протокол не задаётся
// строкой, а выводится из самих настроек — TLS включается наличием
// TLS-настроек, SASL — наличием механизма.
type Config struct {
	// Brokers — адреса брокеров для первичного подключения; остальные клиент
	// обнаруживает через metadata-запрос.
	Brokers []string `yaml:"brokers" env:"KAFKAX_BROKERS" env-separator:"," env-required:"true"`
	// ClientID — идентификатор клиента, отображается в логах и метриках брокера.
	ClientID string `yaml:"client_id" env:"KAFKAX_CLIENT_ID" env-required:"true"`
	// GracefulTimeout — общий бюджет завершения при Stop/Close: его делят
	// ожидание воркеров, финальный коммит и flush.
	GracefulTimeout time.Duration `yaml:"graceful_timeout" env:"KAFKAX_GRACEFUL_TIMEOUT" env-default:"3m"`
	// DialTimeout — таймаут установки TCP/TLS-соединения с брокером.
	DialTimeout time.Duration `yaml:"dial_timeout" env:"KAFKAX_DIAL_TIMEOUT" env-default:"10s"`

	SASL     SASL     `yaml:"sasl"`
	TLS      TLS      `yaml:"tls"`
	Producer Producer `yaml:"producer"`
	Consumer Consumer `yaml:"consumer"`

	// Logger — логгер библиотеки. При nil используется slog.Default().
	// Логи самого franz-go пишутся в него же на уровне Debug.
	Logger *slog.Logger `yaml:"-"`

	// TLSConfig — готовый *tls.Config. Задан — имеет приоритет над секцией
	// TLS целиком: сборка конфигурации из путей к файлам покрывает типовой
	// случай, но не покрывает mTLS с ротацией, кастомный VerifyPeerCertificate
	// или сертификаты из памяти.
	TLSConfig *tls.Config `yaml:"-"`

	// ExtraOpts добавляются к опциям клиента последними и потому побеждают
	// всё, что вывела библиотека. Аварийный выход для настроек, которые
	// kafkax не покрывает, — не замена конфигурации.
	ExtraOpts []kgo.Opt `yaml:"-"`

	// OnPanic вызывается после восстановления паники в горутине библиотеки.
	// site — точка восстановления ("handler", "process_message",
	// "partition_worker", "on_message_skipped"), recovered — значение из
	// recover(), stack — стек на момент паники.
	//
	// Вызывается синхронно из той горутины, где произошла паника: не должен
	// блокироваться надолго. Собственная паника хука перехватывается.
	OnPanic func(ctx context.Context, site string, recovered any, stack []byte) `yaml:"-"`

	// OnMessageSkipped решает судьбу сообщения, которое обработчик не осилил
	// за HandlerMaxRetries попыток. Это единственный выход из «отравленного»
	// сообщения, кроме остановки партиции.
	//
	// Возврат nil означает «я забрал сообщение» (записал в DLQ, в базу, в лог)
	// — оффсет двигается дальше, сообщение считается skipped. Любая другая
	// ошибка, как и отсутствие хука, означает «не забрал»: оффсет не двигается,
	// партиция ставится на паузу, и после ребаланса или перезапуска сообщение
	// приедет снова.
	//
	// Пауза, а не молчаливый пропуск, выбрана по умолчанию намеренно: коммит
	// проваленного сообщения как skipped без хука означал бы потерю данных
	// молча. Застрявшая партиция видна по лагу и по логу уровня Error, потеря
	// — ничем.
	OnMessageSkipped func(ctx context.Context, msg IncomingMessage, cause error) error `yaml:"-"`
}

// LogValue реализует slog.LogValuer для всей конфигурации: типовой способ
// увидеть настройки — записать их в лог один раз на старте, целиком.
//
// До этого метода такая запись работала случайно и по-разному: TextHandler
// печатал Config через %+v и попадал на SASL.String, а JSONHandler спотыкался
// о поля-функции и клал в лог «!ERROR:json: unsupported type: func(...)»
// вместо конфигурации. Пароль при этом не утекал, но и пользы от записи не
// было — а исчезни поля-функции, не стало бы и защиты.
//
// Поля, которые в лог не помещаются (логгер, готовый *tls.Config, ExtraOpts,
// хуки), заменены признаком наличия: их значение всё равно нечитаемо, а вот
// факт, что хук задан, объясняет поведение, которого не видно в остальных
// полях.
func (c Config) LogValue() slog.Value {
	return slog.GroupValue(
		slog.Any("brokers", c.Brokers),
		slog.String("client_id", c.ClientID),
		slog.Duration("graceful_timeout", c.GracefulTimeout),
		slog.Duration("dial_timeout", c.DialTimeout),
		slog.Any("sasl", c.SASL),
		slog.Any("tls", c.TLS),
		slog.Any("producer", c.Producer),
		slog.Any("consumer", c.Consumer),
		slog.Bool("tls_config_set", c.TLSConfig != nil),
		slog.Int("extra_opts", len(c.ExtraOpts)),
		slog.Bool("on_panic_set", c.OnPanic != nil),
		slog.Bool("on_message_skipped_set", c.OnMessageSkipped != nil),
	)
}

// SASL содержит параметры аутентификации SASL.
// Пустой Mechanism означает, что SASL не используется.
type SASL struct {
	// Mechanism — механизм SASL: PLAIN, SCRAM-SHA-256 или SCRAM-SHA-512.
	// Пустое значение отключает SASL. Регистр не важен.
	Mechanism string `yaml:"mechanism" env:"KAFKAX_SASL_MECHANISM"`
	Username  string `yaml:"username" env:"KAFKAX_SASL_USERNAME"`
	Password  string `yaml:"password" env:"KAFKAX_SASL_PASSWORD"`
	// AllowPlaintext разрешает механизм PLAIN без TLS. По умолчанию такая пара
	// отвергается валидацией: PLAIN отправляет брокеру `zid\0user\0pass`
	// открытым текстом, и без шифрования пароль читает любой, кто видит трафик.
	//
	// Флаг существует не для удобства, а чтобы решение было заявлено: у
	// плейнтекста есть законные сценарии (локальный брокер в тестах, шифрование
	// на уровне сети или сайдкара), и отличить их от забытого
	// KAFKAX_TLS_ENABLED библиотека не может — а вот потребовать, чтобы разницу
	// назвали явно, может.
	//
	// На SCRAM не влияет: тот пароль по проводу не передаёт, и без TLS
	// библиотека ограничивается предупреждением при создании клиента.
	AllowPlaintext bool `yaml:"allow_plaintext" env:"KAFKAX_SASL_ALLOW_PLAINTEXT"`
}

// LogValue реализует slog.LogValuer, чтобы пароль не попадал в логи при
// логировании Config или SASL целиком.
func (s SASL) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("mechanism", s.Mechanism),
		slog.String("username", s.Username),
		slog.String("password", redactedOrEmpty(s.Password)),
		slog.Bool("allow_plaintext", s.AllowPlaintext),
	)
}

// String реализует fmt.Stringer по той же причине, что и LogValue: %v на
// SASL не должен печатать пароль.
func (s SASL) String() string {
	return fmt.Sprintf("SASL{Mechanism:%s Username:%s Password:%s AllowPlaintext:%t}",
		s.Mechanism, s.Username, redactedOrEmpty(s.Password), s.AllowPlaintext)
}

// GoString реализует fmt.GoStringer, потому что Stringer здесь не помогает:
// при флаге # fmt спрашивает только GoStringer и String игнорирует полностью.
// Без этого метода одного `log.Printf("%#v", cfg)` в чужом отладочном коде
// хватало, чтобы пароль уехал в лог мимо всей редакции.
//
// Вложенный случай закрывается тем же методом: %#v на Config обходит поля
// рекурсивно и для каждого спрашивает GoStringer.
func (s SASL) GoString() string {
	return fmt.Sprintf("kafkax.SASL{Mechanism:%q, Username:%q, Password:%q, AllowPlaintext:%t}",
		s.Mechanism, s.Username, redactedOrEmpty(s.Password), s.AllowPlaintext)
}

// MarshalJSON реализует json.Marshaler: encoding/json не знает ни о Stringer,
// ни о LogValuer, так что без этого метода `json.Marshal(cfg.SASL)` — обычный
// способ положить конфигурацию в ответ отладочной ручки или в дамп состояния —
// возвращал пароль как есть.
//
// Ключи совпадают с yaml-тегами, чтобы дамп читался тем же глазом, что и
// конфигурационный файл.
func (s SASL) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Mechanism      string `json:"mechanism"`
		Username       string `json:"username"`
		Password       string `json:"password"`
		AllowPlaintext bool   `json:"allow_plaintext"`
	}{
		Mechanism:      s.Mechanism,
		Username:       s.Username,
		Password:       redactedOrEmpty(s.Password),
		AllowPlaintext: s.AllowPlaintext,
	})
}

func redactedOrEmpty(secret string) string {
	if secret == "" {
		return ""
	}

	return "[REDACTED]"
}

// enabled сообщает, нужен ли SASL.
func (s SASL) enabled() bool { return s.Mechanism != "" }

// TLS содержит параметры TLS-соединения с брокером.
// При пустых путях к сертификатам используется системное хранилище CA.
type TLS struct {
	// Enabled включает TLS. Отдельный флаг, а не вывод из непустоты путей:
	// подключение к брокеру с сертификатом из системного trust store не
	// требует ни одного пути, и «пустые пути ⇒ TLS выключен» отключило бы
	// шифрование ровно в этом случае.
	Enabled bool `yaml:"enabled" env:"KAFKAX_TLS_ENABLED"`
	// CACertPath — путь к PEM-файлу CA для проверки сертификата брокера.
	// При пустом значении используется системный trust store.
	CACertPath string `yaml:"ca_cert_path" env:"KAFKAX_TLS_CA_CERT_PATH"`
	// ClientCertPath и ClientKeyPath — клиентский сертификат и ключ для mTLS.
	// Задаются только вместе.
	ClientCertPath string `yaml:"client_cert_path" env:"KAFKAX_TLS_CLIENT_CERT_PATH"`
	ClientKeyPath  string `yaml:"client_key_path" env:"KAFKAX_TLS_CLIENT_KEY_PATH"`
	// ServerName переопределяет имя, по которому проверяется сертификат
	// брокера. Нужно, когда клиент ходит через IP или через прокси.
	ServerName string `yaml:"server_name" env:"KAFKAX_TLS_SERVER_NAME"`
	// InsecureSkipVerify отключает проверку сертификата брокера целиком.
	// Только для локальной отладки: включённое значение делает соединение
	// уязвимым к MITM (CWE-295). Библиотека пишет WARN при каждом создании
	// клиента с этим флагом.
	InsecureSkipVerify bool `yaml:"insecure_skip_verify" env:"KAFKAX_TLS_INSECURE_SKIP_VERIFY"`
}

// enabled сообщает, нужен ли TLS.
func (t TLS) enabled() bool { return t.Enabled }

// transportEncrypted сообщает, будет ли соединение с брокером зашифровано.
//
// Порядок условий повторяет tlsConfig: готовый TLSConfig побеждает секцию TLS,
// поэтому «TLS.Enabled=false, но TLSConfig задан» — это TLS, а не его
// отсутствие. Расхождение этих двух мест означало бы, что валидация судит об
// одном соединении, а собирается другое.
func (c Config) transportEncrypted() bool {
	return c.TLSConfig != nil || c.TLS.enabled()
}

// Producer содержит параметры Kafka-продюсера.
type Producer struct {
	// RequiredAcks — сколько брокеров должны подтвердить запись:
	// -1 = все реплики ISR (по умолчанию и единственное безопасное значение
	// при включённой идемпотентности), 1 = только лидер, 0 = без подтверждения.
	// Значения 1 и 0 требуют EnableIdempotence=false — иначе Validate вернёт
	// ошибку, а не тихо отключит идемпотентность.
	RequiredAcks int `yaml:"required_acks" env:"KAFKAX_PRODUCER_REQUIRED_ACKS" env-default:"-1"`
	// EnableIdempotence включает идемпотентную запись: брокер дедуплицирует
	// повторные отправки и сохраняет порядок внутри партиции при нескольких
	// запросах в полёте.
	EnableIdempotence bool `yaml:"enable_idempotence" env:"KAFKAX_PRODUCER_ENABLE_IDEMPOTENCE" env-default:"true"`
	// MaxInflight — максимум неподтверждённых produce-запросов на брокера.
	//
	// Применяется только при EnableIdempotence=false. Идемпотентный продюсер
	// в franz-go держит потолок сам (пять — предел протокола), и попытка
	// задать значение вручную отвергается конструктором клиента, а не
	// подстраивается молча.
	//
	// Единица нужна при ВЫКЛЮЧЕННОЙ идемпотентности: без sequence numbers
	// повторная отправка второго запроса обгоняет первый и переставляет
	// записи в партиции. При включённой порядок держат sequence numbers,
	// и пять запросов в полёте безопасны.
	MaxInflight int `yaml:"max_inflight" env:"KAFKAX_PRODUCER_MAX_INFLIGHT" env-default:"5"`
	// MaxRetries — сколько раз повторять доставку одной записи при
	// повторяемой ошибке брокера.
	MaxRetries int `yaml:"max_retries" env:"KAFKAX_PRODUCER_MAX_RETRIES" env-default:"3"`
	// AckTimeout — таймаут ожидания подтверждения записи на стороне брокера.
	// Не путать с клиентским MessageTimeout.
	AckTimeout time.Duration `yaml:"ack_timeout" env:"KAFKAX_PRODUCER_ACK_TIMEOUT" env-default:"5s"`
	// RetryBackoff — пауза между повторами. Фиксированная: franz-go по
	// умолчанию наращивает её экспоненциально с джиттером, но тогда значение
	// этой настройки перестало бы совпадать с реальной паузой.
	RetryBackoff time.Duration `yaml:"retry_backoff" env:"KAFKAX_PRODUCER_RETRY_BACKOFF" env-default:"100ms"`
	// Linger — сколько ждать перед отправкой неполного батча. Ноль означает
	// «отправлять сразу»; SendMessage всё равно инициирует немедленную
	// отправку по затронутым партициям, так что linger не добавляет задержки
	// синхронному пути.
	Linger time.Duration `yaml:"linger" env:"KAFKAX_PRODUCER_LINGER" env-default:"0s"`
	// BatchBytes — верхняя граница размера одного батча.
	BatchBytes int32 `yaml:"batch_bytes" env:"KAFKAX_PRODUCER_BATCH_BYTES" env-default:"1048576"`
	// CompressionType — сжатие батчей: none, gzip, snappy, lz4, zstd.
	CompressionType string `yaml:"compression_type" env:"KAFKAX_PRODUCER_COMPRESSION_TYPE" env-default:"lz4"`
	// MaxBufferedRecords — сколько записей клиент готов держать в памяти до
	// подтверждения. Это и есть backpressure: при заполнении Produce ждёт
	// освобождения места.
	//
	// Лимит общий на клиента, а не на отдельного отправителя: защита от
	// переполнения памяти есть, а отправки при этом не сериализуются.
	MaxBufferedRecords int `yaml:"max_buffered_records" env:"KAFKAX_PRODUCER_MAX_BUFFERED_RECORDS" env-default:"10000"`
	// MaxBufferedBytes — тот же лимит в байтах. Ноль означает «без лимита».
	MaxBufferedBytes int `yaml:"max_buffered_bytes" env:"KAFKAX_PRODUCER_MAX_BUFFERED_BYTES" env-default:"0"`
	// MessageTimeout — полный бюджет одного SendMessage: от вызова до
	// delivery ack. Расходуется ровно один раз на весь путь отправки.
	//
	// Минимум — одна секунда: franz-go отвергает меньшее значение при создании
	// клиента, и Validate проверяет границу раньше, чем конструктор.
	MessageTimeout time.Duration `yaml:"message_timeout" env:"KAFKAX_PRODUCER_MESSAGE_TIMEOUT" env-default:"30s"`
	// FlushTimeout — верхняя граница финального Flush при Close. Реальная
	// длительность — min(FlushTimeout, остаток GracefulTimeout).
	FlushTimeout time.Duration `yaml:"flush_timeout" env:"KAFKAX_PRODUCER_FLUSH_TIMEOUT" env-default:"1m"`
}

// Consumer содержит параметры Kafka-консьюмера.
type Consumer struct {
	// Group — идентификатор consumer group. Обязателен.
	Group string `yaml:"group" env:"KAFKAX_CONSUMER_GROUP"`
	// InitialOffset — откуда читать группу без сохранённого оффсета:
	// earliest или latest.
	InitialOffset string `yaml:"initial_offset" env:"KAFKAX_CONSUMER_INITIAL_OFFSET" env-default:"earliest"`
	// MinBytes — минимальный объём данных в ответе на fetch.
	MinBytes int32 `yaml:"min_bytes" env:"KAFKAX_CONSUMER_MIN_BYTES" env-default:"1"`
	// MaxBytes — максимальный объём данных в ответе на fetch.
	MaxBytes int32 `yaml:"max_bytes" env:"KAFKAX_CONSUMER_MAX_BYTES" env-default:"52428800"`
	// MaxPartitionBytes — максимальный объём данных с одной партиции в ответе.
	MaxPartitionBytes int32 `yaml:"max_partition_bytes" env:"KAFKAX_CONSUMER_MAX_PARTITION_BYTES" env-default:"1048576"`
	// MaxWait — сколько брокер ждёт накопления MinBytes перед ответом.
	MaxWait time.Duration `yaml:"max_wait" env:"KAFKAX_CONSUMER_MAX_WAIT" env-default:"500ms"`
	// SessionTimeout — время, после которого координатор считает консьюмера
	// мёртвым при отсутствии heartbeat.
	SessionTimeout time.Duration `yaml:"session_timeout" env:"KAFKAX_CONSUMER_SESSION_TIMEOUT" env-default:"45s"`
	// HeartbeatInterval — период heartbeat. Не более SessionTimeout/3, и это
	// проверяется: запас на два промаха подряд — минимум, при котором
	// единственный потерянный heartbeat не превращается в ребаланс.
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval" env:"KAFKAX_CONSUMER_HEARTBEAT_INTERVAL" env-default:"3s"`
	// RebalanceTimeout — сколько координатор ждёт, пока консьюмер отдаст
	// партиции. Должен превышать максимальное время обработки батча: именно в
	// этот бюджет укладывается остановка партиционных воркеров при ребалансе.
	RebalanceTimeout time.Duration `yaml:"rebalance_timeout" env:"KAFKAX_CONSUMER_REBALANCE_TIMEOUT" env-default:"1m"`
	// IsolationLevel — видимость транзакционных сообщений:
	// read_committed или read_uncommitted.
	IsolationLevel string `yaml:"isolation_level" env:"KAFKAX_CONSUMER_ISOLATION_LEVEL" env-default:"read_committed"`
	// MaxPollRecords — верхняя граница числа записей за один опрос.
	MaxPollRecords int `yaml:"max_poll_records" env:"KAFKAX_CONSUMER_MAX_POLL_RECORDS" env-default:"500"`
	// MessageQueueSize — ёмкость канала партиционного воркера. Определяет,
	// насколько цикл опроса может обгонять обработку.
	MessageQueueSize int `yaml:"message_queue_size" env:"KAFKAX_CONSUMER_MESSAGE_QUEUE_SIZE" env-default:"100"`
	// CommitInterval — период фоновой отправки отмеченных оффсетов.
	// Коммитится только отмеченное (MarkCommitRecords после успешной
	// обработки), поэтому интервал влияет на окно переобработки, но не на
	// гарантию at-least-once.
	CommitInterval time.Duration `yaml:"commit_interval" env:"KAFKAX_CONSUMER_COMMIT_INTERVAL" env-default:"5s"`
	// HandlerMaxRetries — сколько раз повторять обработку сообщения при
	// ошибке обработчика: 0 — без повторов, N — N повторов сверх первого
	// вызова, -1 — бесконечно.
	//
	// Повторы блокируют партицию: пока сообщение повторяется, следующие
	// сообщения этой же партиции ждут. Это цена сохранения порядка.
	//
	// Что происходит после исчерпания повторов, определяет OnMessageSkipped, и
	// умолчание там — остановить партицию, а не пропустить сообщение. Полное
	// описание политики — в документации пакета, раздел «Политика повторов»;
	// прочитайте его перед подбором этих значений.
	HandlerMaxRetries int `yaml:"handler_max_retries" env:"KAFKAX_CONSUMER_HANDLER_MAX_RETRIES" env-default:"0"`
	// HandlerRetryDelay — пауза между повторами обработки. Обязателен при
	// HandlerMaxRetries != 0.
	HandlerRetryDelay time.Duration `yaml:"handler_retry_delay" env:"KAFKAX_CONSUMER_HANDLER_RETRY_DELAY" env-default:"1s"`
}

// DefaultConfig возвращает Config со всеми значениями по умолчанию —
// теми же, что подставит cleanenv из тегов env-default.
//
// Существует потому, что нулевой Config нерабочий: собранный литералом в Go
// (полностью поддерживаемый путь, а не обходной), он даёт около полутора
// десятков ошибок валидации, и каждый вызывающий обязан знать все умолчания
// наизусть. Правильный способ настроить пакет из кода — взять эту базу и
// переопределить то, что нужно:
//
//	cfg := kafkax.DefaultConfig()
//	cfg.Brokers = []string{"kafka:9092"}
//	cfg.ClientID = "billing"
//	cfg.Consumer.Group = "billing-workers"
//
// Обязательные поля без умолчаний — Brokers, ClientID, Consumer.Group — здесь
// остаются пустыми: подставить за пользователя идентификатор группы значило бы
// молча свести два разных сервиса в одну группу. Их отсутствие поймает
// валидация в конструкторе.
//
// Значения дублируют теги структуры, и это проверяется тестом на сверку через
// reflect: разъехаться молча они не могут.
func DefaultConfig() Config {
	return Config{
		GracefulTimeout: 3 * time.Minute,
		DialTimeout:     10 * time.Second,
		Producer: Producer{
			RequiredAcks:       -1,
			EnableIdempotence:  true,
			MaxInflight:        5,
			MaxRetries:         3,
			AckTimeout:         5 * time.Second,
			RetryBackoff:       100 * time.Millisecond,
			Linger:             0,
			BatchBytes:         1048576,
			CompressionType:    CompressionLZ4,
			MaxBufferedRecords: 10000,
			MaxBufferedBytes:   0,
			MessageTimeout:     30 * time.Second,
			FlushTimeout:       time.Minute,
		},
		Consumer: Consumer{
			InitialOffset:     OffsetEarliest,
			MinBytes:          1,
			MaxBytes:          52428800,
			MaxPartitionBytes: 1048576,
			MaxWait:           500 * time.Millisecond,
			SessionTimeout:    45 * time.Second,
			HeartbeatInterval: 3 * time.Second,
			RebalanceTimeout:  time.Minute,
			IsolationLevel:    IsolationReadCommitted,
			MaxPollRecords:    500,
			MessageQueueSize:  100,
			CommitInterval:    5 * time.Second,
			HandlerMaxRetries: 0,
			HandlerRetryDelay: time.Second,
		},
	}
}

// Validate проверяет Config целиком — и продюсерскую, и консьюмерскую секцию.
// Подходит приложению, которое создаёт из одного Config и то, и другое.
//
// Конструкторы вызывают не её, а проверку своей роли: продюсеру незачем
// требовать consumer.group, а консьюмеру — producer.flush_timeout. Config,
// прошедший NewKafkaProducer, может не пройти Validate.
//
// Ошибки собираются все разом, а не возвращаются по первой: иначе неполный
// конфиг чинится по одному полю за перезапуск. Результат отвечает
// errors.Is(err, ErrInvalidConfig), а полный список претензий разворачивается
// через errors.Unwrap() []error. Сентинел в этот список НЕ входит: код,
// печатающий список пользователю, иначе выводил бы «invalid configuration»
// первой строкой перечня полей.
//
// Конструкторы возвращают эту ошибку как есть, не оборачивая: обёртка через
// fmt.Errorf дала бы Unwrap() error вместо Unwrap() []error и сломала бы
// описанный выше разбор.
func (c Config) Validate() error {
	errs := c.commonErrors()
	errs = append(errs, c.producerErrors()...)
	errs = append(errs, c.consumerErrors()...)

	return newConfigError("config", errs)
}

// validateProducer — проверка для NewKafkaProducer: общие поля и секция Producer.
func (c Config) validateProducer() error {
	return newConfigError("producer config", append(c.commonErrors(), c.producerErrors()...))
}

// validateConsumer — проверка для NewKafkaConsumer: общие поля и секция Consumer.
func (c Config) validateConsumer() error {
	return newConfigError("consumer config", append(c.commonErrors(), c.consumerErrors()...))
}

// configError — агрегат ошибок валидации.
//
// Собственный тип, а не errors.Join с подмешанным ErrInvalidConfig: у Join'а
// разворот через Unwrap() []error отдал бы сентинел наравне с ошибками полей.
// Здесь Is отвечает за принадлежность к ErrInvalidConfig, а Unwrap отдаёт
// ровно претензии к полям — в том порядке, в каком их собрали проверки.
type configError struct {
	// subject — что именно не прошло проверку: «config», «producer config»
	// или «consumer config». Роль важна, потому что проверки разные: Config,
	// прошедший NewKafkaProducer, может не пройти Validate.
	subject string
	errs    []error
}

// newConfigError возвращает nil на пустом списке — иначе каждый вызывающий
// писал бы эту проверку сам, а забытая превратила бы валидный конфиг в ошибку.
func newConfigError(subject string, errs []error) error {
	if len(errs) == 0 {
		return nil
	}

	return &configError{subject: subject, errs: errs}
}

func (e *configError) Error() string {
	return fmt.Sprintf("kafkax: invalid %s: %s", e.subject, errors.Join(e.errs...))
}

func (e *configError) Unwrap() []error {
	return e.errs
}

// Is привязывает агрегат к ErrInvalidConfig. Обход вложенных ошибок errors.Is
// делает сам через Unwrap() []error, поэтому здесь достаточно одного сентинела.
func (e *configError) Is(target error) bool {
	return target == ErrInvalidConfig
}

func (c Config) commonErrors() []error {
	var errs []error

	if len(c.Brokers) == 0 {
		errs = append(errs, errors.New("brokers must not be empty"))
	}

	if c.ClientID == "" {
		errs = append(errs, errors.New("client_id must not be empty"))
	}

	errs = appendNonPositive(errs,
		positiveDuration{"graceful_timeout", c.GracefulTimeout},
		positiveDuration{"dial_timeout", c.DialTimeout})

	errs = append(errs, c.saslErrors()...)
	errs = append(errs, c.tlsErrors()...)

	return errs
}

func (c Config) saslErrors() []error {
	if !c.SASL.enabled() {
		return nil
	}

	var errs []error

	switch strings.ToUpper(c.SASL.Mechanism) {
	case SASLMechanismPlain:
		errs = append(errs, c.plaintextPasswordErrors()...)
	case SASLMechanismScramSHA256, SASLMechanismScramSHA512:
	default:
		errs = append(errs, fmt.Errorf(
			"sasl.mechanism must be one of %s, %s, %s; got %q",
			SASLMechanismPlain, SASLMechanismScramSHA256, SASLMechanismScramSHA512, c.SASL.Mechanism))
	}

	if c.SASL.Username == "" {
		errs = append(errs, fmt.Errorf("sasl.username required for sasl.mechanism=%q", c.SASL.Mechanism))
	}

	if c.SASL.Password == "" {
		errs = append(errs, fmt.Errorf("sasl.password required for sasl.mechanism=%q", c.SASL.Mechanism))
	}

	return errs
}

// plaintextPasswordErrors отвергает PLAIN без TLS.
//
// Почему ошибка, а не предупреждение — как у InsecureSkipVerify. Разница в
// том, что теряется. Отключённая проверка сертификата делает сессию уязвимой к
// MITM: чтобы что-то произошло, атакующий должен оказаться на пути и вклиниться
// в соединение. PLAIN без TLS не создаёт уязвимости — он выполняет раскрытие:
// пароль уходит в сеть открытым текстом при каждой аутентификации, включая
// переаутентификации по расписанию брокера. Это необратимо и правкой
// конфигурации не чинится — секрет придётся ротировать, и знать об этом надо до
// первого подключения, а не из WARN, замеченного через месяц в Kibana.
//
// Ошибка возвращается только для PLAIN. SCRAM без TLS остаётся законным без
// опт-аута: пароль по проводу не идёт, MITM-риск остаётся — на него библиотека
// отвечает предупреждением в commonOpts, симметрично InsecureSkipVerify.
//
// Опт-аут именованным полем, а не подавлением предупреждения: сценарии без
// шифрования законны (kfake в тестах, брокер в том же поде, TLS на сайдкаре), и
// требуется от них ровно одно — чтобы решение было записано в конфигурации, а
// не осталось следствием невыставленной переменной окружения.
func (c Config) plaintextPasswordErrors() []error {
	if c.transportEncrypted() || c.SASL.AllowPlaintext {
		return nil
	}

	return []error{fmt.Errorf(
		"sasl.mechanism=%s without TLS sends the password to the broker in cleartext;"+
			" enable tls.enabled (or set Config.TLSConfig), or switch to %s/%s,"+
			" or set sasl.allow_plaintext=true to state that the plaintext connection is intended",
		SASLMechanismPlain, SASLMechanismScramSHA256, SASLMechanismScramSHA512)}
}

func (c Config) tlsErrors() []error {
	var errs []error

	// Сертификат без ключа (или наоборот) — не «частично настроенный mTLS», а
	// конфигурация, при которой tls.LoadX509KeyPair не вызовется вовсе и
	// клиент молча пойдёт без клиентского сертификата.
	if (c.TLS.ClientCertPath == "") != (c.TLS.ClientKeyPath == "") {
		errs = append(errs, errors.New("tls.client_cert_path and tls.client_key_path must be set together"))
	}

	return errs
}

func (c Config) producerErrors() []error {
	errs := appendNonPositive(nil,
		positiveDuration{"producer.message_timeout", c.Producer.MessageTimeout},
		positiveDuration{"producer.flush_timeout", c.Producer.FlushTimeout},
		positiveDuration{"producer.ack_timeout", c.Producer.AckTimeout})

	errs = appendBelowMinimum(errs,
		boundedDuration{"producer.message_timeout", c.Producer.MessageTimeout, time.Second},
		boundedDuration{"producer.ack_timeout", c.Producer.AckTimeout, 100 * time.Millisecond})

	// Верхняя граница linger — тоже проверка kgo.NewClient. Ноль законен и
	// означает «отправлять батч сразу», поэтому нижней границы у поля нет.
	if c.Producer.Linger > time.Minute {
		errs = append(errs, fmt.Errorf("producer.linger must not exceed 1m, got %v", c.Producer.Linger))
	}

	if c.Producer.Linger < 0 {
		errs = append(errs, fmt.Errorf("producer.linger must not be negative, got %v", c.Producer.Linger))
	}

	if c.Producer.MaxBufferedRecords <= 0 {
		errs = append(errs, fmt.Errorf(
			"producer.max_buffered_records must be positive, got %d", c.Producer.MaxBufferedRecords))
	}

	if c.Producer.MaxInflight <= 0 {
		errs = append(errs, fmt.Errorf("producer.max_inflight must be positive, got %d", c.Producer.MaxInflight))
	}

	if c.Producer.MaxRetries < 0 {
		errs = append(errs, fmt.Errorf("producer.max_retries must not be negative, got %d", c.Producer.MaxRetries))
	}

	if c.Producer.BatchBytes <= 0 {
		errs = append(errs, fmt.Errorf("producer.batch_bytes must be positive, got %d", c.Producer.BatchBytes))
	}

	// Ноль — законное «без лимита», а вот отрицательное значение godoc не
	// обещает: opts.go ставит kgo.MaxBufferedBytes только при > 0, так что
	// минус тоже означал бы «без лимита» — молча и не тем способом, каким это
	// написано в конфигурации.
	if c.Producer.MaxBufferedBytes < 0 {
		errs = append(errs, fmt.Errorf(
			"producer.max_buffered_bytes must not be negative (0 means unlimited), got %d",
			c.Producer.MaxBufferedBytes))
	}

	errs = append(errs, c.acksErrors()...)

	if _, err := compressionCodec(c.Producer.CompressionType); err != nil {
		errs = append(errs, err)
	}

	return errs
}

// acksErrors ловит конфликтующую комбинацию: идемпотентность требует acks=all.
// При acks=1 или 0 franz-go отказывается создавать клиента, и без этой проверки
// ошибка всплывала бы из конструктора без указания на поле конфигурации.
func (c Config) acksErrors() []error {
	switch c.Producer.RequiredAcks {
	case -1:
		return nil
	case 0, 1:
		if c.Producer.EnableIdempotence {
			return []error{fmt.Errorf(
				"producer.required_acks=%d requires producer.enable_idempotence=false"+
					" (idempotent writes are only defined for acks=-1)",
				c.Producer.RequiredAcks)}
		}

		return nil
	default:
		return []error{fmt.Errorf("producer.required_acks must be -1, 0 or 1; got %d", c.Producer.RequiredAcks)}
	}
}

func (c Config) consumerErrors() []error {
	var errs []error

	if c.Consumer.Group == "" {
		errs = append(errs, errors.New("consumer.group must not be empty"))
	}

	errs = appendNonPositive(errs,
		positiveDuration{"consumer.session_timeout", c.Consumer.SessionTimeout},
		positiveDuration{"consumer.heartbeat_interval", c.Consumer.HeartbeatInterval},
		positiveDuration{"consumer.rebalance_timeout", c.Consumer.RebalanceTimeout},
		positiveDuration{"consumer.commit_interval", c.Consumer.CommitInterval},
		positiveDuration{"consumer.max_wait", c.Consumer.MaxWait})

	errs = appendBelowMinimum(errs,
		boundedDuration{"consumer.session_timeout", c.Consumer.SessionTimeout, 100 * time.Millisecond},
		boundedDuration{"consumer.rebalance_timeout", c.Consumer.RebalanceTimeout, 100 * time.Millisecond},
		boundedDuration{"consumer.commit_interval", c.Consumer.CommitInterval, 100 * time.Millisecond},
		boundedDuration{"consumer.max_wait", c.Consumer.MaxWait, 10 * time.Millisecond})

	// Не «меньше session_timeout», а «не больше его трети» — ровно то, что
	// обещает godoc HeartbeatInterval. Требование не косметическое: при
	// интервале в половину таймаута достаточно одного потерянного heartbeat,
	// чтобы координатор объявил живого консьюмера мёртвым и запустил ребаланс.
	// Треть даёт запас на два промаха подряд — общепринятый минимум для Kafka.
	maxHeartbeat := c.Consumer.SessionTimeout / 3
	if c.Consumer.SessionTimeout > 0 && c.Consumer.HeartbeatInterval > maxHeartbeat {
		errs = append(errs, fmt.Errorf(
			"consumer.heartbeat_interval (%v) must not exceed a third of consumer.session_timeout (%v), i.e. %v",
			c.Consumer.HeartbeatInterval, c.Consumer.SessionTimeout, maxHeartbeat))
	}

	// Отрицательное значение роняет makechan паникой уже после того, как
	// конструктор вернул nil-ошибку.
	if c.Consumer.MessageQueueSize <= 0 {
		errs = append(errs, fmt.Errorf(
			"consumer.message_queue_size must be positive, got %d", c.Consumer.MessageQueueSize))
	}

	if c.Consumer.MaxPollRecords <= 0 {
		errs = append(errs, fmt.Errorf(
			"consumer.max_poll_records must be positive, got %d", c.Consumer.MaxPollRecords))
	}

	switch strings.ToLower(c.Consumer.InitialOffset) {
	case OffsetEarliest, OffsetLatest:
	default:
		errs = append(errs, fmt.Errorf(
			"consumer.initial_offset must be %q or %q; got %q",
			OffsetEarliest, OffsetLatest, c.Consumer.InitialOffset))
	}

	switch strings.ToLower(c.Consumer.IsolationLevel) {
	case IsolationReadCommitted, IsolationReadUncommitted:
	default:
		errs = append(errs, fmt.Errorf(
			"consumer.isolation_level must be %q or %q; got %q",
			IsolationReadCommitted, IsolationReadUncommitted, c.Consumer.IsolationLevel))
	}

	errs = append(errs, c.fetchSizeErrors()...)

	return append(errs, c.handlerRetryErrors()...)
}

// fetchSizeErrors проверяет байтовые границы fetch-запроса.
//
// Вынесено из consumerErrors не ради красоты: четыре проверки подряд упирают
// функцию в потолок цикломатической сложности, а связаны они между собой
// теснее, чем с остальной секцией.
//
// franz-go эти поля не проверяет вовсе. Ноль проходит и Validate, и конструктор
// клиента, и отказ выглядит не как ошибка конфигурации, а как «консьюмер
// подключился и ничего не читает». Пару max_partition_bytes > max_bytes
// franz-go молча прижимает (kgo/config.go), то есть настройка перестаёт значить
// написанное — и об этом тоже узнают не из лога, а из наблюдения за трафиком.
func (c Config) fetchSizeErrors() []error {
	var errs []error

	if c.Consumer.MinBytes <= 0 {
		errs = append(errs, fmt.Errorf("consumer.min_bytes must be positive, got %d", c.Consumer.MinBytes))
	}

	if c.Consumer.MaxBytes <= 0 {
		errs = append(errs, fmt.Errorf("consumer.max_bytes must be positive, got %d", c.Consumer.MaxBytes))
	}

	if c.Consumer.MaxPartitionBytes <= 0 {
		errs = append(errs, fmt.Errorf(
			"consumer.max_partition_bytes must be positive, got %d", c.Consumer.MaxPartitionBytes))
	}

	// Сравнение имеет смысл только при положительной верхней границе: при
	// max_bytes=0 ошибка о ней уже добавлена, и вторая претензия к той же
	// опечатке только удлинила бы список.
	if c.Consumer.MaxBytes > 0 && c.Consumer.MaxPartitionBytes > c.Consumer.MaxBytes {
		errs = append(errs, fmt.Errorf(
			"consumer.max_partition_bytes (%d) must not exceed consumer.max_bytes (%d)",
			c.Consumer.MaxPartitionBytes, c.Consumer.MaxBytes))
	}

	return errs
}

// handlerRetryErrors проверяет пару полей ретраев обработчика: они осмысленны
// только вместе, и связь между ними неочевидна.
func (c Config) handlerRetryErrors() []error {
	var errs []error

	if c.Consumer.HandlerMaxRetries < -1 {
		errs = append(errs, fmt.Errorf(
			"consumer.handler_max_retries must be -1 (infinite), 0 (no retries) or positive; got %d",
			c.Consumer.HandlerMaxRetries))
	}

	if c.Consumer.HandlerMaxRetries != 0 && c.Consumer.HandlerRetryDelay <= 0 {
		errs = append(errs, errors.New("consumer.handler_retry_delay must be positive when retries are enabled"))
	}

	return errs
}

// positiveDuration — пара «имя поля в yaml, значение» для appendNonPositive.
type positiveDuration struct {
	name  string
	value time.Duration
}

// appendNonPositive добавляет по ошибке на каждую неположительную длительность.
// Имена совпадают с путями в yaml, чтобы из текста ошибки было видно, что
// править, без чтения исходников.
func appendNonPositive(errs []error, ds ...positiveDuration) []error {
	for _, d := range ds {
		if d.value <= 0 {
			errs = append(errs, fmt.Errorf("%s must be positive, got %v", d.name, d.value))
		}
	}

	return errs
}

// boundedDuration — длительность и нижняя граница, которую задаёт franz-go.
type boundedDuration struct {
	name  string
	value time.Duration
	min   time.Duration
}

// appendBelowMinimum добавляет по ошибке на каждую длительность ниже границы,
// заданной franz-go.
//
// Эти границы — не вкус библиотеки, а жёсткие проверки kgo.NewClient
// (kgo/config.go, функция validate). Без них значение вроде
// producer.message_timeout=300ms проходит Validate и падает в конструкторе
// клиента текстом «record timeout 300ms is less than allowed 1s», где нет ни
// имени поля конфигурации, ни файла, из которого оно приехало.
//
// Нулевое значение здесь не проверяется: его ловит appendNonPositive, и две
// ошибки на одно поле только мешают.
func appendBelowMinimum(errs []error, ds ...boundedDuration) []error {
	for _, d := range ds {
		if d.value > 0 && d.value < d.min {
			errs = append(errs, fmt.Errorf("%s must be at least %v, got %v", d.name, d.min, d.value))
		}
	}

	return errs
}

// logger возвращает логгер библиотеки с проставленным component.
func (c Config) logger(component string) *slog.Logger {
	base := c.Logger
	if base == nil {
		base = slog.Default()
	}

	return base.With(slog.String("component", component))
}
