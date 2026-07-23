// Package kafkax предоставляет продюсер и консьюмер Kafka с изоляцией по
// тенантам/партициям, OpenTelemetry-трейсингом и метриками "из коробки".
package kafkax

import (
	"cmp"
	"fmt"
	"strings"
	"time"
)

// Допустимые значения Config.SecurityProtocol.
const (
	SecurityProtocolPlaintext     = "PLAINTEXT"
	SecurityProtocolSSL           = "SSL"
	SecurityProtocolSASLPlaintext = "SASL_PLAINTEXT"
	SecurityProtocolSASLSSL       = "SASL_SSL"
)

// Ключи env-переменных SASL, обязательных при SecurityProtocolSASLPlaintext/
// SecurityProtocolSASLSSL — должны совпадать с тегами env на полях
// SASL.Username/SASL.Password ниже.
const (
	envKeySASLUsername = "KAFKAX_SASL_USERNAME"
	//nolint:gosec // это имя env-переменной (env-тег ниже), а не сам секрет
	envKeySASLPassword = "KAFKAX_SASL_PASSWORD"
)

// Значения ssl.endpoint.identification.algorithm.
const (
	// tlsIdentAlgorithmHTTPS включает проверку hostname брокера по RFC 2818 —
	// это же значение librdkafka использует как собственное умолчание.
	tlsIdentAlgorithmHTTPS = "https"
	// tlsIdentAlgorithmNone отключает проверку hostname. Должно возвращаться
	// только при явном TLS.InsecureSkipVerify=true — никогда как следствие
	// того, что TLS.IdentificationAlgorithm просто не задан.
	tlsIdentAlgorithmNone = "none"
)

// Config — корневая конфигурация клиента Kafka.
// Используется как продюсером, так и консьюмером; секции Producer и Consumer
// применяются только к соответствующему типу клиента.
type Config struct {
	// Brokers — адреса брокеров в формате "host:port". Достаточно указать один;
	// librdkafka автоматически обнаруживает остальные через metadata-запрос.
	Brokers []string `env:"KAFKAX_BROKERS" env-separator:"," env-required:"true"`
	// ClientID — идентификатор клиента, отображается в логах и метриках брокера.
	ClientID string `env:"KAFKAX_CLIENT_ID" env-required:"true"`
	// SecurityProtocol — протокол связи с брокером.
	// Допустимые значения: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL.
	// При SASL_PLAINTEXT и SASL_SSL поля SASL.Username и SASL.Password обязательны.
	SecurityProtocol string `yaml:"security_protocol"`
	// GracefulTimeout — максимальное время ожидания завершения воркеров при Stop/Close.
	// По истечении таймаута оставшиеся горутины прерываются принудительно.
	GracefulTimeout time.Duration `yaml:"graceful_timeout" env-default:"3m"`
	SASL            SASL          `yaml:"sasl"`
	TLS             TLS           `yaml:"tls"`
	Producer        Producer      `yaml:"producer"`
	Consumer        Consumer      `yaml:"consumer"`
}

// Validate проверяет конфигурацию. SASL-поля обязательны только при
// SASL_PLAINTEXT / SASL_SSL, поэтому env-required убран с полей SASL,
// а проверка перенесена сюда.
func (c Config) Validate() error {
	proto := strings.ToUpper(c.SecurityProtocol)
	if proto == SecurityProtocolSASLPlaintext || proto == SecurityProtocolSASLSSL {
		if c.SASL.Username == "" {
			return fmt.Errorf("%s required for security.protocol=%q", envKeySASLUsername, c.SecurityProtocol)
		}

		if c.SASL.Password == "" {
			return fmt.Errorf("%s required for security.protocol=%q", envKeySASLPassword, c.SecurityProtocol)
		}
	}

	return nil
}

// SASL содержит параметры аутентификации SASL.
// Username и Password обязательны только при SecurityProtocol = SASL_PLAINTEXT или SASL_SSL;
// для остальных протоколов игнорируются.
type SASL struct {
	Username string `env:"KAFKAX_SASL_USERNAME"`
	Password string `env:"KAFKAX_SASL_PASSWORD"`
	// Mechanism — механизм SASL. Допустимые значения: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512.
	Mechanism string `yaml:"mechanism"`
}

// TLS содержит параметры TLS-соединения с брокером.
// При пустых путях к сертификатам используется системное хранилище CA.
type TLS struct {
	// CaCertPath — путь к PEM-файлу CA-сертификата для проверки сертификата брокера.
	// При пустом значении используется системный trust store.
	CaCertPath string `yaml:"ca_cert_path"`
	// ClientCertPath и ClientKeyPath — пути к клиентскому сертификату и ключу для mTLS.
	// Оба поля должны быть указаны одновременно или оба оставлены пустыми.
	ClientCertPath string `yaml:"client_cert_path"`
	ClientKeyPath  string `yaml:"client_key_path"`
	// IdentificationAlgorithm — алгоритм проверки hostname в сертификате брокера
	// (ssl.endpoint.identification.algorithm). Пустое значение равносильно "https"
	// (secure by default — совпадает с собственным умолчанием librdkafka).
	// При InsecureSkipVerify = true всегда используется "none", независимо от
	// этого поля.
	IdentificationAlgorithm string `yaml:"identification_algorithm"`
	// InsecureSkipVerify отключает проверку TLS-сертификата сервера.
	// Допустимо только в среде разработки; в продакшене недопустимо.
	InsecureSkipVerify bool `yaml:"insecure_skip_verify" env-default:"false"`
}

// endpointIdentAlgorithm возвращает значение ssl.endpoint.identification.algorithm.
//
// security: librdkafka отклоняет пустую строку для этого enum-параметра, поэтому
// при незаданном IdentificationAlgorithm сюда нужно подставлять какое-то
// конкретное значение — но отсутствие явной настройки НЕ должно молча отключать
// проверку hostname (CWE-295). Раньше пустое значение резолвилось в "none" ровно
// так же, как и явный InsecureSkipVerify=true, из-за чего TLS без явно
// прописанного IdentificationAlgorithm="https" был уязвим к MITM (см.
// sprints/security-audit.md). Теперь по умолчанию используется "https" —
// собственное умолчание librdkafka — и "none" возвращается только при явном
// InsecureSkipVerify=true.
func (t TLS) endpointIdentAlgorithm() string {
	if t.InsecureSkipVerify {
		return tlsIdentAlgorithmNone
	}

	return cmp.Or(t.IdentificationAlgorithm, tlsIdentAlgorithmHTTPS)
}

// Producer содержит параметры Kafka-продюсера.
type Producer struct {
	// RequiredAcks определяет, сколько брокеров должны подтвердить запись перед ответом.
	// 0 = без подтверждения (fire-and-forget), 1 = только лидер партиции, -1 = все реплики (ISR).
	RequiredAcks int `yaml:"required_acks" env-default:"1"`
	// AckTimeout — таймаут ожидания подтверждения записи от брокера (request.timeout.ms).
	// Применяется на стороне брокера, не путать с клиентским MessageTimeout.
	AckTimeout time.Duration `yaml:"ack_timeout" env-default:"5s"`
	// FlushTimeout — максимальное время финального Flush при вызове Close.
	// Сообщения, не доставленные за этот период, считаются потерянными.
	FlushTimeout time.Duration `yaml:"flush_timeout" env-default:"1m"`
	// MaxRetries — количество повторных попыток отправки при временных ошибках брокера.
	MaxRetries int `yaml:"max_retries" env-default:"3"`
	// RetryBackoff — пауза между повторными попытками отправки.
	RetryBackoff time.Duration `yaml:"retry_backoff" env-default:"100ms"`
	// BatchSize — максимальное количество сообщений в одном батче (batch.num.messages).
	BatchSize int `yaml:"batch_size" env-default:"1000"`
	// BatchBytes — максимальный размер батча в байтах (batch.size). По умолчанию 1 МБ.
	BatchBytes int `yaml:"batch_bytes" env-default:"1048576"`
	// BatchTimeout — максимальное время накопления батча перед отправкой (queue.buffering.max.ms).
	BatchTimeout time.Duration `yaml:"batch_timeout" env-default:"1s"`
	// Linger — дополнительная задержка перед отправкой для накопления батча (linger.ms).
	// Увеличивает пропускную способность за счёт латентности.
	Linger time.Duration `yaml:"linger" env-default:"0ms"`
	// CompressionType — алгоритм сжатия сообщений.
	// Допустимые значения: none, gzip, snappy, lz4, zstd.
	CompressionType string `yaml:"compression_type" env-default:"lz4"`
	// MaxInflight — максимальное количество неподтверждённых запросов на одно соединение.
	// При EnableIdempotence = true должен быть равен 1 для гарантии порядка.
	MaxInflight int `yaml:"max_inflight" env-default:"1"`
	// EnableIdempotence обеспечивает exactly-once семантику на уровне продюсера:
	// брокер дедуплицирует повторные отправки одного и того же сообщения.
	EnableIdempotence bool `yaml:"enable_idempotence" env-default:"true"`
	// MessageQueueSize — ёмкость буферного канала воркера тенанта.
	// При заполнении SendMessage блокируется до освобождения места или истечения MessageTimeout.
	MessageQueueSize int `yaml:"message_queue_size" env-default:"1000"`
	// MessageTimeout — суммарный таймаут SendMessage: включает ожидание постановки сообщения
	// в очередь воркера и получение delivery ack от Kafka.
	// Не путать с AckTimeout (request.timeout.ms) — это клиентский таймаут, независимый от брокерского.
	MessageTimeout time.Duration `yaml:"message_timeout" env-default:"30s"`
	// InactiveWorkerTTL — время жизни воркера тенанта без активности до его завершения.
	// Освобождает ресурсы для тенантов, которые давно не отправляли сообщения.
	InactiveWorkerTTL time.Duration `yaml:"inactive_worker_ttl" env-default:"1h"`
	// CleanupWorkerInterval — период запуска фонового сборщика неактивных воркеров.
	CleanupWorkerInterval time.Duration `yaml:"cleanup_worker_interval" env-default:"10m"`
}

// Consumer содержит параметры Kafka-консьюмера.
type Consumer struct {
	// Group — идентификатор consumer group. Kafka балансирует партиции между
	// всеми активными членами группы с одинаковым Group ID.
	Group string `env:"KAFKAX_CONSUMER_GROUP" env-required:"true"`
	// EnableAutoCommit включает автоматический коммит offset брокером.
	// Должен оставаться false: консьюмер выполняет ручной коммит после обработки сообщения.
	EnableAutoCommit bool `yaml:"enable_auto_commit" env-default:"false"`
	// InitialOffset определяет, с какого offset начинать чтение при первом запуске группы.
	// Допустимые значения: earliest (с начала лога), latest (только новые сообщения).
	InitialOffset string `yaml:"initial_offset" env-default:"earliest"`
	// MinBytes — минимальный объём данных, который брокер накапливает перед ответом на fetch.
	// Значение 1 означает отвечать сразу при наличии хотя бы одного байта.
	MinBytes int `yaml:"min_bytes" env-default:"1"`
	// MaxBytes — максимальный объём данных в одном fetch-ответе. По умолчанию 10 МБ.
	MaxBytes int `yaml:"max_bytes" env-default:"10485760"`
	// MaxWait — максимальное время ожидания данных брокером при fetch (fetch.wait.max.ms).
	// Работает совместно с MinBytes: ответ отправляется при выполнении любого из условий.
	MaxWait time.Duration `yaml:"max_wait" env-default:"250ms"`
	// SocketTimeout — таймаут TCP-соединения с брокером.
	SocketTimeout time.Duration `yaml:"socket_timeout" env-default:"30s"`
	// SessionTimeout — таймаут сессии в consumer group. При превышении координатор
	// считает члена недоступным и инициирует ребалансировку.
	SessionTimeout time.Duration `yaml:"session_timeout" env-default:"45s"`
	// HeartbeatInterval — период отправки heartbeat-сообщений координатору группы.
	// Рекомендуется устанавливать не более SessionTimeout / 3.
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval" env-default:"3s"`
	// IsolationLevel определяет видимость транзакционных сообщений.
	// read_committed — видны только завершённые транзакции (рекомендуется).
	// read_uncommitted — видны все сообщения, включая незавершённые транзакции.
	IsolationLevel string `yaml:"isolation_level" env-default:"read_committed"`
	// MaxPollInterval — максимальный интервал между вызовами ReadMessage.
	// При превышении координатор инициирует ребалансировку. Должен быть больше
	// максимального времени обработки одного батча сообщений.
	MaxPollInterval time.Duration `yaml:"max_poll_interval" env-default:"1m"`
	// ReadTimeout — таймаут одного вызова ReadMessage. При ErrTimedOut цикл продолжается;
	// ненулевое значение необходимо для отзывчивости на сигнал остановки.
	ReadTimeout time.Duration `yaml:"read_timeout" env-default:"2s"`
	// ReadErrorBackoff — пауза перед повторным ReadMessage после нетаймаутной ошибки.
	// Предотвращает busy-loop при системных или сетевых ошибках брокера.
	ReadErrorBackoff time.Duration `yaml:"read_error_backoff" env-default:"1s"`
	// MessageQueueSize — ёмкость буферного канала воркера партиции.
	// При заполнении consumer loop блокируется, создавая обратное давление.
	MessageQueueSize int `yaml:"message_queue_size" env-default:"1000"`
	// HandlerMaxRetries — максимальное число вызовов ProcessMessage при ошибке обработчика.
	// После исчерпания попыток offset коммитится и сообщение пропускается (poison pill защита).
	// 0 означает повторять бесконечно — не рекомендуется в продакшене.
	HandlerMaxRetries int `yaml:"handler_max_retries" env-default:"3"`
	// HandlerRetryDelay — пауза между повторными вызовами обработчика.
	HandlerRetryDelay time.Duration `yaml:"handler_retry_delay" env-default:"1s"`
	// InactiveWorkerTTL — время жизни воркера партиции без активности до его завершения.
	// Освобождает ресурсы после ребалансировки, когда партиция переходит к другому члену группы.
	InactiveWorkerTTL time.Duration `yaml:"inactive_worker_ttl" env-default:"1h"`
	// CleanupWorkerInterval — период запуска фонового сборщика неактивных воркеров.
	CleanupWorkerInterval time.Duration `yaml:"cleanup_worker_interval" env-default:"10m"`
}
