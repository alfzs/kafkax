package kafkax

import "time"

// brokers configuration
type Config struct {
	Brokers          []string      `env:"KAFKAX_BROKERS" env-separator:"," env-required:"true"`
	ClientID         string        `env:"KAFKAX_CLIENT_ID" env-required:"true"`
	SecurityProtocol string        `yaml:"security_protocol"`
	GracefulTimeout  time.Duration `yaml:"graceful_timeout" env-default:"3m"`
	SASL             SASL          `yaml:"sasl"`
	TLS              TLS           `yaml:"tls"`
	Producer         Producer      `yaml:"producer"`
	Consumer         Consumer      `yaml:"consumer"`
}

type SASL struct {
	Username  string `env:"KAFKAX_SASL_USERNAME" env-required:"true"`
	Password  string `env:"KAFKAX_SASL_PASSWORD" env-required:"true"`
	Mechanism string `yaml:"mechanism"`
}

type TLS struct {
	CaCertPath              string `yaml:"ca_cert_path"`
	ClientCertPath          string `yaml:"client_cert_path"`
	ClientKeyPath           string `yaml:"client_key_path"`
	IdentificationAlgorithm string `yaml:"identification_algorithm"`
	InsecureSkipVerify      bool   `yaml:"insecure_skip_verify" env-default:"false"`
}

type Producer struct {
	RequiredAcks          int           `yaml:"required_acks" env-default:"1"` // 0: NoACK, 1: LeaderACK, -1: AllACK
	AckTimeout            time.Duration `yaml:"ack_timeout" env-default:"5s"`
	FlushTimeout          time.Duration `yaml:"flush_timeout" env-default:"1m"`
	MaxRetries            int           `yaml:"max_retries" env-default:"3"`
	RetryBackoff          time.Duration `yaml:"retry_backoff" env-default:"100ms"`
	BatchSize             int           `yaml:"batch_size" env-default:"1000"`
	BatchBytes            int           `yaml:"batch_bytes" env-default:"1048576"` // 1MB
	BatchTimeout          time.Duration `yaml:"batch_timeout" env-default:"1s"`
	Linger                time.Duration `yaml:"linger" env-default:"0ms"`
	CompressionType       string        `yaml:"compression_type" env-default:"lz4"` // none, gzip, snappy, lz4, zstd
	MaxInflight           int           `yaml:"max_inflight" env-default:"1"`
	EnableIdempotence     bool          `yaml:"enable_idempotence" env-default:"true"`
	MessageQueueSize      int           `yaml:"message_queue_size" env-default:"1000"`
	InactiveWorkerTTL     time.Duration `yaml:"inactive_worker_ttl" env-default:"1h"`
	CleanupWorkerInterval time.Duration `yaml:"cleanup_worker_interval" env-default:"10m"`
}

type Consumer struct {
	Group                 string        `env:"KAFKAX_CONSUMER_GROUP" env-required:"true"`
	EnableAutoCommit      bool          `yaml:"enable_auto_commit" env-default:"false"`
	InitialOffset         string        `yaml:"initial_offset" env-default:"earliest"` // earliest, latest
	MinBytes              int           `yaml:"min_bytes" env-default:"1"`
	MaxBytes              int           `yaml:"max_bytes" env-default:"10485760"` // 10MB
	MaxWait               time.Duration `yaml:"max_wait" env-default:"250ms"`
	SocketTimeout         time.Duration `yaml:"socket_timeout" env-default:"30s"`
	SessionTimeout        time.Duration `yaml:"session_timeout" env-default:"45s"`
	HeartbeatInterval     time.Duration `yaml:"heartbeat_interval" env-default:"3s"`
	IsolationLevel        string        `yaml:"isolation_level" env-default:"read_committed"` // read_uncommitted, read_committed
	MaxPollInterval       time.Duration `yaml:"max_poll_interval" env-default:"1m"`
	ReadTimeout           time.Duration `yaml:"read_timeout" env-default:"2s"`
	MaxEnqueueTimeout     time.Duration `yaml:"max_enqueue_timeout" env-default:"1s"`
	MessageQueueSize      int           `yaml:"message_queue_size" env-default:"1000"`
	InactiveWorkerTTL     time.Duration `yaml:"inactive_worker_ttl" env-default:"1h"`
	CleanupWorkerInterval time.Duration `yaml:"cleanup_worker_interval" env-default:"10m"`
}
