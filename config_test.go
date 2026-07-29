package kafkax

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Тесты валидации конфигурации. Брокер не нужен: Validate — чистая функция от
// значения Config, и вся её ценность в том, что дефект конфигурации виден до
// первого сетевого вызова, а не всплывает из конструктора клиента franz-go
// текстом вроде «idempotency requires acks=all» без указания на поле yaml.

// cfgUnwrapJoined разворачивает результат errors.Join в плоский список.
//
// Разворачивание — часть контракта Validate: ошибки собираются все разом,
// иначе неполный конфиг чинится по одному полю за перезапуск. Если реализация
// начнёт возвращать первую ошибку, длина списка станет единицей и тесты
// «сколько дефектов — столько ошибок» упадут.
func cfgUnwrapJoined(t *testing.T, err error) []error {
	t.Helper()

	if err == nil {
		return nil
	}

	joined, ok := err.(interface{ Unwrap() []error })
	if !ok {
		t.Fatalf("ошибка %T не разворачивается через Unwrap() []error: %v", err, err)
	}

	return joined.Unwrap()
}

// cfgWantErr требует ненулевую ошибку, текст которой содержит все want.
func cfgWantErr(t *testing.T, err error, want ...string) {
	t.Helper()

	if err == nil {
		t.Fatalf("ожидалась ошибка, содержащая %q, получен nil", want)
	}

	for _, w := range want {
		if !strings.Contains(err.Error(), w) {
			t.Errorf("текст ошибки не содержит %q:\n%v", w, err)
		}
	}
}

// cfgWantNoErr валит тест на любой ошибке.
func cfgWantNoErr(t *testing.T, err error) {
	t.Helper()

	if err != nil {
		t.Fatalf("ожидался nil, получено: %v", err)
	}
}

func TestConfigValidateAcceptsValidConfig(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t)

	// Все три входа должны принимать одну и ту же валидную конфигурацию:
	// testConfig — база для остальных тестов пакета, и её протухание
	// проявилось бы падениями в чужих файлах без внятной причины.
	cfgWantNoErr(t, cfg.Validate())
	cfgWantNoErr(t, cfg.validateProducer())
	cfgWantNoErr(t, cfg.validateConsumer())
}

func TestConfigValidateCommonFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{
			name:   "nil brokers",
			mutate: func(c *Config) { c.Brokers = nil },
			want:   "brokers must not be empty",
		},
		{
			name:   "пустой список brokers",
			mutate: func(c *Config) { c.Brokers = []string{} },
			want:   "brokers must not be empty",
		},
		{
			name:   "пустой client_id",
			mutate: func(c *Config) { c.ClientID = "" },
			want:   "client_id must not be empty",
		},
		{
			name:   "нулевой graceful_timeout",
			mutate: func(c *Config) { c.GracefulTimeout = 0 },
			want:   "graceful_timeout must be positive",
		},
		{
			name:   "отрицательный graceful_timeout",
			mutate: func(c *Config) { c.GracefulTimeout = -time.Second },
			want:   "graceful_timeout must be positive",
		},
		{
			name:   "нулевой dial_timeout",
			mutate: func(c *Config) { c.DialTimeout = 0 },
			want:   "dial_timeout must be positive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := testConfig(t)
			tt.mutate(&cfg)

			// Общие поля обязаны проверяться обеими ролевыми проверками:
			// продюсер без brokers так же нежизнеспособен, как консьюмер.
			cfgWantErr(t, cfg.Validate(), tt.want)
			cfgWantErr(t, cfg.validateProducer(), tt.want)
			cfgWantErr(t, cfg.validateConsumer(), tt.want)
		})
	}
}

func TestConfigValidateCollectsAllErrors(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t)
	cfg.Brokers = nil
	cfg.ClientID = ""
	cfg.GracefulTimeout = 0
	cfg.DialTimeout = -time.Second

	errs := cfgUnwrapJoined(t, cfg.validateProducer())

	// Ровно четыре дефекта — ровно четыре ошибки. Проверяется не текст, а
	// количество: возврат по первой ошибке дал бы единицу.
	if len(errs) != 4 {
		t.Fatalf("получено %d ошибок, ожидалось 4: %v", len(errs), errs)
	}

	cfgWantErr(t, cfg.validateProducer(),
		"brokers must not be empty",
		"client_id must not be empty",
		"graceful_timeout must be positive",
		"dial_timeout must be positive")
}

func TestConfigValidateZeroConfigReportsBothSections(t *testing.T) {
	t.Parallel()

	var cfg Config

	// Пустая структура — это приложение, забывшее заполнить конфиг вообще.
	// Оно должно получить полный список претензий сразу, включая обе секции.
	cfgWantErr(t, cfg.Validate(),
		"brokers must not be empty",
		"producer.message_timeout must be positive",
		"consumer.group must not be empty")

	if got := len(cfgUnwrapJoined(t, cfg.Validate())); got < 10 {
		t.Errorf("пустой Config дал всего %d ошибок — валидация явно поредела", got)
	}
}

func TestConfigValidateSectionsAreIndependent(t *testing.T) {
	t.Parallel()

	// Гарантия, ради которой конструкторы вызывают не Validate, а свою
	// ролевую проверку: продюсеру незачем требовать consumer.group, а
	// консьюмеру — producer.compression_type. Если проверки склеятся,
	// NewKafkaProducer начнёт отказывать на конфиге без секции Consumer.
	t.Run("дефект consumer не мешает продюсеру", func(t *testing.T) {
		t.Parallel()

		cfg := testConfig(t)
		cfg.Consumer.Group = ""
		cfg.Consumer.MaxPollRecords = 0
		cfg.Consumer.InitialOffset = "sideways"

		cfgWantNoErr(t, cfg.validateProducer())
		cfgWantErr(t, cfg.validateConsumer(), "consumer.group must not be empty")
		cfgWantErr(t, cfg.Validate(), "consumer.group must not be empty")
	})

	t.Run("дефект producer не мешает консьюмеру", func(t *testing.T) {
		t.Parallel()

		cfg := testConfig(t)
		cfg.Producer.MessageTimeout = 0
		cfg.Producer.CompressionType = "brotli"
		cfg.Producer.MaxInflight = 0

		cfgWantNoErr(t, cfg.validateConsumer())
		cfgWantErr(t, cfg.validateProducer(), "producer.message_timeout must be positive")
		cfgWantErr(t, cfg.Validate(), "producer.message_timeout must be positive")
	})
}

func TestConfigValidateSASL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		sasl     SASL
		wantErrs int
		want     []string
	}{
		{
			name: "выключен — учётные данные не требуются",
			sasl: SASL{},
		},
		{
			name: "PLAIN с учётными данными",
			sasl: SASL{Mechanism: SASLMechanismPlain, Username: "u", Password: "p"},
		},
		{
			// Механизм из yaml приходит в произвольном регистре, а сравнение
			// идёт через ToUpper: "plain" обязан приниматься наравне с "PLAIN".
			name: "нижний регистр механизма",
			sasl: SASL{Mechanism: "plain", Username: "u", Password: "p"},
		},
		{
			name: "SCRAM-SHA-256 в смешанном регистре",
			sasl: SASL{Mechanism: "Scram-Sha-256", Username: "u", Password: "p"},
		},
		{
			name: "SCRAM-SHA-512",
			sasl: SASL{Mechanism: SASLMechanismScramSHA512, Username: "u", Password: "p"},
		},
		{
			name:     "неизвестный механизм",
			sasl:     SASL{Mechanism: "GSSAPI", Username: "u", Password: "p"},
			wantErrs: 1,
			want:     []string{"sasl.mechanism must be one of", `got "GSSAPI"`},
		},
		{
			name:     "пустой username",
			sasl:     SASL{Mechanism: SASLMechanismPlain, Password: "p"},
			wantErrs: 1,
			want:     []string{"sasl.username required for"},
		},
		{
			name:     "пустой password",
			sasl:     SASL{Mechanism: SASLMechanismPlain, Username: "u"},
			wantErrs: 1,
			want:     []string{"sasl.password required for"},
		},
		{
			// Три независимых дефекта в одной секции — три ошибки: сообщение
			// «механизм неизвестен» не должно прятать отсутствие пароля.
			name:     "неизвестный механизм без учётных данных",
			sasl:     SASL{Mechanism: "kerberos"},
			wantErrs: 3,
			want:     []string{"sasl.mechanism", "sasl.username", "sasl.password"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := testConfig(t)
			cfg.SASL = tt.sasl

			err := cfg.validateProducer()
			if tt.wantErrs == 0 {
				cfgWantNoErr(t, err)

				return
			}

			cfgWantErr(t, err, tt.want...)

			if got := len(cfgUnwrapJoined(t, err)); got != tt.wantErrs {
				t.Errorf("получено %d ошибок, ожидалось %d: %v", got, tt.wantErrs, err)
			}
		})
	}
}

func TestConfigValidateTLS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		tls  TLS
		want string
	}{
		{
			name: "TLS выключен",
			tls:  TLS{},
		},
		{
			name: "включён без сертификатов — системный trust store",
			tls:  TLS{Enabled: true},
		},
		{
			name: "включён только с CA",
			tls:  TLS{Enabled: true, CACertPath: "/dev/null"},
		},
		{
			name: "полный mTLS",
			tls:  TLS{Enabled: true, ClientCertPath: "/c.pem", ClientKeyPath: "/k.pem"},
		},
		{
			// Сертификат без ключа не «частично настроенный mTLS»:
			// tls.LoadX509KeyPair не вызовется вовсе, и клиент молча пойдёт
			// к брокеру без клиентского сертификата.
			name: "сертификат без ключа",
			tls:  TLS{Enabled: true, ClientCertPath: "/c.pem"},
			want: "tls.client_cert_path and tls.client_key_path must be set together",
		},
		{
			name: "ключ без сертификата",
			tls:  TLS{Enabled: true, ClientKeyPath: "/k.pem"},
			want: "tls.client_cert_path and tls.client_key_path must be set together",
		},
		{
			// Пара проверяется независимо от флага Enabled: полузаполненная
			// секция — почти наверняка забытый Enabled, и промолчать здесь
			// значит увести пользователя к незашифрованному соединению.
			name: "полупара при выключенном TLS",
			tls:  TLS{ClientCertPath: "/c.pem"},
			want: "tls.client_cert_path and tls.client_key_path must be set together",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := testConfig(t)
			cfg.TLS = tt.tls

			err := cfg.validateProducer()
			if tt.want == "" {
				cfgWantNoErr(t, err)

				return
			}

			cfgWantErr(t, err, tt.want)
		})
	}
}

func TestConfigValidateProducerFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*Producer)
		want   string
	}{
		{
			name:   "нулевой message_timeout",
			mutate: func(p *Producer) { p.MessageTimeout = 0 },
			want:   "producer.message_timeout must be positive",
		},
		{
			name:   "отрицательный flush_timeout",
			mutate: func(p *Producer) { p.FlushTimeout = -time.Second },
			want:   "producer.flush_timeout must be positive",
		},
		{
			name:   "нулевой ack_timeout",
			mutate: func(p *Producer) { p.AckTimeout = 0 },
			want:   "producer.ack_timeout must be positive",
		},
		{
			name:   "нулевой max_buffered_records",
			mutate: func(p *Producer) { p.MaxBufferedRecords = 0 },
			want:   "producer.max_buffered_records must be positive",
		},
		{
			// Ноль здесь отвергается конструктором клиента franz-go при
			// выключенной идемпотентности, и ошибка всплыла бы без указания
			// на поле конфигурации.
			name:   "нулевой max_inflight",
			mutate: func(p *Producer) { p.MaxInflight = 0 },
			want:   "producer.max_inflight must be positive",
		},
		{
			name:   "отрицательный max_retries",
			mutate: func(p *Producer) { p.MaxRetries = -1 },
			want:   "producer.max_retries must not be negative",
		},
		{
			name:   "нулевой batch_bytes",
			mutate: func(p *Producer) { p.BatchBytes = 0 },
			want:   "producer.batch_bytes must be positive",
		},
		{
			name:   "неизвестный compression_type",
			mutate: func(p *Producer) { p.CompressionType = "brotli" },
			want:   "producer.compression_type must be one of",
		},
		{
			// Пустая строка — не «сжатие по умолчанию»: значение приходит из
			// yaml, и незаполненное поле должно быть видно, а не молча
			// превращаться в none.
			name:   "пустой compression_type",
			mutate: func(p *Producer) { p.CompressionType = "" },
			want:   "producer.compression_type must be one of",
		},
		{
			name:   "нулевой max_retries допустим",
			mutate: func(p *Producer) { p.MaxRetries = 0 },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := testConfig(t)
			tt.mutate(&cfg.Producer)

			err := cfg.validateProducer()
			if tt.want == "" {
				cfgWantNoErr(t, err)

				return
			}

			cfgWantErr(t, err, tt.want)
			// Та же претензия не должна возникать у консьюмера.
			cfgWantNoErr(t, cfg.validateConsumer())
		})
	}
}

func TestConfigValidateAcksAndIdempotence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		acks        int
		idempotence bool
		want        string
	}{
		{name: "acks=-1 с идемпотентностью", acks: -1, idempotence: true},
		{name: "acks=-1 без идемпотентности", acks: -1, idempotence: false},
		{name: "acks=1 без идемпотентности", acks: 1, idempotence: false},
		{name: "acks=0 без идемпотентности", acks: 0, idempotence: false},
		{
			// Конфликтующая комбинация: franz-go откажется создавать клиента
			// с текстом «idempotency requires acks=all», и без этой проверки
			// пользователь узнал бы о конфликте из конструктора, а не из
			// имени поля.
			name:        "acks=1 с идемпотентностью",
			acks:        1,
			idempotence: true,
			want:        "producer.required_acks=1 requires producer.enable_idempotence=false",
		},
		{
			name:        "acks=0 с идемпотентностью",
			acks:        0,
			idempotence: true,
			want:        "producer.required_acks=0 requires producer.enable_idempotence=false",
		},
		{
			name:        "acks=2 вне диапазона",
			acks:        2,
			idempotence: false,
			want:        "producer.required_acks must be -1, 0 or 1",
		},
		{
			name:        "acks=-2 вне диапазона",
			acks:        -2,
			idempotence: true,
			want:        "producer.required_acks must be -1, 0 or 1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := testConfig(t)
			cfg.Producer.RequiredAcks = tt.acks
			cfg.Producer.EnableIdempotence = tt.idempotence

			err := cfg.validateProducer()
			if tt.want == "" {
				cfgWantNoErr(t, err)

				return
			}

			cfgWantErr(t, err, tt.want)
		})
	}
}

// consumerFieldCase — сценарий валидации секции Consumer: mutate портит (или
// намеренно оставляет корректным) одно поле, want — подстрока ожидаемой ошибки,
// пустая строка означает «конфиг обязан пройти».
type consumerFieldCase struct {
	name   string
	mutate func(*Consumer)
	want   string
}

// consumerFieldCases держит таблицу отдельно от тела теста: список растёт с
// каждым новым полем Consumer, а сама проверка не меняется — держать их одной
// функцией значит регулярно упираться в лимит длины на ровном месте.
func consumerFieldCases() []consumerFieldCase {
	return []consumerFieldCase{
		{
			name:   "пустая группа",
			mutate: func(c *Consumer) { c.Group = "" },
			want:   "consumer.group must not be empty",
		},
		{
			name:   "нулевой session_timeout",
			mutate: func(c *Consumer) { c.SessionTimeout = 0 },
			want:   "consumer.session_timeout must be positive",
		},
		{
			name:   "нулевой heartbeat_interval",
			mutate: func(c *Consumer) { c.HeartbeatInterval = 0 },
			want:   "consumer.heartbeat_interval must be positive",
		},
		{
			name:   "нулевой rebalance_timeout",
			mutate: func(c *Consumer) { c.RebalanceTimeout = 0 },
			want:   "consumer.rebalance_timeout must be positive",
		},
		{
			name:   "нулевой commit_interval",
			mutate: func(c *Consumer) { c.CommitInterval = 0 },
			want:   "consumer.commit_interval must be positive",
		},
		{
			name:   "нулевой max_wait",
			mutate: func(c *Consumer) { c.MaxWait = 0 },
			want:   "consumer.max_wait must be positive",
		},
		{
			// Heartbeat не короче сессии означает, что группа развалится по
			// таймауту раньше первого удара сердца.
			name: "heartbeat_interval равен session_timeout",
			mutate: func(c *Consumer) {
				c.HeartbeatInterval = c.SessionTimeout
			},
			want: "must be less than consumer.session_timeout",
		},
		{
			name: "heartbeat_interval больше session_timeout",
			mutate: func(c *Consumer) {
				c.HeartbeatInterval = c.SessionTimeout + time.Second
			},
			want: "must be less than consumer.session_timeout",
		},
		{
			// Отрицательное значение уронило бы make(chan, n) паникой уже
			// после того, как конструктор вернул nil-ошибку.
			name:   "отрицательный message_queue_size",
			mutate: func(c *Consumer) { c.MessageQueueSize = -1 },
			want:   "consumer.message_queue_size must be positive",
		},
		{
			name:   "нулевой max_poll_records",
			mutate: func(c *Consumer) { c.MaxPollRecords = 0 },
			want:   "consumer.max_poll_records must be positive",
		},
		{
			name:   "неизвестный initial_offset",
			mutate: func(c *Consumer) { c.InitialOffset = "beginning" },
			want:   "consumer.initial_offset must be",
		},
		{
			name:   "пустой initial_offset",
			mutate: func(c *Consumer) { c.InitialOffset = "" },
			want:   "consumer.initial_offset must be",
		},
		{
			name:   "неизвестный isolation_level",
			mutate: func(c *Consumer) { c.IsolationLevel = "read_dirty" },
			want:   "consumer.isolation_level must be",
		},
		{
			name:   "handler_max_retries меньше -1",
			mutate: func(c *Consumer) { c.HandlerMaxRetries = -2 },
			want:   "consumer.handler_max_retries must be -1",
		},
		{
			// Ретраи включены, а паузы между ними нет: партиция закрутилась бы
			// в busy loop на первом же отравленном сообщении.
			name: "ретраи без задержки",
			mutate: func(c *Consumer) {
				c.HandlerMaxRetries = 3
				c.HandlerRetryDelay = 0
			},
			want: "consumer.handler_retry_delay must be positive",
		},
		{
			// При выключенных ретраях задержка не используется, и требовать
			// её значило бы отвергать вполне рабочий конфиг.
			name: "нулевая задержка без ретраев",
			mutate: func(c *Consumer) {
				c.HandlerMaxRetries = 0
				c.HandlerRetryDelay = 0
			},
		},
		{
			name: "бесконечные ретраи с задержкой",
			mutate: func(c *Consumer) {
				c.HandlerMaxRetries = -1
				c.HandlerRetryDelay = time.Second
			},
		},
		{
			// Регистр значений из yaml не фиксирован, сравнение идёт через
			// ToLower.
			name:   "initial_offset в верхнем регистре",
			mutate: func(c *Consumer) { c.InitialOffset = "LATEST" },
		},
		{
			name:   "isolation_level в верхнем регистре",
			mutate: func(c *Consumer) { c.IsolationLevel = "READ_COMMITTED" },
		},
	}
}

func TestConfigValidateConsumerFields(t *testing.T) {
	t.Parallel()

	for _, tt := range consumerFieldCases() {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := testConfig(t)
			tt.mutate(&cfg.Consumer)

			err := cfg.validateConsumer()
			if tt.want == "" {
				cfgWantNoErr(t, err)

				return
			}

			cfgWantErr(t, err, tt.want)
			// Секция Consumer не должна волновать продюсер.
			cfgWantNoErr(t, cfg.validateProducer())
		})
	}
}

// TestConfigValidateDurationLimitsMatchFranzGo проверяет не только текст нашей
// ошибки, но и её основание: каждое значение прогоняется через настоящий
// kgo.NewClient в обход Validate. Если franz-go смягчит или ужесточит границу,
// упадёт именно этот тест, а не пользователь на проде.
func TestConfigValidateDurationLimitsMatchFranzGo(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mutate   func(*Config)
		want     string
		kgoWant  string
		consumer bool
	}{
		{
			name:    "message_timeout ниже секунды",
			mutate:  func(c *Config) { c.Producer.MessageTimeout = 900 * time.Millisecond },
			want:    "producer.message_timeout must be at least 1s",
			kgoWant: "record timeout",
		},
		{
			name:    "ack_timeout ниже 100ms",
			mutate:  func(c *Config) { c.Producer.AckTimeout = 50 * time.Millisecond },
			want:    "producer.ack_timeout must be at least 100ms",
			kgoWant: "produce timeout",
		},
		{
			name:    "linger больше минуты",
			mutate:  func(c *Config) { c.Producer.Linger = 2 * time.Minute },
			want:    "producer.linger must not exceed 1m",
			kgoWant: "linger",
		},
		{
			name: "session_timeout ниже 100ms",
			mutate: func(c *Config) {
				// Heartbeat опускается вместе с ним: иначе к проверке границы
				// примешается отдельная претензия «heartbeat >= session».
				c.Consumer.SessionTimeout = 50 * time.Millisecond
				c.Consumer.HeartbeatInterval = 20 * time.Millisecond
			},
			want:     "consumer.session_timeout must be at least 100ms",
			kgoWant:  "session timeout",
			consumer: true,
		},
		{
			name:     "rebalance_timeout ниже 100ms",
			mutate:   func(c *Config) { c.Consumer.RebalanceTimeout = 50 * time.Millisecond },
			want:     "consumer.rebalance_timeout must be at least 100ms",
			kgoWant:  "rebalance timeout",
			consumer: true,
		},
		{
			name:     "commit_interval ниже 100ms",
			mutate:   func(c *Config) { c.Consumer.CommitInterval = 50 * time.Millisecond },
			want:     "consumer.commit_interval must be at least 100ms",
			kgoWant:  "autocommit interval",
			consumer: true,
		},
		{
			name:     "max_wait ниже 10ms",
			mutate:   func(c *Config) { c.Consumer.MaxWait = 5 * time.Millisecond },
			want:     "consumer.max_wait must be at least 10ms",
			kgoWant:  "max fetch wait",
			consumer: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := testConfig(t)
			tt.mutate(&cfg)

			if tt.consumer {
				cfgWantErr(t, cfg.validateConsumer(), tt.want)
				cfgWantNoErr(t, cfg.validateProducer())
			} else {
				cfgWantErr(t, cfg.validateProducer(), tt.want)
				cfgWantNoErr(t, cfg.validateConsumer())
			}

			cfgWantErr(t, cfgClientError(t, cfg, tt.consumer), tt.kgoWant)
		})
	}
}

// cfgClientError собирает клиента franz-go напрямую из опций, минуя Validate, и
// возвращает ошибку конструктора.
func cfgClientError(t *testing.T, cfg Config, consumer bool) error {
	t.Helper()

	var (
		opts []kgo.Opt
		err  error
	)

	if consumer {
		opts, err = cfg.consumerOpts(testLogger(t), []string{testTopic}, rebalanceCallbacks{})
	} else {
		opts, err = cfg.producerOpts(testLogger(t))
	}

	if err != nil {
		t.Fatalf("сборка опций: %v", err)
	}

	client, err := kgo.NewClient(opts...)
	if err == nil {
		client.Close()
		t.Fatal("kgo.NewClient принял конфигурацию, которую отвергает Validate")
	}

	return err
}

func TestConfigValidateReturnsJoinedErrors(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t)
	cfg.Consumer.Group = ""
	cfg.Consumer.MaxPollRecords = 0

	err := cfg.validateConsumer()

	// errors.Is по каждому элементу списка: клиентский код вправе искать в
	// объединённой ошибке конкретную, а не разбирать текст.
	errs := cfgUnwrapJoined(t, err)
	if len(errs) != 2 {
		t.Fatalf("получено %d ошибок, ожидалось 2: %v", len(errs), errs)
	}

	for _, e := range errs {
		if !errors.Is(err, e) {
			t.Errorf("errors.Is не находит вложенную ошибку %v в %v", e, err)
		}
	}
}
