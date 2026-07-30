package kafkax

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/twmb/franz-go/plugin/kslog"
)

// commonOpts собирает опции, общие для продюсера и консьюмера: адреса,
// идентификатор, транспорт, аутентификация, логирование.
//
// Опции собираются типизированными конструкторами, а не строковыми ключами:
// опечатку в имени настройки или несовпадение типа значения ловит компилятор,
// а не рантайм при создании клиента.
func (c Config) commonOpts(logger *slog.Logger) ([]kgo.Opt, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(c.Brokers...),
		kgo.ClientID(c.ClientID),
		kgo.DialTimeout(c.DialTimeout),
		kgo.WithLogger(kslog.New(c.kafkaLogger(logger))),
	}

	tlsCfg, err := c.tlsConfig(logger)
	if err != nil {
		return nil, err
	}

	if tlsCfg != nil {
		opts = append(opts, kgo.DialTLSConfig(tlsCfg))
	}

	if c.SASL.enabled() {
		mech, err := c.saslMechanism()
		if err != nil {
			return nil, err
		}

		// Проверяется результат tlsConfig, а не поля конфигурации: здесь уже
		// известно, поедет ли в клиента DialTLSConfig, и никакое расхождение с
		// валидацией невозможно.
		if tlsCfg == nil {
			warnPlaintextSASL(logger, c.SASL.Mechanism)
		}

		opts = append(opts, kgo.SASL(mech))
	}

	return opts, nil
}

// tlsConfig строит *tls.Config или возвращает nil, если TLS не нужен.
//
// Порядок приоритета: явный Config.TLSConfig побеждает секцию TLS целиком.
// Смешивать их нельзя — иначе непонятно, чей ServerName и чей RootCAs
// оказываются в итоге.
func (c Config) tlsConfig(logger *slog.Logger) (*tls.Config, error) {
	if c.TLSConfig != nil {
		if c.TLSConfig.InsecureSkipVerify {
			warnInsecureTLS(logger)
		}

		return c.TLSConfig, nil
	}

	if !c.TLS.enabled() {
		return nil, nil //nolint:nilnil // «TLS не нужен» — законный результат, а не отсутствие значения
	}

	// MinVersion задаётся явно: умолчание crypto/tls зависит от версии Go, а
	// молчаливое согласие на TLS 1.0 в брокерском соединении — не то, что
	// библиотека должна делать за пользователя.
	cfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		ServerName:         c.TLS.ServerName,
		InsecureSkipVerify: c.TLS.InsecureSkipVerify, //nolint:gosec // значение приходит из конфигурации и логируется
	}

	if c.TLS.InsecureSkipVerify {
		warnInsecureTLS(logger)
	}

	if c.TLS.CACertPath != "" {
		pool, err := caCertPool(c.TLS.CACertPath)
		if err != nil {
			return nil, err
		}

		cfg.RootCAs = pool
	}

	if c.TLS.ClientCertPath != "" {
		cert, err := tls.LoadX509KeyPair(c.TLS.ClientCertPath, c.TLS.ClientKeyPath)
		if err != nil {
			return nil, fmt.Errorf("loading client key pair: %w", err)
		}

		cfg.Certificates = []tls.Certificate{cert}
	}

	return cfg, nil
}

// warnInsecureTLS пишет предупреждение при отключённой проверке сертификата.
//
// Молча согласиться нельзя: конфигурация, отключающая проверку, обычно
// приезжает из локальной отладки и доезжает до прода незамеченной. Ошибкой это
// тоже не делается — сценарий отладки законный.
func warnInsecureTLS(logger *slog.Logger) {
	logger.Warn("TLS certificate verification is disabled (InsecureSkipVerify); " +
		"the connection is vulnerable to man-in-the-middle attacks")
}

// warnPlaintextSASL пишет предупреждение при аутентификации поверх
// незашифрованного соединения.
//
// Два текста, потому что риски разные и путать их вредно. SCRAM пароль по
// проводу не передаёт — без TLS он уязвим к MITM ровно как соединение с
// InsecureSkipVerify, и предупреждение здесь той же силы. PLAIN отправляет
// `zid\0user\0pass` открытым текстом; сюда эта ветка доходит только с явным
// sasl.allow_plaintext=true (иначе конфигурация не прошла бы валидацию), но
// напомнить об уже принятом решении в момент подключения всё равно стоит:
// флаг обычно ставят один раз для локального стенда, а конфигурация потом
// уезжает дальше.
func warnPlaintextSASL(logger *slog.Logger, mechanism string) {
	if strings.EqualFold(mechanism, SASLMechanismPlain) {
		logger.Warn("SASL PLAIN is used over an unencrypted connection (sasl.allow_plaintext=true); "+
			"the password is sent to the broker in cleartext and must be treated as disclosed",
			slog.String("sasl_mechanism", mechanism))

		return
	}

	logger.Warn("SASL authentication is used over an unencrypted connection; "+
		"the exchange is vulnerable to man-in-the-middle attacks",
		slog.String("sasl_mechanism", mechanism))
}

func caCertPool(path string) (*x509.CertPool, error) {
	// Путь берётся из tls.ca_cert_path — это и есть назначение функции.
	// Источник пути тот же, что у адресов брокеров и учётных данных SASL: кто
	// правит конфигурацию приложения, тот и так управляет его соединениями.
	pem, err := os.ReadFile(path) //nolint:gosec // путь к CA-сертификату приходит из конфигурации по назначению
	if err != nil {
		return nil, fmt.Errorf("reading CA certificate: %w", err)
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("parsing CA certificate %s: no certificates found", path)
	}

	return pool, nil
}

// saslMechanism строит механизм SASL по имени.
//
// Учётные данные передаются замыканием, а не значением: franz-go вызывает его
// на каждой переаутентификации, так что здесь появляется точка, куда позже
// можно подставить ротацию секрета, не меняя сигнатуру.
func (c Config) saslMechanism() (sasl.Mechanism, error) {
	switch strings.ToUpper(c.SASL.Mechanism) {
	case SASLMechanismPlain:
		return plain.Auth{User: c.SASL.Username, Pass: c.SASL.Password}.AsMechanism(), nil
	case SASLMechanismScramSHA256:
		return scram.Auth{User: c.SASL.Username, Pass: c.SASL.Password}.AsSha256Mechanism(), nil
	case SASLMechanismScramSHA512:
		return scram.Auth{User: c.SASL.Username, Pass: c.SASL.Password}.AsSha512Mechanism(), nil
	default:
		return nil, fmt.Errorf("unsupported %s %q", cfgField("SASL.Mechanism"), c.SASL.Mechanism)
	}
}

// producerOpts собирает опции продюсера поверх общих.
func (c Config) producerOpts(logger *slog.Logger) ([]kgo.Opt, error) {
	opts, err := c.commonOpts(logger)
	if err != nil {
		return nil, err
	}

	codec, err := compressionCodec(c.Producer.CompressionType)
	if err != nil {
		return nil, err
	}

	opts = append(opts,
		kgo.RequiredAcks(acks(c.Producer.RequiredAcks)),
		kgo.ProduceRequestTimeout(c.Producer.AckTimeout),
		kgo.RecordRetries(c.Producer.MaxRetries),
		kgo.RecordDeliveryTimeout(c.Producer.MessageTimeout),
		kgo.ProducerLinger(c.Producer.Linger),
		kgo.ProducerBatchMaxBytes(c.Producer.BatchBytes),
		kgo.ProducerBatchCompression(codec),
		kgo.MaxBufferedRecords(c.Producer.MaxBufferedRecords),
		kgo.RetryBackoffFn(constantBackoff(c.Producer.RetryBackoff)),
	)

	if c.Producer.MaxBufferedBytes > 0 {
		opts = append(opts, kgo.MaxBufferedBytes(c.Producer.MaxBufferedBytes))
	}

	// Идемпотентность в franz-go включена по умолчанию, поэтому опция нужна
	// только чтобы её выключить. Комбинация «идемпотентность + acks≠-1»
	// отсечена в Validate, здесь она уже невозможна.
	//
	// MaxInflight задаётся только вместе с отключением: у идемпотентного
	// продюсера потолок определяет протокол, и franz-go отвергает попытку
	// задать его вручную ошибкой конструктора клиента.
	if !c.Producer.EnableIdempotence {
		opts = append(opts,
			kgo.DisableIdempotentWrite(),
			kgo.MaxProduceRequestsInflightPerBroker(c.Producer.MaxInflight),
		)
	}

	return append(opts, c.ExtraOpts...), nil
}

// consumerOpts собирает опции консьюмера поверх общих.
//
// AutoCommitMarks вместо автокоммита по времени: коммитится только то, что
// явно отмечено через MarkCommitRecords после успешной обработки. Обычный
// автокоммит двигал бы оффсет по факту чтения, а не обработки, и превращал бы
// at-least-once в at-most-once при падении воркера.
func (c Config) consumerOpts(logger *slog.Logger, topics []string, cb rebalanceCallbacks) ([]kgo.Opt, error) {
	opts, err := c.commonOpts(logger)
	if err != nil {
		return nil, err
	}

	opts = append(opts,
		kgo.ConsumerGroup(c.Consumer.Group),
		kgo.ConsumeTopics(topics...),
		kgo.ConsumeResetOffset(initialOffset(c.Consumer.InitialOffset)),
		kgo.FetchIsolationLevel(isolationLevel(c.Consumer.IsolationLevel)),
		kgo.FetchMinBytes(c.Consumer.MinBytes),
		kgo.FetchMaxBytes(c.Consumer.MaxBytes),
		kgo.FetchMaxPartitionBytes(c.Consumer.MaxPartitionBytes),
		kgo.FetchMaxWait(c.Consumer.MaxWait),
		kgo.SessionTimeout(c.Consumer.SessionTimeout),
		kgo.HeartbeatInterval(c.Consumer.HeartbeatInterval),
		kgo.RebalanceTimeout(c.Consumer.RebalanceTimeout),
		kgo.AutoCommitMarks(),
		kgo.AutoCommitInterval(c.Consumer.CommitInterval),
		kgo.OnPartitionsAssigned(cb.assigned),
		kgo.OnPartitionsRevoked(cb.revoked),
		kgo.OnPartitionsLost(cb.lost),
		// Опрос и колбэки ребаланса становятся взаимно исключающими, так что
		// карта партиционных воркеров не требует мьютекса. Плата: каждая
		// итерация цикла обязана заканчиваться AllowRebalance(), а закрытие —
		// CloseAllowingRebalance() вместо Close().
		kgo.BlockRebalanceOnPoll(),
	)

	return append(opts, c.ExtraOpts...), nil
}

// rebalanceCallbacks — три колбэка ребаланса, которые консьюмер передаёт в
// конфигурацию клиента. Собраны в структуру, чтобы consumerOpts не разрастался
// тремя параметрами одного типа, которые легко перепутать местами.
type rebalanceCallbacks struct {
	assigned func(context.Context, *kgo.Client, map[string][]int32)
	revoked  func(context.Context, *kgo.Client, map[string][]int32)
	lost     func(context.Context, *kgo.Client, map[string][]int32)
}

func acks(required int) kgo.Acks {
	switch required {
	case 0:
		return kgo.NoAck()
	case 1:
		return kgo.LeaderAck()
	default:
		return kgo.AllISRAcks()
	}
}

func compressionCodec(name string) (kgo.CompressionCodec, error) {
	switch strings.ToLower(name) {
	case CompressionNone:
		return kgo.NoCompression(), nil
	case CompressionGzip:
		return kgo.GzipCompression(), nil
	case CompressionSnappy:
		return kgo.SnappyCompression(), nil
	case CompressionLZ4:
		return kgo.Lz4Compression(), nil
	case CompressionZstd:
		return kgo.ZstdCompression(), nil
	default:
		return kgo.CompressionCodec{}, fmt.Errorf(
			"%s must be one of %s, %s, %s, %s, %s; got %q",
			cfgField("Producer.CompressionType"),
			CompressionNone, CompressionGzip, CompressionSnappy,
			CompressionLZ4, CompressionZstd, name)
	}
}

func initialOffset(name string) kgo.Offset {
	if strings.EqualFold(name, OffsetLatest) {
		return kgo.NewOffset().AtEnd()
	}

	return kgo.NewOffset().AtStart()
}

func isolationLevel(name string) kgo.IsolationLevel {
	if strings.EqualFold(name, IsolationReadUncommitted) {
		return kgo.ReadUncommitted()
	}

	return kgo.ReadCommitted()
}

// constantBackoff повторяет паузу без роста.
//
// franz-go по умолчанию наращивает паузу экспоненциально с джиттером, что
// лучше; функция существует только чтобы настройка producer.retry_backoff
// означала именно фиксированную паузу.
func constantBackoff(d time.Duration) func(int) time.Duration {
	return func(int) time.Duration { return d }
}
