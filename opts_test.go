package kafkax

import (
	"context"
	"crypto/tls"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Тесты сборки опций kgo.
//
// Сами kgo.Opt непрозрачны — это функции над приватной структурой, сравнить их
// значением нельзя. Поэтому проверка идёт через kgo.NewClient: клиент валидирует
// набор опций в конструкторе и отдаёт применённые значения через OptValue.
// Так тест ловит не «мы позвали нужную функцию», а «franz-go принял результат»,
// что и есть предмет ошибок в этом слое.

// optsClient создаёт клиента из готового набора опций и закрывает его по
// завершении теста. Брокер недоступен намеренно: franz-go подключается лениво,
// конструктор до сети не доходит.
func optsClient(t *testing.T, opts []kgo.Opt) *kgo.Client {
	t.Helper()

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}

	t.Cleanup(cl.Close)

	return cl
}

// optsNoopCallbacks — заглушки колбэков ребаланса для consumerOpts.
func optsNoopCallbacks() rebalanceCallbacks {
	noop := func(context.Context, *kgo.Client, map[string][]int32) {}

	return rebalanceCallbacks{assigned: noop, revoked: noop, lost: noop}
}

func TestProducerOptsBuildValidClient(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t)

	opts, err := cfg.producerOpts(testLogger(t))
	if err != nil {
		t.Fatalf("producerOpts: %v", err)
	}

	cl := optsClient(t, opts)

	if got := cl.OptValue(kgo.ClientID); got != testClientID {
		t.Errorf("ClientID = %v, want %q", got, testClientID)
	}

	if got := cl.OptValue(kgo.RequiredAcks); got != kgo.AllISRAcks() {
		t.Errorf("RequiredAcks = %v, want AllISRAcks", got)
	}

	if got := cl.OptValue(kgo.MaxBufferedRecords); got != int64(cfg.Producer.MaxBufferedRecords) {
		t.Errorf("MaxBufferedRecords = %v, want %d", got, cfg.Producer.MaxBufferedRecords)
	}

	if got := cl.OptValue(kgo.RecordDeliveryTimeout); got != cfg.Producer.MessageTimeout {
		t.Errorf("RecordDeliveryTimeout = %v, want %v", got, cfg.Producer.MessageTimeout)
	}

	// TLS и SASL в базовой конфигурации выключены: наличие *tls.Config здесь
	// означало бы, что библиотека включает шифрование сама по себе.
	if got := cl.OptValue(kgo.DialTLSConfig); got != (*tls.Config)(nil) {
		t.Errorf("DialTLSConfig = %v, want nil", got)
	}
}

func TestConsumerOptsBuildValidClient(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t)

	opts, err := cfg.consumerOpts(testLogger(t), []string{testTopic}, optsNoopCallbacks())
	if err != nil {
		t.Fatalf("consumerOpts: %v", err)
	}

	cl := optsClient(t, opts)

	if got := cl.OptValue(kgo.ConsumerGroup); got != testGroup {
		t.Errorf("ConsumerGroup = %v, want %q", got, testGroup)
	}

	if got := cl.OptValue(kgo.SessionTimeout); got != cfg.Consumer.SessionTimeout {
		t.Errorf("SessionTimeout = %v, want %v", got, cfg.Consumer.SessionTimeout)
	}

	// Две опции, на которых держатся гарантии консьюмера: AutoCommitMarks
	// делает коммит зависимым от обработки, а не от чтения (иначе
	// at-least-once превращается в at-most-once), а BlockRebalanceOnPoll —
	// причина, по которой карта партиционных воркеров живёт без мьютекса.
	if got := cl.OptValue(kgo.AutoCommitMarks); got != true {
		t.Errorf("AutoCommitMarks = %v, want true", got)
	}

	if got := cl.OptValue(kgo.BlockRebalanceOnPoll); got != true {
		t.Errorf("BlockRebalanceOnPoll = %v, want true", got)
	}
}

func TestCompressionCodecMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want kgo.CompressionCodec
	}{
		{name: "none", in: CompressionNone, want: kgo.NoCompression()},
		{name: "gzip", in: CompressionGzip, want: kgo.GzipCompression()},
		{name: "snappy", in: CompressionSnappy, want: kgo.SnappyCompression()},
		{name: "lz4", in: CompressionLZ4, want: kgo.Lz4Compression()},
		{name: "zstd", in: CompressionZstd, want: kgo.ZstdCompression()},
		// Значение приходит из yaml, где регистр не фиксирован.
		{name: "верхний регистр", in: "GZIP", want: kgo.GzipCompression()},
		{name: "смешанный регистр", in: "Zstd", want: kgo.ZstdCompression()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := compressionCodec(tt.in)
			if err != nil {
				t.Fatalf("compressionCodec(%q): %v", tt.in, err)
			}

			if got != tt.want {
				t.Errorf("compressionCodec(%q) = %+v, want %+v", tt.in, got, tt.want)
			}
		})
	}
}

func TestCompressionCodecRejectsUnknown(t *testing.T) {
	t.Parallel()

	for _, in := range []string{"", "brotli", "gzip2", " gzip"} {
		t.Run("вход "+in, func(t *testing.T) {
			t.Parallel()

			// Неизвестный кодек не должен молча превращаться в none: тогда
			// топик, настроенный на сжатие, тихо принимал бы несжатые батчи.
			if _, err := compressionCodec(in); err == nil {
				t.Fatalf("compressionCodec(%q) вернул nil-ошибку", in)
			}

			cfg := testConfig(t)
			cfg.Producer.CompressionType = in

			if _, err := cfg.producerOpts(testLogger(t)); err == nil {
				t.Fatal("producerOpts не отверг неизвестный compression_type")
			}
		})
	}
}

func TestProducerOptsCompressionReachesClient(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t)
	cfg.Producer.CompressionType = CompressionZstd

	opts, err := cfg.producerOpts(testLogger(t))
	if err != nil {
		t.Fatalf("producerOpts: %v", err)
	}

	cl := optsClient(t, opts)

	codecs, ok := cl.OptValue(kgo.ProducerBatchCompression).([]kgo.CompressionCodec)
	if !ok || len(codecs) == 0 {
		t.Fatalf("ProducerBatchCompression = %#v, ожидался непустой []CompressionCodec", cl.OptValue(kgo.ProducerBatchCompression))
	}

	if codecs[0] != kgo.ZstdCompression() {
		t.Errorf("кодек клиента = %+v, want zstd", codecs[0])
	}
}

// TestProducerOptsIdempotenceInflight — регрессия на пару
// EnableIdempotence/MaxInflight.
//
// franz-go отвергает MaxProduceRequestsInflightPerBroker вместе с
// идемпотентностью ошибкой конструктора «invalid usage of
// MaxProduceRequestsInflightPerBroker with idempotency enabled»: у
// идемпотентного продюсера потолок задаёт протокол. Опция должна добавляться
// ТОЛЬКО при EnableIdempotence=false.
//
// Проверка поведенческая — через kgo.NewClient: набор kgo.Opt непрозрачен, и
// увидеть лишнюю опцию иначе нельзя. Если условие в producerOpts исчезнет,
// упадёт подтест с идемпотентностью, то есть конфигурация по умолчанию.
func TestProducerOptsIdempotenceInflight(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		idempotence bool
		wantDisable bool
	}{
		{name: "идемпотентность включена (умолчание)", idempotence: true, wantDisable: false},
		{name: "идемпотентность выключена", idempotence: false, wantDisable: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := testConfig(t)
			cfg.Producer.EnableIdempotence = tt.idempotence
			cfg.Producer.MaxInflight = 5

			opts, err := cfg.producerOpts(testLogger(t))
			if err != nil {
				t.Fatalf("producerOpts: %v", err)
			}

			cl := optsClient(t, opts)

			if got := cl.OptValue(kgo.DisableIdempotentWrite); got != tt.wantDisable {
				t.Errorf("DisableIdempotentWrite = %v, want %v", got, tt.wantDisable)
			}

			if !tt.idempotence {
				// При выключенной идемпотентности значение из конфигурации
				// обязано доехать: единица там нужна для сохранения порядка.
				if got := cl.OptValue(kgo.MaxProduceRequestsInflightPerBroker); got != cfg.Producer.MaxInflight {
					t.Errorf("MaxProduceRequestsInflightPerBroker = %v, want %d", got, cfg.Producer.MaxInflight)
				}
			}
		})
	}
}

func TestProducerOptsMaxBufferedBytes(t *testing.T) {
	t.Parallel()

	// Ноль в конфигурации означает «без лимита» и опцию выставлять не должен.
	// Отдельная ветка нужна потому, что int64(0) в franz-go — тоже «без
	// лимита», но полагаться на совпадение умолчаний чужой библиотеки нельзя:
	// смена умолчания превратила бы «без лимита» в мгновенно полный буфер.
	cfg := testConfig(t)
	cfg.Producer.MaxBufferedBytes = 0

	opts, err := cfg.producerOpts(testLogger(t))
	if err != nil {
		t.Fatalf("producerOpts: %v", err)
	}

	if got := optsClient(t, opts).OptValue(kgo.MaxBufferedBytes); got != int64(0) {
		t.Errorf("MaxBufferedBytes = %v, ожидалось отсутствие лимита (0)", got)
	}

	cfg.Producer.MaxBufferedBytes = 1 << 20

	opts, err = cfg.producerOpts(testLogger(t))
	if err != nil {
		t.Fatalf("producerOpts: %v", err)
	}

	if got := optsClient(t, opts).OptValue(kgo.MaxBufferedBytes); got != int64(cfg.Producer.MaxBufferedBytes) {
		t.Errorf("MaxBufferedBytes = %v, want %d", got, cfg.Producer.MaxBufferedBytes)
	}
}

func TestExtraOptsAppendedLast(t *testing.T) {
	t.Parallel()

	// ExtraOpts — аварийный выход: они добавляются последними и потому
	// побеждают всё, что вывела библиотека. Проверяется переопределением
	// ClientID, который commonOpts выставляет из конфигурации.
	const override = "extra-opts-wins"

	t.Run("producer", func(t *testing.T) {
		t.Parallel()

		cfg := testConfig(t)
		cfg.ExtraOpts = []kgo.Opt{kgo.ClientID(override)}

		opts, err := cfg.producerOpts(testLogger(t))
		if err != nil {
			t.Fatalf("producerOpts: %v", err)
		}

		if got := optsClient(t, opts).OptValue(kgo.ClientID); got != override {
			t.Errorf("ClientID = %v, want %q", got, override)
		}
	})

	t.Run("consumer", func(t *testing.T) {
		t.Parallel()

		cfg := testConfig(t)
		cfg.ExtraOpts = []kgo.Opt{kgo.ClientID(override)}

		opts, err := cfg.consumerOpts(testLogger(t), []string{testTopic}, optsNoopCallbacks())
		if err != nil {
			t.Fatalf("consumerOpts: %v", err)
		}

		if got := optsClient(t, opts).OptValue(kgo.ClientID); got != override {
			t.Errorf("ClientID = %v, want %q", got, override)
		}
	})
}

func TestTLSConfigExplicitWinsOverSection(t *testing.T) {
	t.Parallel()

	explicit := &tls.Config{MinVersion: tls.VersionTLS13, ServerName: "explicit"}

	cfg := testConfig(t)
	cfg.TLSConfig = explicit
	cfg.TLS = TLS{Enabled: true, ServerName: "from-section", CACertPath: "/nonexistent/ca.pem"}

	got, err := cfg.tlsConfig(testLogger(t))
	if err != nil {
		t.Fatalf("tlsConfig: %v", err)
	}

	// Именно тот же указатель, а не «похожая» копия: смешивание источников
	// сделало бы неочевидным, чей ServerName и чей RootCAs победили. Заодно
	// это доказывает, что секция TLS не читалась вовсе — иначе несуществующий
	// CACertPath дал бы ошибку.
	if got != explicit {
		t.Fatalf("tlsConfig вернул %p, ожидался явный Config %p", got, explicit)
	}
}

func TestTLSConfigFromSection(t *testing.T) {
	t.Parallel()

	t.Run("выключен — nil без ошибки", func(t *testing.T) {
		t.Parallel()

		cfg := testConfig(t)
		cfg.TLS = TLS{ServerName: "ignored", CACertPath: "/nonexistent/ca.pem"}

		got, err := cfg.tlsConfig(testLogger(t))
		if err != nil {
			t.Fatalf("tlsConfig: %v", err)
		}

		// Пути без Enabled не включают TLS: флаг — единственный переключатель.
		if got != nil {
			t.Fatalf("tlsConfig = %+v, want nil", got)
		}
	})

	t.Run("включён без сертификатов", func(t *testing.T) {
		t.Parallel()

		cfg := testConfig(t)
		cfg.TLS = TLS{Enabled: true, ServerName: "broker.example"}

		got, err := cfg.tlsConfig(testLogger(t))
		if err != nil {
			t.Fatalf("tlsConfig: %v", err)
		}

		if got == nil {
			t.Fatal("tlsConfig = nil при TLS.Enabled=true")
		}

		// MinVersion задаётся явно: умолчание crypto/tls зависит от версии Go,
		// и молчаливое согласие на TLS 1.0 в брокерском соединении — не то,
		// что библиотека вправе решать за пользователя.
		if got.MinVersion != tls.VersionTLS12 {
			t.Errorf("MinVersion = %#x, want TLS1.2 (%#x)", got.MinVersion, tls.VersionTLS12)
		}

		if got.ServerName != "broker.example" {
			t.Errorf("ServerName = %q, want %q", got.ServerName, "broker.example")
		}

		if got.InsecureSkipVerify {
			t.Error("InsecureSkipVerify включился сам по себе")
		}
	})

	t.Run("insecure_skip_verify пробрасывается", func(t *testing.T) {
		t.Parallel()

		cfg := testConfig(t)
		cfg.TLS = TLS{Enabled: true, InsecureSkipVerify: true}

		got, err := cfg.tlsConfig(testLogger(t))
		if err != nil {
			t.Fatalf("tlsConfig: %v", err)
		}

		// Отладочный сценарий законный, поэтому здесь предупреждение в лог, а
		// не ошибка. Проверяется, что значение доезжает, а не отбрасывается.
		if !got.InsecureSkipVerify {
			t.Error("InsecureSkipVerify не доехал до *tls.Config")
		}
	})
}

func TestTLSConfigCACertErrors(t *testing.T) {
	t.Parallel()

	garbage := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(garbage, []byte("not a certificate"), 0o600); err != nil {
		t.Fatalf("подготовка файла: %v", err)
	}

	tests := []struct {
		name string
		path string
		want string
	}{
		{name: "файла нет", path: filepath.Join(t.TempDir(), "missing.pem"), want: "reading CA certificate"},
		{name: "не PEM", path: garbage, want: "parsing CA certificate"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := testConfig(t)
			cfg.TLS = TLS{Enabled: true, CACertPath: tt.path}

			// Нечитаемый CA обязан валить сборку опций, а не тихо откатываться
			// к системному trust store: иначе клиент пошёл бы проверять
			// брокера не тем набором корней, чем указано в конфигурации.
			_, err := cfg.producerOpts(testLogger(t))
			cfgWantErr(t, err, tt.want)
		})
	}
}

func TestSASLMechanismMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		mechanism string
		wantName  string
		wantErr   bool
	}{
		{name: "PLAIN", mechanism: SASLMechanismPlain, wantName: "PLAIN"},
		{name: "нижний регистр", mechanism: "plain", wantName: "PLAIN"},
		{name: "SCRAM-SHA-256", mechanism: SASLMechanismScramSHA256, wantName: "SCRAM-SHA-256"},
		{name: "SCRAM-SHA-512", mechanism: SASLMechanismScramSHA512, wantName: "SCRAM-SHA-512"},
		{name: "неизвестный", mechanism: "GSSAPI", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := testConfig(t)
			cfg.SASL = SASL{Mechanism: tt.mechanism, Username: "u", Password: "p"}

			mech, err := cfg.saslMechanism()
			if tt.wantErr {
				if err == nil {
					t.Fatal("saslMechanism принял неизвестный механизм")
				}

				return
			}

			if err != nil {
				t.Fatalf("saslMechanism: %v", err)
			}

			if mech.Name() != tt.wantName {
				t.Errorf("Name() = %q, want %q", mech.Name(), tt.wantName)
			}
		})
	}
}

func TestCommonOptsAttachSASL(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t)
	cfg.SASL = SASL{Mechanism: SASLMechanismScramSHA512, Username: "u", Password: "p"}

	opts, err := cfg.producerOpts(testLogger(t))
	if err != nil {
		t.Fatalf("producerOpts: %v", err)
	}

	// Механизм должен доехать до клиента: без kgo.SASL клиент подключится без
	// аутентификации и получит отказ уже от брокера.
	if got := optsClient(t, opts).OptValue(kgo.SASL); got == nil {
		t.Fatal("SASL-механизм не попал в опции клиента")
	}

	cfg.SASL.Mechanism = "GSSAPI"

	if _, err := cfg.producerOpts(testLogger(t)); err == nil {
		t.Error("producerOpts принял неизвестный механизм SASL")
	}
}

func TestInitialOffsetMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want kgo.Offset
	}{
		{name: "earliest", in: OffsetEarliest, want: kgo.NewOffset().AtStart()},
		{name: "latest", in: OffsetLatest, want: kgo.NewOffset().AtEnd()},
		{name: "latest в верхнем регистре", in: "LATEST", want: kgo.NewOffset().AtEnd()},
		// Невалидное значение отсекается на Validate; здесь фиксируется
		// безопасное поведение остатка пути — откат к началу, а не к концу:
		// пропустить накопленное хуже, чем перечитать.
		{name: "мусор откатывается к earliest", in: "beginning", want: kgo.NewOffset().AtStart()},
		{name: "пустая строка", in: "", want: kgo.NewOffset().AtStart()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := initialOffset(tt.in); got != tt.want {
				t.Errorf("initialOffset(%q) = %+v, want %+v", tt.in, got, tt.want)
			}
		})
	}
}

func TestIsolationLevelMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want kgo.IsolationLevel
	}{
		{name: "read_committed", in: IsolationReadCommitted, want: kgo.ReadCommitted()},
		{name: "read_uncommitted", in: IsolationReadUncommitted, want: kgo.ReadUncommitted()},
		{name: "верхний регистр", in: "READ_UNCOMMITTED", want: kgo.ReadUncommitted()},
		// Мусор даёт read_committed — более строгий из двух вариантов.
		{name: "мусор", in: "read_dirty", want: kgo.ReadCommitted()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := isolationLevel(tt.in); got != tt.want {
				t.Errorf("isolationLevel(%q) = %+v, want %+v", tt.in, got, tt.want)
			}
		})
	}
}

func TestAcksMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   int
		want kgo.Acks
	}{
		{name: "0 — без подтверждения", in: 0, want: kgo.NoAck()},
		{name: "1 — только лидер", in: 1, want: kgo.LeaderAck()},
		{name: "-1 — все реплики ISR", in: -1, want: kgo.AllISRAcks()},
		// Значения вне диапазона отсекает Validate; здесь важно, что остаток
		// пути падает в самый безопасный вариант, а не в NoAck.
		{name: "мусор — все реплики ISR", in: 42, want: kgo.AllISRAcks()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := acks(tt.in); got != tt.want {
				t.Errorf("acks(%d) = %+v, want %+v", tt.in, got, tt.want)
			}
		})
	}
}

func TestConstantBackoffDoesNotGrow(t *testing.T) {
	t.Parallel()

	const delay = 250 * time.Millisecond

	fn := constantBackoff(delay)

	// Функция существует только чтобы настройка producer.retry_backoff
	// означала именно фиксированную паузу: умолчание franz-go растёт
	// экспоненциально.
	for try := range 5 {
		if got := fn(try); got != delay {
			t.Errorf("backoff(%d) = %v, want %v", try, got, delay)
		}
	}
}
