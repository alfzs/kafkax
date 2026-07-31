package kafkax

import (
	"bytes"
	"context"
	"crypto/tls"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
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

	// Остальные скалярные настройки продюсера: каждую из них можно было
	// выбросить из producerOpts, не уронив ни одного теста (O26, O29 и соседи,
	// docs/audit/09-mutation-sweep.md). Отказ при этом молчаливый — клиент
	// берёт умолчание franz-go и работает, а написанное в конфигурации просто
	// перестаёт значить что-либо: бюджет ответа брокера становится 10s вместо
	// заданного, число повторов записи — фактически бесконечным.
	//
	// Таблицей, а не цепочкой if: два десятка ветвлений подряд упирают функцию
	// в потолок цикломатической сложности, а проверка у всех одна.
	checkOptValues(t, cl, []optCheck{
		{"DialTimeout", kgo.DialTimeout, cfg.DialTimeout},
		{"ProduceRequestTimeout", kgo.ProduceRequestTimeout, cfg.Producer.AckTimeout},
		// recordRetries внутри franz-go — int64; умолчание там math.MaxInt64,
		// то есть «повторять практически вечно».
		{"RecordRetries", kgo.RecordRetries, int64(cfg.Producer.MaxRetries)},
		{"ProducerLinger", kgo.ProducerLinger, cfg.Producer.Linger},
		{"ProducerBatchMaxBytes", kgo.ProducerBatchMaxBytes, cfg.Producer.BatchBytes},
	})

	// Backoff отдаётся функцией, поэтому сравнивается её результат: сама
	// constantBackoff проверена отдельно (TestConstantBackoffDoesNotGrow), а
	// здесь проверяется, что до клиента доехала именно она, а не умолчание
	// franz-go с экспоненциальным ростом и джиттером.
	backoff, ok := cl.OptValue(kgo.RetryBackoffFn).(func(int) time.Duration)
	if !ok {
		t.Fatalf("RetryBackoffFn = %#v, ожидалась func(int) time.Duration", cl.OptValue(kgo.RetryBackoffFn))
	}

	if got := backoff(7); got != cfg.Producer.RetryBackoff {
		t.Errorf("RetryBackoffFn(7) = %v, want %v", got, cfg.Producer.RetryBackoff)
	}
}

// optCheck — одна сверка применённой опции клиента с настройкой конфигурации.
type optCheck struct {
	name string
	// opt — сама функция-опция kgo: OptValue ищет значение по её имени.
	opt any
	// want — значение из конфигурации, приведённое к типу, которым его хранит
	// franz-go.
	want any
}

// checkOptValues сверяет применённые опции клиента с ожидаемыми значениями.
func checkOptValues(t *testing.T, cl *kgo.Client, checks []optCheck) {
	t.Helper()

	for _, c := range checks {
		if got := cl.OptValue(c.opt); got != c.want {
			t.Errorf("%s = %#v, want %#v", c.name, got, c.want)
		}
	}
}

func TestConsumerOptsBuildValidClient(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t)

	// Четыре настройки тестовой конфигурации совпадают с умолчаниями franz-go,
	// а на совпадении ассерт не значит ничего: выброси опцию из сборки — клиент
	// вернёт ровно то же значение, и тест останется зелёным (так и выжила
	// мутация O28, docs/audit/09-mutation-sweep.md). Поэтому здесь они
	// переопределяются на заведомо другие.
	cfg.Consumer.MinBytes = 3                  // franz-go: 1
	cfg.Consumer.MaxPartitionBytes = 512 << 10 // franz-go: 1 MiB
	cfg.Consumer.InitialOffset = OffsetLatest  // franz-go: с начала лога
	cfg.Consumer.IsolationLevel = IsolationReadCommitted

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

	// Скалярные настройки выборки и членства в группе. Все они переживали
	// выбрасывание из consumerOpts зелёным набором (O28 и соседи,
	// docs/audit/09-mutation-sweep.md): клиент подставляет умолчание franz-go и
	// работает, а конфигурация перестаёт значить написанное — вместо 50 мс
	// ожидания батча получается 5 с, вместо секундного heartbeat трёхсекундный,
	// вместо заданного потолка партиции мегабайт.
	checkOptValues(t, cl, []optCheck{
		{"FetchMinBytes", kgo.FetchMinBytes, cfg.Consumer.MinBytes},
		{"FetchMaxBytes", kgo.FetchMaxBytes, cfg.Consumer.MaxBytes},
		{"FetchMaxPartitionBytes", kgo.FetchMaxPartitionBytes, cfg.Consumer.MaxPartitionBytes},
		{"FetchMaxWait", kgo.FetchMaxWait, cfg.Consumer.MaxWait},
		{"HeartbeatInterval", kgo.HeartbeatInterval, cfg.Consumer.HeartbeatInterval},
		{"RebalanceTimeout", kgo.RebalanceTimeout, cfg.Consumer.RebalanceTimeout},
		{"AutoCommitInterval", kgo.AutoCommitInterval, cfg.Consumer.CommitInterval},
		// Маппинг имён проверяют TestInitialOffsetMapping и
		// TestIsolationLevelMapping; здесь проверяется, что результат маппинга
		// доехал до клиента. Уровень изоляции franz-go отдаёт сырым int8, а не
		// kgo.IsolationLevel: 1 — read_committed, 0 — read_uncommitted.
		{"ConsumeResetOffset", kgo.ConsumeResetOffset, kgo.NewOffset().AtEnd()},
		{"FetchIsolationLevel", kgo.FetchIsolationLevel, int8(1)},
	})
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
	//
	// Сравнение с nil здесь недостижимо, и на этом тест однажды уже прогорел:
	// OptValue возвращает any, в который упакован []sasl.Mechanism, поэтому
	// интерфейс не-nil даже у клиента без единой опции SASL — там лежит
	// типизированный nil-слайс. Судится длина слайса и имя механизма; за
	// «доехали ли учётные данные» отвечает круг против брокера в
	// opts_sasl_tls_test.go, потому что наружу sasl.Mechanism их не отдаёт.
	mechs, ok := optsClient(t, opts).OptValue(kgo.SASL).([]sasl.Mechanism)
	if !ok || len(mechs) != 1 {
		t.Fatalf("SASL-механизм не попал в опции клиента: %#v", mechs)
	}

	// Литерал, а не SASLMechanismScramSHA512: имя механизма едет к брокеру в
	// протокольном кадре, и сверка константы с ней же ничего бы не сказала.
	const wantMech = "SCRAM-SHA-512"

	if got := mechs[0].Name(); got != wantMech {
		t.Errorf("Name() = %q, want %q", got, wantMech)
	}

	cfg.SASL.Mechanism = "GSSAPI"

	if _, err := cfg.producerOpts(testLogger(t)); err == nil {
		t.Error("producerOpts принял неизвестный механизм SASL")
	}
}

// TestCommonOptsWarnsOnUnencryptedSASL — аутентификация поверх нешифрованного
// соединения не проходит молча.
//
// Находка С1 (docs/audit/05-security.md) начиналась с асимметрии:
// InsecureSkipVerify получал WARN, а «пароль открытым текстом» — ничего.
// PLAIN без TLS теперь отвергает валидация, но два случая до этого слоя всё
// равно доходят: SCRAM (законен без опт-аута) и PLAIN с явным
// sasl.allow_plaintext. Оба обязаны оставить след в логе — при разборе
// «почему брокер видит нас неаутентифицированными» или «откуда утёк пароль»
// эта строка и есть ответ.
//
// Предупреждение привязано к результату tlsConfig, а не к полям конфигурации:
// иначе готовый Config.TLSConfig считался бы отсутствием шифрования.
func TestCommonOptsWarnsOnUnencryptedSASL(t *testing.T) {
	t.Parallel()

	const canary = "opts-secret-canary"

	tests := []struct {
		name     string
		mutate   func(*Config)
		wantWarn string
	}{
		{
			name:     "SCRAM без TLS",
			mutate:   func(c *Config) { c.SASL.Mechanism = SASLMechanismScramSHA512 },
			wantWarn: "SASL authentication is used over an unencrypted connection",
		},
		{
			name: "PLAIN с опт-аутом без TLS",
			mutate: func(c *Config) {
				c.SASL.Mechanism = SASLMechanismPlain
				c.SASL.AllowPlaintext = true
			},
			wantWarn: "the password is sent to the broker in cleartext",
		},
		{
			name: "SCRAM поверх секции TLS",
			mutate: func(c *Config) {
				c.SASL.Mechanism = SASLMechanismScramSHA512
				c.TLS = TLS{Enabled: true}
			},
		},
		{
			// Готовый TLSConfig — тот же зашифрованный транспорт, просто собран
			// не библиотекой. Предупреждение здесь означало бы, что проверка
			// смотрит на поля, а не на соединение.
			name: "SCRAM поверх готового TLSConfig",
			mutate: func(c *Config) {
				c.SASL.Mechanism = SASLMechanismScramSHA512
				c.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS13}
			},
		},
		{
			name:   "SASL выключен",
			mutate: func(*Config) {},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer

			cfg := testConfig(t)
			cfg.SASL = SASL{Username: "u", Password: canary}
			tt.mutate(&cfg)

			logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
			if _, err := cfg.producerOpts(logger); err != nil {
				t.Fatalf("producerOpts: %v", err)
			}

			got := buf.String()

			// Пароль в предупреждении о пароле — отдельный класс курьёза,
			// поэтому проверяется всегда, а не только в ветке с ожидаемым WARN.
			if strings.Contains(got, canary) {
				t.Fatalf("пароль попал в предупреждение:\n%s", got)
			}

			if tt.wantWarn == "" {
				if strings.Contains(got, "unencrypted") || strings.Contains(got, "cleartext") {
					t.Errorf("предупреждение о плейнтексте выдано на зашифрованном соединении:\n%s", got)
				}

				return
			}

			if !strings.Contains(got, tt.wantWarn) {
				t.Errorf("в логе нет предупреждения %q:\n%s", tt.wantWarn, got)
			}

			if !strings.Contains(got, "level=WARN") {
				t.Errorf("предупреждение записано не на уровне WARN:\n%s", got)
			}
		})
	}
}

// TestCommonOptsWarnsOnInsecureTLS — отключённая проверка сертификата не
// проходит молча.
//
// Предупреждение — единственный след этого решения в работающем процессе:
// ошибкой оно не делается (отладочный сценарий законный), в метрики не идёт, а
// сама конфигурация обычно приезжает из локального стенда и уезжает в прод
// незамеченной. Проверяются оба входа в tlsConfig — секция TLS и готовый
// Config.TLSConfig, — потому что предупреждение в них выдаётся разными
// ветками, и потерять его можно в любой поодиночке.
func TestCommonOptsWarnsOnInsecureTLS(t *testing.T) {
	t.Parallel()

	const wantWarn = "TLS certificate verification is disabled (InsecureSkipVerify)"

	tests := []struct {
		name     string
		mutate   func(*Config)
		wantWarn bool
	}{
		{
			name:     "секция TLS с insecure_skip_verify",
			mutate:   func(c *Config) { c.TLS = TLS{Enabled: true, InsecureSkipVerify: true} },
			wantWarn: true,
		},
		{
			name: "готовый TLSConfig с InsecureSkipVerify",
			mutate: func(c *Config) {
				c.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS13, InsecureSkipVerify: true} //nolint:gosec // ровно то, о чём тест
			},
			wantWarn: true,
		},
		{
			name:   "секция TLS с проверкой",
			mutate: func(c *Config) { c.TLS = TLS{Enabled: true} },
		},
		{
			name:   "готовый TLSConfig с проверкой",
			mutate: func(c *Config) { c.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS13} },
		},
		{
			name:   "TLS выключен",
			mutate: func(*Config) {},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer

			cfg := testConfig(t)
			tt.mutate(&cfg)

			logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
			if _, err := cfg.producerOpts(logger); err != nil {
				t.Fatalf("producerOpts: %v", err)
			}

			got := buf.String()

			if !tt.wantWarn {
				if strings.Contains(got, wantWarn) {
					t.Errorf("предупреждение выдано при включённой проверке сертификата:\n%s", got)
				}

				return
			}

			if !strings.Contains(got, wantWarn) {
				t.Errorf("в логе нет предупреждения %q:\n%s", wantWarn, got)
			}

			if !strings.Contains(got, "level=WARN") {
				t.Errorf("предупреждение записано не на уровне WARN:\n%s", got)
			}
		})
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
