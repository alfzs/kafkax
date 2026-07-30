package kafkax

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
)

// Аутентификация и шифрование против настоящего брокера.
//
// Всё, что проверяется здесь, нельзя проверить по набору kgo.Opt. sasl.Mechanism
// наружу не отдаёт ни имени пользователя, ни пароля — «механизм построен» и
// «учётные данные доехали» с точки зрения опций неразличимы. *tls.Config тем
// более не говорит, примет ли его вторая сторона. Судья поэтому брокер: kfake
// умеет и SASL (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512), и TLS-слушатель, так что
// весь круг идёт в памяти процесса и Docker не требует.
//
// Класс дефектов, ради которого набор существует: библиотека молча идёт к
// брокеру неаутентифицированной или незашифрованной. Пропажа kgo.SASL(mech),
// потерянный пароль, неприменённый RootCAs — на опциях всё это выглядит
// исправным.

const (
	saslTestUser = "kafkax"
	saslTestPass = "kafkax-password"
)

// newTestCluster поднимает kfake с произвольными добавочными опциями.
//
// Пороги сессии те же, что у newFakeCluster: умолчания Kafka сделали бы каждый
// круг с группой шестисекундным.
func newTestCluster(t *testing.T, extra ...kfake.Opt) []string {
	t.Helper()

	opts := append([]kfake.Opt{
		kfake.NumBrokers(1),
		kfake.SeedTopics(1, testTopic),
		kfake.GroupMinSessionTimeout(100 * time.Millisecond),
		kfake.GroupMaxSessionTimeout(time.Minute),
	}, extra...)

	cluster, err := kfake.NewCluster(opts...)
	if err != nil {
		t.Fatalf("kfake.NewCluster: %v", err)
	}

	t.Cleanup(cluster.Close)

	return cluster.ListenAddrs()
}

// newAuthCluster поднимает kfake, требующий аутентификации.
func newAuthCluster(t *testing.T, extra ...kfake.Opt) []string {
	t.Helper()

	return newTestCluster(t, append(extra, kfake.EnableSASL())...)
}

// authConfig — конфигурация с верными учётными данными для механизма mech.
func authConfig(t *testing.T, brokers []string, mech string) Config {
	t.Helper()

	cfg := testConfig(t, brokers...)
	cfg.SASL = SASL{
		Mechanism: mech,
		Username:  saslTestUser,
		Password:  saslTestPass,
		// PLAIN без TLS не проходит валидацию без явного опт-аута; тесты
		// шифрованного транспорта ниже задают его сами.
		AllowPlaintext: true,
	}

	return cfg
}

// authRoundTrip прогоняет полный круг «отправил — получил».
//
// Круг, а не Ping: аутентификация проверяется на каждом соединении отдельно, и
// консьюмер в группе открывает их больше, чем продюсер, — координатор,
// heartbeat, коммит оффсетов. Отказ на любом из них означает, что учётные
// данные доехали не везде.
func authRoundTrip(t *testing.T, cfg Config) {
	t.Helper()

	const payload = "authenticated"

	p := mustProducer(t, cfg)
	if err := p.SendMessage(t.Context(), PublishRequest{
		Topic: testTopic,
		Key:   []byte("k"),
		Value: []byte(payload),
	}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	h := &mockHandler{}
	c := mustConsumer(t, cfg)
	mustAddHandler(t, c, testTopic, h)

	if err := c.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	waitFor(t, 15*time.Second, "сообщение доехало до обработчика", func() bool {
		return h.callCount() == 1
	})

	if got := string(h.messages()[0].Value); got != payload {
		t.Fatalf("value = %q, want %q", got, payload)
	}

	if err := c.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// wantBrokerRejects требует отказа на отправке.
//
// Проверяется только факт отказа, без разбора кода ошибки: franz-go отдаёт
// наружу то, что успело случиться первым — отказ рукопожатия, обрыв
// соединения или истёкший RecordDeliveryTimeout. Существенно здесь ровно
// одно: сообщение до брокера не дошло.
func wantBrokerRejects(t *testing.T, cfg Config) {
	t.Helper()

	p := mustProducer(t, cfg)

	err := p.SendMessage(t.Context(), PublishRequest{
		Topic: testTopic,
		Key:   []byte("k"),
		Value: []byte("must not be accepted"),
	})
	if err == nil {
		t.Fatal("брокер принял сообщение от клиента, который не должен был пройти")
	}

	t.Logf("отказ (ожидаемый): %v", err)
}

// TestSASLRoundTripAgainstBroker — механизмы SASL действительно выполняют
// рукопожатие.
//
// До этого теста проверялось только имя механизма (mech.Name()), то есть
// challenge-response SCRAM не выполнялся ни разу нигде: интеграционный набор
// гоняет против брокера только PLAIN. «Имя принято валидацией» и «механизм
// работает» — разные утверждения, и различает их только брокер.
func TestSASLRoundTripAgainstBroker(t *testing.T) {
	t.Parallel()

	for _, mech := range []string{
		SASLMechanismPlain,
		SASLMechanismScramSHA256,
		SASLMechanismScramSHA512,
	} {
		t.Run(mech, func(t *testing.T) {
			t.Parallel()

			brokers := newAuthCluster(t, kfake.Superuser(mech, saslTestUser, saslTestPass))

			authRoundTrip(t, authConfig(t, brokers, mech))
		})
	}
}

// TestSASLRejectedByBroker — брокер отвергает всё, что не является верной парой
// «механизм + учётные данные».
//
// Первый подтест — судья для kgo.SASL(mech): он доказывает, что kfake с
// EnableSASL() действительно не пускает неаутентифицированного клиента, и
// значит зелёный TestSASLRoundTripAgainstBroker выше означает именно
// «опция доехала». Остальные подтесты закрывают содержимое механизма: пароль
// и имя пользователя наружу не видны, и подменить их незаметно можно только
// здесь.
func TestSASLRejectedByBroker(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{
			name:   "аутентификации нет вовсе",
			mutate: func(c *Config) { c.SASL = SASL{} },
		},
		{
			name:   "неверный пароль",
			mutate: func(c *Config) { c.SASL.Password = saslTestPass + "-wrong" },
		},
		{
			name:   "неизвестный пользователь",
			mutate: func(c *Config) { c.SASL.Username = saslTestUser + "-wrong" },
		},
		{
			// Брокер знает пользователя только по SCRAM-SHA-512; клиент,
			// назвавший другой механизм, обязан получить отказ, а не тихо
			// договориться о чём-нибудь ещё.
			name:   "чужой механизм",
			mutate: func(c *Config) { c.SASL.Mechanism = SASLMechanismScramSHA256 },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			brokers := newAuthCluster(t, kfake.Superuser(SASLMechanismScramSHA512, saslTestUser, saslTestPass))

			good := authConfig(t, brokers, SASLMechanismScramSHA512)

			bad := good
			tt.mutate(&bad)

			wantBrokerRejects(t, bad)

			// Круг верными данными по тому же кластеру. Без него отказ выше
			// доказывал бы только «сообщение не доехало» — тот же исход дал бы
			// упавший брокер или опечатка в адресе. И заодно: обработчик
			// получает ровно то, что отправлено вторым, значит отвергнутая
			// запись до топика действительно не добралась.
			authRoundTrip(t, good)
		})
	}
}

// TestTLSVerificationAgainstBroker — проверка сертификата брокера настоящая.
//
// До этого теста TLS-путь проверялся только структурно: «в *tls.Config лежит
// непустой RootCAs», «ServerName переписан», «InsecureSkipVerify=true».
// Соответствует ли это тому, что делает рукопожатие, из структуры не видно —
// потерянный RootCAs выглядит там ровно так же, как применённый.
//
// MinVersion в этом наборе намеренно отсутствует, и проверено, что иначе быть
// не может: у клиента crypto/tls нижняя граница по умолчанию и так TLS 1.2
// (измерено на go1.26 против слушателя с MaxVersion=TLS1.1 — рукопожатие
// отвергается одинаково и с явным MinVersion, и без него). Поведенческого
// теста, различающего эти два случая, не существует; строка в tls.Config
// защищена только структурным ассертом в TestTLSConfigFromSection и остаётся
// страховкой на случай смены умолчания Go.
func TestTLSVerificationAgainstBroker(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		section func(testPKI) TLS
		wantOK  bool
	}{
		{
			name:    "корень из конфигурации",
			section: func(p testPKI) TLS { return TLS{Enabled: true, CACertPath: p.caPath} },
			wantOK:  true,
		},
		{
			// Сертификат брокера подписан корнем, которого нет в системном
			// хранилище: без ca_cert_path клиенту довериться нечем.
			name:    "корень неизвестен",
			section: func(testPKI) TLS { return TLS{Enabled: true} },
		},
		{
			// Единственное, ради чего InsecureSkipVerify существует. Если
			// значение перестанет доезжать до рукопожатия, отладочный сценарий
			// сломается молча — и молча же починится «добавлением CA».
			name:    "insecure_skip_verify пропускает недоверенный корень",
			section: func(testPKI) TLS { return TLS{Enabled: true, InsecureSkipVerify: true} },
			wantOK:  true,
		},
		{
			// Имя из SAN сертификата. Клиент дозванивается на 127.0.0.1, так
			// что успех здесь означает именно то, что ServerName доехал и был
			// использован вместо адреса.
			name: "имя сервера из сертификата",
			section: func(p testPKI) TLS {
				return TLS{Enabled: true, CACertPath: p.caPath, ServerName: "localhost"}
			},
			wantOK: true,
		},
		{
			name: "чужое имя сервера",
			section: func(p testPKI) TLS {
				return TLS{Enabled: true, CACertPath: p.caPath, ServerName: "not-the-broker.example"}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pki := newTestPKI(t)
			brokers := newTestCluster(t, kfake.TLS(pki.brokerTLS(false)))

			cfg := testConfig(t, brokers...)
			cfg.TLS = tt.section(pki)

			if tt.wantOK {
				authRoundTrip(t, cfg)

				return
			}

			wantBrokerRejects(t, cfg)
		})
	}
}

// TestMTLSAgainstBroker — клиентская пара доезжает до рукопожатия.
//
// Раньше про неё было известно только то, что tls.LoadX509KeyPair положил её в
// Certificates. Брокер, требующий клиентский сертификат, в интеграции выключен
// (KAFKA_SSL_CLIENT_AUTH: "none"), так что вторая сторона этой пары не
// проверялась нигде.
func TestMTLSAgainstBroker(t *testing.T) {
	t.Parallel()

	t.Run("без клиентской пары — отказ", func(t *testing.T) {
		t.Parallel()

		pki := newTestPKI(t)
		brokers := newTestCluster(t, kfake.TLS(pki.brokerTLS(true)))

		cfg := testConfig(t, brokers...)
		cfg.TLS = TLS{Enabled: true, CACertPath: pki.caPath}

		wantBrokerRejects(t, cfg)
	})

	t.Run("с клиентской парой — круг проходит", func(t *testing.T) {
		t.Parallel()

		pki := newTestPKI(t)
		brokers := newTestCluster(t, kfake.TLS(pki.brokerTLS(true)))

		cfg := testConfig(t, brokers...)
		cfg.TLS = TLS{
			Enabled:        true,
			CACertPath:     pki.caPath,
			ClientCertPath: pki.clientCertPath,
			ClientKeyPath:  pki.clientKeyPath,
		}

		authRoundTrip(t, cfg)
	})
}

// TestSASLOverTLSRoundTripAgainstBroker — обе защиты вместе.
//
// Ровно та конфигурация, которую библиотека предлагает для прода: SCRAM поверх
// проверенного TLS. Проверяется как целое, потому что порядок здесь имеет
// значение — SASL-рукопожатие идёт уже внутри установленного TLS-соединения, и
// ошибка в сборке опций способна разорвать эту связку, оставив обе половины
// формально настроенными.
func TestSASLOverTLSRoundTripAgainstBroker(t *testing.T) {
	t.Parallel()

	pki := newTestPKI(t)
	brokers := newAuthCluster(t,
		kfake.TLS(pki.brokerTLS(false)),
		kfake.Superuser(SASLMechanismScramSHA512, saslTestUser, saslTestPass),
	)

	cfg := authConfig(t, brokers, SASLMechanismScramSHA512)
	cfg.SASL.AllowPlaintext = false
	cfg.TLS = TLS{Enabled: true, CACertPath: pki.caPath}

	authRoundTrip(t, cfg)
}

// testPKI — самоподписанный корень и выпущенные им пары для брокера и клиента.
//
// Настоящая PKI нужна потому, что библиотека принимает пути к файлам, а не
// готовые *x509.Certificate: путь к CA — это ещё и чтение файла, и разбор PEM,
// и попадание RootCAs в тот самый tls.Config, с которым клиент пойдёт на
// рукопожатие.
type testPKI struct {
	caPath         string // PEM корня для TLS.CACertPath
	clientCertPath string
	clientKeyPath  string

	serverCert tls.Certificate
	caPool     *x509.CertPool
}

// brokerTLS — конфигурация слушателя kfake. requireClientCert включает mTLS.
func (p testPKI) brokerTLS(requireClientCert bool) *tls.Config {
	cfg := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{p.serverCert},
	}

	if requireClientCert {
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
		cfg.ClientCAs = p.caPool
	}

	return cfg
}

// newTestPKI выпускает корень, серверную пару на localhost/127.0.0.1 и
// клиентскую пару для mTLS.
func newTestPKI(t *testing.T) testPKI {
	t.Helper()

	dir := t.TempDir()

	caKey, caDER := issueCert(t, &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "kafkax test CA"},
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}, nil, nil)

	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("разбор корневого сертификата: %v", err)
	}

	// kfake слушает на 127.0.0.1 и его же объявляет в метаданных, поэтому в
	// сертификате нужен IP-SAN. Имя localhost добавлено для проверки
	// ServerName: клиент, назвавший его явно, должен пройти, а назвавший
	// чужое — нет.
	serverKey, serverDER := issueCert(t, &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "kafkax test broker"},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}, caCert, caKey)

	clientKey, clientDER := issueCert(t, &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: "kafkax test client"},
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}, caCert, caKey)

	pool := x509.NewCertPool()
	pool.AddCert(caCert)

	return testPKI{
		caPath:         writePEM(t, filepath.Join(dir, "ca.pem"), "CERTIFICATE", caDER),
		clientCertPath: writePEM(t, filepath.Join(dir, "client.pem"), "CERTIFICATE", clientDER),
		clientKeyPath:  writePEM(t, filepath.Join(dir, "client-key.pem"), "EC PRIVATE KEY", marshalECKey(t, clientKey)),
		serverCert:     tls.Certificate{Certificate: [][]byte{serverDER}, PrivateKey: serverKey},
		caPool:         pool,
	}
}

// issueCert подписывает tmpl ключом parentKey; при parent == nil сертификат
// самоподписанный.
func issueCert(t *testing.T, tmpl, parent *x509.Certificate, parentKey *ecdsa.PrivateKey) (*ecdsa.PrivateKey, []byte) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("генерация ключа: %v", err)
	}

	tmpl.NotBefore = time.Now().Add(-time.Hour)
	tmpl.NotAfter = time.Now().Add(time.Hour)

	if tmpl.KeyUsage == 0 {
		tmpl.KeyUsage = x509.KeyUsageDigitalSignature
	}

	if parent == nil {
		parent, parentKey = tmpl, key
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, parent, &key.PublicKey, parentKey)
	if err != nil {
		t.Fatalf("выпуск сертификата: %v", err)
	}

	return key, der
}

func marshalECKey(t *testing.T, key *ecdsa.PrivateKey) []byte {
	t.Helper()

	der, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("сериализация ключа: %v", err)
	}

	return der
}

// writePEM кладёт блок в файл и отдаёт путь.
func writePEM(t *testing.T, path, blockType string, der []byte) string {
	t.Helper()

	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der}), 0o600); err != nil {
		t.Fatalf("запись %s: %v", path, err)
	}

	return path
}
