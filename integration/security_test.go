package integration

// security_test.go — SASL и TLS против брокера, который их требует.
//
// Граница с соседями. Остальные сценарии набора идут против общего брокера без
// аутентификации; здесь каждый тест поднимает СВОЙ контейнер, потому что
// протокол слушателя — свойство брокера, а не соединения, и общий трогать
// нельзя: на нём висит весь остальной набор.
//
// Против kfake это не проверяется по существу. Модульные тесты пакета
// доказывают, что Config превращается в нужные kgo.Opt, — но опции проверяются
// на самих себе: подтверждение приходит от того же кода, который их и собрал.
// Здесь единственный судья — брокер: он либо пустил, либо нет.

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"

	"github.com/alfzs/kafkax/v2"
)

const (
	// saslUser/saslPassword — единственная учётная запись стенда. Пароль
	// прописан и в JAAS брокера, и в конфигурации клиента; расхождение между
	// ними и есть отрицательный сценарий.
	saslUser     = "kafkax"
	saslPassword = "kafkax-secret"

	// serverPEMPath — куда кладётся ключ и цепочка брокера. Каталог
	// /etc/kafka/secrets существует в образе и предназначен ровно для этого.
	serverPEMPath = "/etc/kafka/secrets/server.pem"

	// certHost — имя в сертификате брокера. Testcontainers пробрасывает порт на
	// хост докера, и клиент ходит либо на localhost, либо на 127.0.0.1; в
	// сертификат кладутся оба, а TLS.ServerName снимает вопрос окончательно.
	certHost = "localhost"

	// rejectBudget — Producer.MessageTimeout в отрицательных сценариях, и он
	// нарочно огромен. См. rejectWithin.
	rejectBudget = 3 * time.Minute

	// rejectWithin — потолок на отказ.
	//
	// Смысл не в скорости, а в различении двух исходов. Отказ по существу
	// («брокер не пустил», «сертификату не верю») клиент выносит сам и
	// перестаёт пытаться; исчерпание бюджета доставки выглядит для вызывающего
	// почти так же — тоже ошибка, тоже без доставки, — но означает, что запись
	// протухла в очереди, а отказ окончательным клиент так и не признал.
	// Различить их можно только по времени, поэтому бюджет доставки поднят до
	// заведомо недостижимых трёх минут, а потолок держится много ниже:
	// уложились — значит клиент остановился сам.
	//
	// Немедленным отказ при этом не является, и притворяться иначе тест не
	// должен: измеренная задержка — около шестнадцати секунд, и она не зависит
	// от Producer.MessageTimeout (проверено на 20 с, 60 с и 120 с — везде те же
	// шестнадцать). Это собственный бюджет franz-go на попытки соединиться и
	// обновить метаданные: неверный пароль и лежащий брокер для него на уровне
	// соединения одно и то же. Потолок с запасом закрывает разброс и остаётся
	// втрое ниже бюджета доставки.
	rejectWithin = 45 * time.Second
)

// TestSASLPlainRequiredByBroker — механизм PLAIN против брокера с
// SASL_PLAINTEXT.
//
// Класс дефекта: настроенный SASL, который до брокера не доезжает. Config.SASL
// превращается в kgo.SASL(...) в одном месте (opts.go), и промах там — забытый
// механизм, обрезанный пароль, перепутанные местами user и zid — против брокера
// без аутентификации не виден вообще: соединение устанавливается, набор
// зелёный, а в проде клиент не подключается ни разу. Судить о таком промахе
// может только сторона, которая проверяет предъявленное.
//
// Почему ассерты такие. Положительный сценарий — полный круг, а не одно
// подключение: SASL проходится заново на каждом соединении, а продюсер и
// консьюмер открывают свои и ходят в том числе к координатору группы. Проверять
// нужно оба пути, и круг проверяет оба разом. Отрицательный сценарий с неверным
// паролем — контроль положительного: не включись SASL на слушателе, брокер
// пустил бы кого угодно, и первая половина теста зеленела бы сама по себе.
//
// Отдельно проверяется, чем именно кончается отправка с неверным паролем:
// названной причиной и остановкой клиента, а не молчанием до исчерпания
// бюджета доставки (см. requireRejected и rejectWithin). Неверный пароль — не
// временная неполадка, повторять с ним нечего, и приложению нужно не «брокер
// недоступен», а «пароль не тот»: чинится это совсем иначе.
func TestSASLPlainRequiredByBroker(t *testing.T) {
	t.Parallel()

	addrs := secureBrokers(t, saslListenerEnv("SASL_PLAINTEXT"))

	base := configFor(t, addrs)
	base.SASL.Mechanism = kafkax.SASLMechanismPlain
	base.SASL.Username = saslUser
	base.SASL.Password = saslPassword
	// Пароль уходит по проводу открытым текстом, и пакет требует, чтобы это было
	// заявлено. Стенд — ровно тот случай, ради которого флаг и существует.
	base.SASL.AllowPlaintext = true

	topic := secureTopic(t, base, nil)

	t.Run("верные учётные данные", func(t *testing.T) {
		t.Parallel()

		cfg := base
		cfg.Consumer.Group = newGroup(t)

		requireRoundTrip(t, cfg, topic, "plain-ok")
	})

	t.Run("неверный пароль", func(t *testing.T) {
		t.Parallel()

		cfg := base
		cfg.SASL.Password = saslPassword + "-wrong"

		requireRejected(t, cfg, topic, "SASL_AUTHENTICATION_FAILED")
	})
}

// TestSASLOverTLSRequiredByBroker — тот же PLAIN, но поверх TLS, и с проверкой
// сертификата брокера по закреплённому корню.
//
// Класс дефекта: TLS, который шифрует, но не проверяет. По удавшемуся
// подключению отличить «клиент доверился именно этому корню» от «клиент принял
// бы любой сертификат» невозможно — зелёными выглядят оба исхода, а разница
// между ними и есть весь смысл TLS. Поэтому проверку доказывает не первый
// подсценарий, а второй: клиенту подсовывается чужой корень, и он обязан
// отказаться. Уехавший в *tls.Config InsecureSkipVerify или потерянный по
// дороге RootCAs ловятся только так.
//
// Почему такой стенд. Брокеру отдаётся PEM-хранилище (KIP-651): Kafka умеет
// читать ключ и цепочку прямо из PEM с версии 2.7, и это снимает необходимость
// собирать JKS через keytool внутри контейнера — вся криптография теста
// остаётся в стандартной библиотеке Go и видна в исходнике. Корень
// самоподписанный и живёт ровно один прогон: закреплять в репозитории
// сертификат со сроком годности значит однажды получить красный набор в день
// его истечения.
//
// Здесь же снимается вопрос, ради которого в пакете есть SASL.AllowPlaintext:
// с TLS этот флаг не нужен, и конфигурация проходит валидацию без него.
func TestSASLOverTLSRequiredByBroker(t *testing.T) {
	t.Parallel()

	broker := newBrokerCert(t)

	addrs := secureBrokers(t, saslListenerEnv("SASL_SSL"),
		testcontainers.WithEnv(map[string]string{
			// PEM, а не JKS: тип хранилища задаётся явно, иначе Kafka ждёт
			// JKS и падает на первом же байте файла.
			"KAFKA_SSL_KEYSTORE_TYPE":     "PEM",
			"KAFKA_SSL_KEYSTORE_LOCATION": serverPEMPath,
			// Клиентский сертификат не требуется: проверяется доверие клиента к
			// брокеру, а не mTLS.
			"KAFKA_SSL_CLIENT_AUTH": "none",
		}),
		testcontainers.WithFiles(testcontainers.ContainerFile{
			Reader:            bytes.NewReader(broker.serverPEM),
			ContainerFilePath: serverPEMPath,
			FileMode:          0o644,
		}))

	base := configFor(t, addrs)
	base.SASL.Mechanism = kafkax.SASLMechanismPlain
	base.SASL.Username = saslUser
	base.SASL.Password = saslPassword
	base.TLS.Enabled = true
	base.TLS.CACertPath = broker.caPath
	base.TLS.ServerName = certHost

	topic := secureTopic(t, base, broker.clientTLS(t))

	t.Run("сертификат брокера подписан доверенным корнем", func(t *testing.T) {
		t.Parallel()

		cfg := base
		cfg.Consumer.Group = newGroup(t)

		requireRoundTrip(t, cfg, topic, "tls-ok")
	})

	t.Run("чужой корень", func(t *testing.T) {
		t.Parallel()

		// Второй корень выпущен так же, как первый, и точно так же валиден сам
		// по себе. Отличается он ровно одним: сертификат брокера подписан не им.
		cfg := base
		cfg.TLS.CACertPath = newBrokerCert(t).caPath

		requireRejected(t, cfg, topic, "certificate signed by unknown authority")
	})
}

// requireRoundTrip проверяет, что с этой конфигурацией проходит полный круг:
// отправка, доставка обработчику.
func requireRoundTrip(t *testing.T, cfg kafkax.Config, topic, value string) {
	t.Helper()

	producer, err := kafkax.NewProducer(cfg)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}

	t.Cleanup(func() { _ = producer.Close() })

	if err := producer.SendMessage(t.Context(), kafkax.PublishRequest{
		Topic: topic,
		Value: []byte(value),
	}); err != nil {
		t.Fatalf("SendMessage(%s): %v", value, err)
	}

	received := &collector{}
	startConsumer(t, cfg, topic, received)

	await(t, "консьюмер получил отправленное", func() bool {
		return received.has(value)
	})
}

// requireRejected требует, чтобы отправка с этой конфигурацией провалилась,
// провалилась с названной причиной и не дожидаясь бюджета доставки.
//
// Причина проверяется по подстроке текста, а не по типу ошибки: формулирует её
// franz-go, обещание пакета здесь ровно одно — не проглотить и не подменить.
// Подстрока это и стережёт. Если однажды «SASL_AUTHENTICATION_FAILED»
// превратится в безликое «unable to dial», тест покраснеет — и правильно
// сделает: по такому тексту дежурный не отличит неверный пароль от упавшего
// брокера, а чинятся они по-разному.
//
// Отдельно стоит знать, чего этот ассерт НЕ утверждает. Он не утверждает, что
// причина теряется при коротком Producer.MessageTimeout: там ошибка приходит
// ровно по таймауту, но текст остаётся содержательным — «timeout waiting for
// delivery ack: ... last err: SASL_AUTHENTICATION_FAILED». Проверяется здесь
// более сильное свойство: с недостижимым бюджетом клиент останавливается сам.
func requireRejected(t *testing.T, cfg kafkax.Config, topic, wantReason string) {
	t.Helper()

	// Бюджет доставки поднимается здесь, а не в сценарии: он не настройка
	// случая, а условие самого ассерта — см. rejectWithin.
	cfg.Producer.MessageTimeout = rejectBudget

	producer, err := kafkax.NewProducer(cfg)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}

	t.Cleanup(func() { _ = producer.Close() })

	started := time.Now()

	err = producer.SendMessage(t.Context(), kafkax.PublishRequest{
		Topic: topic,
		Value: []byte("must-not-be-delivered"),
	})
	elapsed := time.Since(started)

	if err == nil {
		t.Fatalf("отправка прошла за %s: брокер принял соединение, которое обязан был отвергнуть", elapsed)
	}

	t.Logf("отказ за %s: %v", elapsed, err)

	if elapsed > rejectWithin {
		t.Fatalf("отказ занял %s при потолке %s и бюджете доставки %s: клиент не остановился "+
			"сам, а домолчал до исчерпания бюджета; ошибка: %v",
			elapsed, rejectWithin, rejectBudget, err)
	}

	if !strings.Contains(err.Error(), wantReason) {
		t.Fatalf("в ошибке нет %q, по ней причину не установить: %v", wantReason, err)
	}
}

// saslListenerEnv переводит внешний слушатель брокера на заданный протокол и
// заводит на нём учётную запись PLAIN.
//
// Имя слушателя остаётся PLAINTEXT: его задаёт стартовый скрипт модуля
// testcontainers, и оно же попадает в advertised.listeners. Совпадение имени с
// названием протокола здесь случайное — протокол определяется картой
// listener.security.protocol.map, и именно её тест и подменяет.
//
// Межброкерный (BROKER) и контроллерный (CONTROLLER) слушатели остаются
// незашифрованными: они живут внутри контейнера, а поднять на них SASL значило
// бы добавить к сценарию аутентификацию брокера самому себе — к проверяемому
// это отношения не имеет, зато ломается заметно веселее.
func saslListenerEnv(protocol string) testcontainers.CustomizeRequestOption {
	jaas := `org.apache.kafka.common.security.plain.PlainLoginModule required ` +
		`username="` + saslUser + `" password="` + saslPassword + `" ` +
		`user_` + saslUser + `="` + saslPassword + `";`

	return testcontainers.WithEnv(map[string]string{
		"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "BROKER:PLAINTEXT,PLAINTEXT:" + protocol + ",CONTROLLER:PLAINTEXT",
		// Встроенный REST-прокси образа ходит на межброкерный слушатель:
		// оставленный на внешнем, он молотил бы в лог отказами аутентификации.
		"KAFKA_REST_BOOTSTRAP_SERVERS":                          "BROKER://0.0.0.0:9092",
		"KAFKA_SASL_ENABLED_MECHANISMS":                         kafkax.SASLMechanismPlain,
		"KAFKA_LISTENER_NAME_PLAINTEXT_SASL_ENABLED_MECHANISMS": kafkax.SASLMechanismPlain,
		"KAFKA_LISTENER_NAME_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG":  jaas,
	})
}

// secureBrokers поднимает отдельный брокер с собственными настройками и гасит
// его по окончании теста.
func secureBrokers(t *testing.T, opts ...testcontainers.ContainerCustomizer) []string {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), startTimeout)
	defer cancel()

	all := append([]testcontainers.ContainerCustomizer{
		tckafka.WithClusterID("kafkax-" + sanitize(t.Name())),
	}, opts...)

	container, err := tckafka.Run(ctx, kafkaImage, all...)

	// Cleanup регистрируется до разбора ошибки: не дождавшийся готовности
	// контейнер всё равно создан и всё равно занимает порт.
	t.Cleanup(func() {
		if container == nil {
			return
		}

		stop, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()

		if err := container.Terminate(stop); err != nil {
			t.Logf("не удалось погасить контейнер: %v", err)
		}
	})

	if err != nil {
		logBrokerOutput(t, ctx, container)
		brokerUnavailable(t, err)
	}

	addrs, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("адреса брокера: %v", err)
	}

	return addrs
}

// logBrokerOutput выкладывает начало лога брокера в лог теста.
//
// Без него разбирать нечего: подъём контейнера падает по таймауту ожидания
// строки о готовности, и одинаково выглядят отсутствие Docker, невзлетевший
// образ и опечатка в JAAS. Начало, а не хвост: брокер с испорченной
// конфигурацией не доходит до цикла работы, и причина стоит в первых
// килобайтах.
func logBrokerOutput(t *testing.T, ctx context.Context, container *tckafka.KafkaContainer) { //nolint:revive // ctx после t — цена того, что первым аргументом хелпера теста идёт *testing.T
	t.Helper()

	if container == nil {
		return
	}

	logs, err := container.Logs(ctx)
	if err != nil {
		t.Logf("лог брокера недоступен: %v", err)

		return
	}

	head := make([]byte, 16<<10)

	n, _ := logs.Read(head)
	if n > 0 {
		t.Logf("лог брокера:\n%s", head[:n])
	}
}

// secureTopic создаёт тему на защищённом брокере.
//
// Свой клиент, а не kafkax: тема — это подготовка стенда, и собирать её тем же
// кодом, который проверяется, значит получить зелёный тест ровно в том случае,
// когда пакет ошибается одинаково в обе стороны.
func secureTopic(t *testing.T, cfg kafkax.Config, tlsCfg *tls.Config) string {
	t.Helper()

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.SASL(plain.Auth{User: cfg.SASL.Username, Pass: cfg.SASL.Password}.AsMechanism()),
	}

	if tlsCfg != nil {
		opts = append(opts, kgo.DialTLSConfig(tlsCfg))
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatalf("административный клиент: %v", err)
	}

	t.Cleanup(client.Close)

	topic := "it-" + sanitize(t.Name())

	resp, err := kadm.NewClient(client).CreateTopics(t.Context(), 1, 1, nil, topic)
	if err != nil {
		t.Fatalf("создание темы %s: %v", topic, err)
	}

	for _, created := range resp {
		if created.Err != nil {
			t.Fatalf("создание темы %s: %v", topic, created.Err)
		}
	}

	return topic
}

// brokerCert — самоподписанный корень и подписанный им сертификат брокера.
type brokerCert struct {
	// caPEM — корень в том виде, в каком его читает Config.TLS.CACertPath.
	caPEM []byte
	// caPath — он же, положенный в файл: пакет принимает путь, не байты.
	caPath string
	// serverPEM — ключ брокера и цепочка одним файлом, как того требует
	// ssl.keystore.type=PEM.
	serverPEM []byte
}

// clientTLS собирает *tls.Config, доверяющий этому корню, — для клиентов теста,
// которым конфигурация пакета не подходит (административный kgo).
func (b brokerCert) clientTLS(t *testing.T) *tls.Config {
	t.Helper()

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(b.caPEM) {
		t.Fatal("корень не разобран")
	}

	return &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool, ServerName: certHost}
}

// newBrokerCert выпускает корень и серверный сертификат на один прогон.
//
// RSA, а не ECDSA: Kafka разбирает PEM-хранилище перебором алгоритмов ключа, и
// проверять на стенде ещё и этот перебор незачем — сценарий не про него.
func newBrokerCert(t *testing.T) brokerCert {
	t.Helper()

	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("ключ корня: %v", err)
	}

	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "kafkax integration CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("сертификат корня: %v", err)
	}

	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("разбор сертификата корня: %v", err)
	}

	serverKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("ключ брокера: %v", err)
	}

	serverTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: certHost},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{certHost},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
	}

	serverDER, err := x509.CreateCertificate(rand.Reader, serverTemplate, caCert, &serverKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("сертификат брокера: %v", err)
	}

	serverKeyDER, err := x509.MarshalPKCS8PrivateKey(serverKey)
	if err != nil {
		t.Fatalf("сериализация ключа брокера: %v", err)
	}

	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})

	// Порядок в хранилище: сначала ключ, затем цепочка от листа к корню. Kafka
	// принимает и другой, но этот — общепринятый, и файл читается глазами.
	serverPEM := bytes.Join([][]byte{
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: serverKeyDER}),
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverDER}),
		caPEM,
	}, nil)

	caPath := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caPath, caPEM, 0o600); err != nil {
		t.Fatalf("запись корня: %v", err)
	}

	return brokerCert{caPEM: caPEM, caPath: caPath, serverPEM: serverPEM}
}
