package kafkax

import (
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
		// PLAIN без TLS не проходит валидацию без явного опт-аута; тесты с
		// шифрованным транспортом снимают флаг сами.
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
