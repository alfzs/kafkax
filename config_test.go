package kafkax

import (
	"crypto/tls"
	"errors"
	"reflect"
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

// cfgLabel — то, как поле называется в тексте ошибки валидации: Go-путь плюс
// имя переменной окружения.
//
// Собирается тем же кодом, что и сообщение, намеренно. Здесь проверяется
// связка «поле — претензия»: что о неположительном MaxBytes ругаются именно на
// Consumer.MaxBytes, а не на соседа. Правильность самого имени переменной —
// отдельный вопрос, и его сторожит TestEnvNamesMatchStructTags, сверяя вывод с
// тегами структуры; дублировать её здесь значило бы переписывать сорок строк
// на каждое переименование поля.
func cfgLabel(goPath string) string {
	return cfgField(goPath).String()
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
	cfgWantNoErr(t, cfg.validateProducer(behavior{}))
	cfgWantNoErr(t, cfg.validateConsumer(behavior{}))
}

func TestConfigValidateCommonFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{
			name:   "nil Brokers",
			mutate: func(c *Config) { c.Brokers = nil },
			want:   cfgLabel("Brokers") + " must not be empty",
		},
		{
			name:   "пустой список Brokers",
			mutate: func(c *Config) { c.Brokers = []string{} },
			want:   cfgLabel("Brokers") + " must not be empty",
		},
		{
			name:   "пустой ClientID",
			mutate: func(c *Config) { c.ClientID = "" },
			want:   cfgLabel("ClientID") + " must not be empty",
		},
		{
			name:   "нулевой GracefulTimeout",
			mutate: func(c *Config) { c.GracefulTimeout = 0 },
			want:   cfgLabel("GracefulTimeout") + " must be positive",
		},
		{
			name:   "отрицательный GracefulTimeout",
			mutate: func(c *Config) { c.GracefulTimeout = -time.Second },
			want:   cfgLabel("GracefulTimeout") + " must be positive",
		},
		{
			name:   "нулевой DialTimeout",
			mutate: func(c *Config) { c.DialTimeout = 0 },
			want:   cfgLabel("DialTimeout") + " must be positive",
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
			cfgWantErr(t, cfg.validateProducer(behavior{}), tt.want)
			cfgWantErr(t, cfg.validateConsumer(behavior{}), tt.want)
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

	errs := cfgUnwrapJoined(t, cfg.validateProducer(behavior{}))

	// Ровно четыре дефекта — ровно четыре ошибки. Проверяется не текст, а
	// количество: возврат по первой ошибке дал бы единицу.
	if len(errs) != 4 {
		t.Fatalf("получено %d ошибок, ожидалось 4: %v", len(errs), errs)
	}

	cfgWantErr(t, cfg.validateProducer(behavior{}),
		cfgLabel("Brokers")+" must not be empty",
		cfgLabel("ClientID")+" must not be empty",
		cfgLabel("GracefulTimeout")+" must be positive",
		cfgLabel("DialTimeout")+" must be positive")
}

func TestConfigValidateZeroConfigReportsBothSections(t *testing.T) {
	t.Parallel()

	var cfg Config

	// Пустая структура — это приложение, забывшее заполнить конфиг вообще.
	// Оно должно получить полный список претензий сразу, включая обе секции.
	cfgWantErr(t, cfg.Validate(),
		cfgLabel("Brokers")+" must not be empty",
		cfgLabel("Producer.MessageTimeout")+" must be positive",
		cfgLabel("Consumer.Group")+" must not be empty")

	if got := len(cfgUnwrapJoined(t, cfg.Validate())); got < 10 {
		t.Errorf("пустой Config дал всего %d ошибок — валидация явно поредела", got)
	}
}

func TestConfigValidateSectionsAreIndependent(t *testing.T) {
	t.Parallel()

	// Гарантия, ради которой конструкторы вызывают не Validate, а свою
	// ролевую проверку: продюсеру незачем требовать consumer.group, а
	// консьюмеру — producer.compression_type. Если проверки склеятся,
	// NewProducer начнёт отказывать на конфиге без секции Consumer.
	t.Run("дефект consumer не мешает продюсеру", func(t *testing.T) {
		t.Parallel()

		cfg := testConfig(t)
		cfg.Consumer.Group = ""
		cfg.Consumer.MaxPollRecords = 0
		cfg.Consumer.InitialOffset = "sideways"

		cfgWantNoErr(t, cfg.validateProducer(behavior{}))
		cfgWantErr(t, cfg.validateConsumer(behavior{}), cfgLabel("Consumer.Group")+" must not be empty")
		cfgWantErr(t, cfg.Validate(), cfgLabel("Consumer.Group")+" must not be empty")
	})

	t.Run("дефект producer не мешает консьюмеру", func(t *testing.T) {
		t.Parallel()

		cfg := testConfig(t)
		cfg.Producer.MessageTimeout = 0
		cfg.Producer.CompressionType = "brotli"
		cfg.Producer.MaxInflight = 0

		cfgWantNoErr(t, cfg.validateConsumer(behavior{}))
		cfgWantErr(t, cfg.validateProducer(behavior{}), cfgLabel("Producer.MessageTimeout")+" must be positive")
		cfgWantErr(t, cfg.Validate(), cfgLabel("Producer.MessageTimeout")+" must be positive")
	})
}

func TestConfigValidateSASL(t *testing.T) {
	t.Parallel()

	// TLS включён во всех случаях с PLAIN: без него срабатывает отдельная
	// проверка «пароль открытым текстом», и тест про пустой username считал бы
	// две ошибки вместо одной. Плейнтекстовая пара разобрана отдельно, в
	// TestConfigValidateSASLPlaintext.
	tests := []struct {
		name     string
		sasl     SASL
		tls      TLS
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
			tls:  TLS{Enabled: true},
		},
		{
			// Механизм из yaml приходит в произвольном регистре, а сравнение
			// идёт через ToUpper: "plain" обязан приниматься наравне с "PLAIN".
			name: "нижний регистр механизма",
			sasl: SASL{Mechanism: "plain", Username: "u", Password: "p"},
			tls:  TLS{Enabled: true},
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
			want:     []string{cfgLabel("SASL.Mechanism") + " must be one of", `got "GSSAPI"`},
		},
		{
			name:     "пустой username",
			sasl:     SASL{Mechanism: SASLMechanismPlain, Password: "p"},
			tls:      TLS{Enabled: true},
			wantErrs: 1,
			want:     []string{cfgLabel("SASL.Username") + " must be set when SASL.Mechanism is"},
		},
		{
			name:     "пустой password",
			sasl:     SASL{Mechanism: SASLMechanismPlain, Username: "u"},
			tls:      TLS{Enabled: true},
			wantErrs: 1,
			want:     []string{cfgLabel("SASL.Password") + " must be set when SASL.Mechanism is"},
		},
		{
			// Три независимых дефекта в одной секции — три ошибки: сообщение
			// «механизм неизвестен» не должно прятать отсутствие пароля.
			name:     "неизвестный механизм без учётных данных",
			sasl:     SASL{Mechanism: "kerberos"},
			wantErrs: 3,
			want:     []string{"SASL.Mechanism", "SASL.Username", "SASL.Password"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := testConfig(t)
			cfg.SASL = tt.sasl
			cfg.TLS = tt.tls

			err := cfg.validateProducer(behavior{})
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

// TestConfigValidateSASLPlaintext — PLAIN без шифрования отвергается, и
// отменить это можно только названным полем.
//
// Находка С1 (docs/audit/05-security.md): kgo.SASL добавлялся независимо от
// того, вернул ли tlsConfig nil, и опечатка в KAFKA_TLS_ENABLED отправляла
// пароль в сеть открытым текстом без единого сигнала. Проверяется вся матрица,
// потому что «TLS есть» имеет два независимых источника, а «плейнтекст
// разрешён» — одно поле, и любая из четырёх комбинаций, решённая неправильно,
// либо ломает законный сценарий, либо возвращает утечку.
func TestConfigValidateSASLPlaintext(t *testing.T) {
	t.Parallel()

	const plaintextErr = "sends the password to the broker in cleartext"

	tests := []struct {
		name    string
		mutate  func(*Config)
		opts    []Option
		wantErr bool
	}{
		{
			name:    "PLAIN без TLS",
			mutate:  func(*Config) {},
			wantErr: true,
		},
		{
			// Регистр механизма не важен нигде в пакете — не должен быть важен
			// и здесь: «plain» из yaml отправляет ровно тот же пароль.
			name:    "нижний регистр без TLS",
			mutate:  func(c *Config) { c.SASL.Mechanism = "plain" },
			wantErr: true,
		},
		{
			name:   "PLAIN с секцией TLS",
			mutate: func(c *Config) { c.TLS = TLS{Enabled: true} },
		},
		{
			// Готовый *tls.Config побеждает секцию TLS целиком (см. tlsConfig),
			// и валидация обязана судить о том же соединении, которое
			// соберётся: иначе mTLS из памяти — полностью поддерживаемый путь —
			// упирался бы в требование выставить ещё и tls.enabled. Опция
			// доезжает до валидации ровно за этим.
			name:   "PLAIN с готовым WithTLSConfig",
			mutate: func(*Config) {},
			opts:   []Option{WithTLSConfig(&tls.Config{MinVersion: tls.VersionTLS13})},
		},
		{
			name:   "PLAIN без TLS с явным опт-аутом",
			mutate: func(c *Config) { c.SASL.AllowPlaintext = true },
		},
		{
			// SCRAM пароль по проводу не передаёт, поэтому опт-аут ему не
			// нужен: на MITM-риск библиотека отвечает предупреждением при
			// создании клиента, а не отказом.
			name:   "SCRAM без TLS",
			mutate: func(c *Config) { c.SASL.Mechanism = SASLMechanismScramSHA512 },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := testConfig(t)
			// Пароль-канарейка, а не "p": текст этой ошибки читают в логе
			// старта, и попадание пароля в него было бы ровно той утечкой, от
			// которой заведена вся проверка.
			cfg.SASL = SASL{Mechanism: SASLMechanismPlain, Username: "u", Password: redactionCanary}
			tt.mutate(&cfg)

			b := testBehavior(t, tt.opts...)

			if !tt.wantErr {
				cfgWantNoErr(t, cfg.validateProducer(b))
				cfgWantNoErr(t, cfg.validateConsumer(b))

				return
			}

			// Проверка общая, а не продюсерская: пароль уходит в сеть с любой
			// стороны, и консьюмер обязан отвергать ту же конфигурацию.
			err := cfg.validateProducer(b)
			cfgWantErr(t, err, plaintextErr, "SASL.AllowPlaintext=true")
			cfgWantErr(t, cfg.validateConsumer(b), plaintextErr)

			if strings.Contains(err.Error(), redactionCanary) {
				t.Errorf("пароль попал в текст ошибки валидации:\n%v", err)
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
			// Пути заведомо несуществующие, и это не упрощение: Validate — чистая
			// функция от значения Config, она диск не трогает и трогать не должна
			// (иначе конфигурация, валидная на машине разработчика, оказывалась бы
			// невалидной в контейнере). Здесь проверяется только то, что заполненная
			// пара путей проходит валидацию. Что по этим путям действительно
			// читается сертификат с ключом — предмет config_mtls_test.go.
			name: "полный mTLS",
			tls:  TLS{Enabled: true, ClientCertPath: "/c.pem", ClientKeyPath: "/k.pem"},
		},
		{
			// Сертификат без ключа не «частично настроенный mTLS»:
			// tls.LoadX509KeyPair не вызовется вовсе, и клиент молча пойдёт
			// к брокеру без клиентского сертификата.
			name: "сертификат без ключа",
			tls:  TLS{Enabled: true, ClientCertPath: "/c.pem"},
			want: cfgLabel("TLS.ClientCertPath") + " and " + cfgLabel("TLS.ClientKeyPath") + " must be set together",
		},
		{
			name: "ключ без сертификата",
			tls:  TLS{Enabled: true, ClientKeyPath: "/k.pem"},
			want: cfgLabel("TLS.ClientCertPath") + " and " + cfgLabel("TLS.ClientKeyPath") + " must be set together",
		},
		{
			// Пара проверяется независимо от флага Enabled: полузаполненная
			// секция — почти наверняка забытый Enabled, и промолчать здесь
			// значит увести пользователя к незашифрованному соединению.
			name: "полупара при выключенном TLS",
			tls:  TLS{ClientCertPath: "/c.pem"},
			want: cfgLabel("TLS.ClientCertPath") + " and " + cfgLabel("TLS.ClientKeyPath") + " must be set together",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := testConfig(t)
			cfg.TLS = tt.tls

			err := cfg.validateProducer(behavior{})
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
		mutate func(*ProducerConfig)
		want   string
	}{
		{
			name:   "нулевой message_timeout",
			mutate: func(p *ProducerConfig) { p.MessageTimeout = 0 },
			want:   cfgLabel("Producer.MessageTimeout") + " must be positive",
		},
		{
			name:   "отрицательный flush_timeout",
			mutate: func(p *ProducerConfig) { p.FlushTimeout = -time.Second },
			want:   cfgLabel("Producer.FlushTimeout") + " must be positive",
		},
		{
			name:   "нулевой ack_timeout",
			mutate: func(p *ProducerConfig) { p.AckTimeout = 0 },
			want:   cfgLabel("Producer.AckTimeout") + " must be positive",
		},
		{
			name:   "нулевой max_buffered_records",
			mutate: func(p *ProducerConfig) { p.MaxBufferedRecords = 0 },
			want:   cfgLabel("Producer.MaxBufferedRecords") + " must be positive",
		},
		{
			// Ноль здесь отвергается конструктором клиента franz-go при
			// выключенной идемпотентности, и ошибка всплыла бы без указания
			// на поле конфигурации.
			name:   "нулевой max_inflight",
			mutate: func(p *ProducerConfig) { p.MaxInflight = 0 },
			want:   cfgLabel("Producer.MaxInflight") + " must be positive",
		},
		{
			name:   "max_retries меньше -1",
			mutate: func(p *ProducerConfig) { p.MaxRetries = -2 },
			want:   cfgLabel("Producer.MaxRetries") + " must be -1 or greater",
		},
		{
			name:   "нулевой batch_bytes",
			mutate: func(p *ProducerConfig) { p.BatchBytes = 0 },
			want:   cfgLabel("Producer.BatchBytes") + " must be positive",
		},
		{
			name:   "неизвестный compression_type",
			mutate: func(p *ProducerConfig) { p.CompressionType = "brotli" },
			want:   cfgLabel("Producer.CompressionType") + " must be one of",
		},
		{
			// Пустая строка — не «сжатие по умолчанию»: значение приходит из
			// yaml, и незаполненное поле должно быть видно, а не молча
			// превращаться в none.
			name:   "пустой compression_type",
			mutate: func(p *ProducerConfig) { p.CompressionType = "" },
			want:   cfgLabel("Producer.CompressionType") + " must be one of",
		},
		{
			name:   "нулевой max_retries допустим",
			mutate: func(p *ProducerConfig) { p.MaxRetries = 0 },
		},
		{
			// -1 — это умолчание пакета, «повторять без ограничения». Случай
			// стоит в таблице отдельной строкой, потому что до правки оно же
			// было единственным отвергаемым значением: перепутать границу
			// заново дешевле, чем кажется.
			name:   "max_retries -1 допустим",
			mutate: func(p *ProducerConfig) { p.MaxRetries = -1 },
		},
		{
			// Верхнюю границу linger franz-go проверяет, нижней у него нет:
			// минус проходит и конструктор, и первый батч. Дальше поле просто
			// перестаёт значить написанное — путь «без задержки» выбирается по
			// сравнению с нулём, а минус в него не попадает и уходит в таймер,
			// просроченный в момент создания. Настройку, которая молча делает
			// не то, отличить от работающей можно только по трафику.
			name:   "отрицательный linger",
			mutate: func(p *ProducerConfig) { p.Linger = -time.Second },
			want:   cfgLabel("Producer.Linger") + " must not be negative",
		},
		{
			// opts.go ставит kgo.MaxBufferedBytes только при значении > 0, так
			// что минус означал бы «без лимита» — то же, что ноль, но не тем
			// способом, каким это написано в godoc поля.
			name:   "отрицательный max_buffered_bytes",
			mutate: func(p *ProducerConfig) { p.MaxBufferedBytes = -1 },
			want:   cfgLabel("Producer.MaxBufferedBytes") + " must not be negative",
		},
		{
			name:   "нулевой max_buffered_bytes допустим — это «без лимита»",
			mutate: func(p *ProducerConfig) { p.MaxBufferedBytes = 0 },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := testConfig(t)
			tt.mutate(&cfg.Producer)

			err := cfg.validateProducer(behavior{})
			if tt.want == "" {
				cfgWantNoErr(t, err)

				return
			}

			cfgWantErr(t, err, tt.want)
			// Та же претензия не должна возникать у консьюмера.
			cfgWantNoErr(t, cfg.validateConsumer(behavior{}))
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
			want:        cfgLabel("Producer.RequiredAcks") + " must be -1 unless Producer.EnableIdempotence is false",
		},
		{
			name:        "acks=0 с идемпотентностью",
			acks:        0,
			idempotence: true,
			want:        cfgLabel("Producer.RequiredAcks") + " must be -1 unless Producer.EnableIdempotence is false",
		},
		{
			name:        "acks=2 вне диапазона",
			acks:        2,
			idempotence: false,
			want:        cfgLabel("Producer.RequiredAcks") + " must be -1, 0 or 1",
		},
		{
			name:        "acks=-2 вне диапазона",
			acks:        -2,
			idempotence: true,
			want:        cfgLabel("Producer.RequiredAcks") + " must be -1, 0 or 1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := testConfig(t)
			cfg.Producer.RequiredAcks = tt.acks
			cfg.Producer.EnableIdempotence = tt.idempotence

			err := cfg.validateProducer(behavior{})
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
	mutate func(*ConsumerConfig)
	want   string
}

// consumerFieldCases держит таблицу отдельно от тела теста: список растёт с
// каждым новым полем Consumer, а сама проверка не меняется — держать их одной
// функцией значит регулярно упираться в лимит длины на ровном месте.
//
// Байтовые границы вынесены во вторую функцию по той же причине: таблица
// упёрлась в лимит длины ровно на них.
func consumerFieldCases() []consumerFieldCase {
	return append(consumerTimingFieldCases(), consumerFetchSizeCases()...)
}

func consumerTimingFieldCases() []consumerFieldCase {
	return []consumerFieldCase{
		{
			name:   "пустая группа",
			mutate: func(c *ConsumerConfig) { c.Group = "" },
			want:   cfgLabel("Consumer.Group") + " must not be empty",
		},
		{
			name:   "нулевой session_timeout",
			mutate: func(c *ConsumerConfig) { c.SessionTimeout = 0 },
			want:   cfgLabel("Consumer.SessionTimeout") + " must be positive",
		},
		{
			name:   "нулевой heartbeat_interval",
			mutate: func(c *ConsumerConfig) { c.HeartbeatInterval = 0 },
			want:   cfgLabel("Consumer.HeartbeatInterval") + " must be positive",
		},
		{
			name:   "нулевой rebalance_timeout",
			mutate: func(c *ConsumerConfig) { c.RebalanceTimeout = 0 },
			want:   cfgLabel("Consumer.RebalanceTimeout") + " must be positive",
		},
		{
			name:   "нулевой commit_interval",
			mutate: func(c *ConsumerConfig) { c.CommitInterval = 0 },
			want:   cfgLabel("Consumer.CommitInterval") + " must be positive",
		},
		{
			name:   "нулевой max_wait",
			mutate: func(c *ConsumerConfig) { c.MaxWait = 0 },
			want:   cfgLabel("Consumer.MaxWait") + " must be positive",
		},
		{
			// Heartbeat не короче сессии означает, что группа развалится по
			// таймауту раньше первого удара сердца.
			name: "heartbeat_interval равен session_timeout",
			mutate: func(c *ConsumerConfig) {
				c.HeartbeatInterval = c.SessionTimeout
			},
			want: "must not exceed a third of Consumer.SessionTimeout",
		},
		{
			name: "heartbeat_interval больше session_timeout",
			mutate: func(c *ConsumerConfig) {
				c.HeartbeatInterval = c.SessionTimeout + time.Second
			},
			want: "must not exceed a third of Consumer.SessionTimeout",
		},
		{
			// Граница проверки — треть, а не сам таймаут: при интервале в
			// половину сессии один потерянный heartbeat уже стоит ребаланса.
			// Прежняя проверка такую конфигурацию пропускала, хотя godoc
			// запрещал её с самого начала.
			name: "heartbeat_interval между третью session_timeout и им самим",
			mutate: func(c *ConsumerConfig) {
				c.HeartbeatInterval = c.SessionTimeout/3 + time.Millisecond
			},
			want: "must not exceed a third of Consumer.SessionTimeout",
		},
		{
			// Отрицательное значение уронило бы make(chan, n) паникой уже
			// после того, как конструктор вернул nil-ошибку.
			name:   "отрицательный message_queue_size",
			mutate: func(c *ConsumerConfig) { c.MessageQueueSize = -1 },
			want:   cfgLabel("Consumer.MessageQueueSize") + " must be positive",
		},
		{
			// Ноль опаснее минуса именно тем, что не падает: make(chan, 0)
			// паники не даёт, он даёт небуферизованный канал — и цикл опроса
			// начинает блокироваться на каждом батче до тех пор, пока его не
			// заберёт воркер. Это молчаливая смена режима работы консьюмера, а
			// не отказ, и по логам она неотличима от медленного обработчика.
			name:   "нулевой message_queue_size",
			mutate: func(c *ConsumerConfig) { c.MessageQueueSize = 0 },
			want:   cfgLabel("Consumer.MessageQueueSize") + " must be positive",
		},
		{
			name:   "нулевой max_poll_records",
			mutate: func(c *ConsumerConfig) { c.MaxPollRecords = 0 },
			want:   cfgLabel("Consumer.MaxPollRecords") + " must be positive",
		},
		{
			name:   "неизвестный initial_offset",
			mutate: func(c *ConsumerConfig) { c.InitialOffset = "beginning" },
			want:   cfgLabel("Consumer.InitialOffset") + " must be",
		},
		{
			name:   "пустой initial_offset",
			mutate: func(c *ConsumerConfig) { c.InitialOffset = "" },
			want:   cfgLabel("Consumer.InitialOffset") + " must be",
		},
		{
			name:   "неизвестный isolation_level",
			mutate: func(c *ConsumerConfig) { c.IsolationLevel = "read_dirty" },
			want:   cfgLabel("Consumer.IsolationLevel") + " must be",
		},
		{
			name:   "handler_max_retries меньше -1",
			mutate: func(c *ConsumerConfig) { c.HandlerMaxRetries = -2 },
			want:   cfgLabel("Consumer.HandlerMaxRetries") + " must be -1",
		},
		{
			// Ретраи включены, а паузы между ними нет: партиция закрутилась бы
			// в busy loop на первом же отравленном сообщении.
			name: "ретраи без задержки",
			mutate: func(c *ConsumerConfig) {
				c.HandlerMaxRetries = 3
				c.HandlerRetryDelay = 0
			},
			want: cfgLabel("Consumer.HandlerRetryDelay") + " must be positive",
		},
		{
			// При выключенных ретраях задержка не используется, и требовать
			// её значило бы отвергать вполне рабочий конфиг.
			name: "нулевая задержка без ретраев",
			mutate: func(c *ConsumerConfig) {
				c.HandlerMaxRetries = 0
				c.HandlerRetryDelay = 0
			},
		},
		{
			name: "бесконечные ретраи с задержкой",
			mutate: func(c *ConsumerConfig) {
				c.HandlerMaxRetries = -1
				c.HandlerRetryDelay = time.Second
			},
		},
		{
			// Регистр значений из yaml не фиксирован, сравнение идёт через
			// ToLower.
			name:   "initial_offset в верхнем регистре",
			mutate: func(c *ConsumerConfig) { c.InitialOffset = "LATEST" },
		},
		{
			name:   "isolation_level в верхнем регистре",
			mutate: func(c *ConsumerConfig) { c.IsolationLevel = "READ_COMMITTED" },
		},
	}
}

// consumerFetchSizeCases — байтовые границы fetch (находка М1 в
// docs/audit/05-security.md).
//
// franz-go их не проверяет вовсе: ноль проходит и Validate, и конструктор, и
// первый опрос — отказ выглядит не как ошибка конфигурации, а как «консьюмер
// подключился и молчит».
func consumerFetchSizeCases() []consumerFieldCase {
	return []consumerFieldCase{
		{
			name:   "нулевой min_bytes",
			mutate: func(c *ConsumerConfig) { c.MinBytes = 0 },
			want:   cfgLabel("Consumer.MinBytes") + " must be positive",
		},
		{
			name:   "отрицательный min_bytes",
			mutate: func(c *ConsumerConfig) { c.MinBytes = -1 },
			want:   cfgLabel("Consumer.MinBytes") + " must be positive",
		},
		{
			name:   "нулевой max_bytes",
			mutate: func(c *ConsumerConfig) { c.MaxBytes = 0 },
			want:   cfgLabel("Consumer.MaxBytes") + " must be positive",
		},
		{
			name:   "нулевой max_partition_bytes",
			mutate: func(c *ConsumerConfig) { c.MaxPartitionBytes = 0 },
			want:   cfgLabel("Consumer.MaxPartitionBytes") + " must be positive",
		},
		{
			// franz-go эту пару принимает и молча прижимает первое ко второму
			// (kgo/config.go), то есть настройка перестаёт значить написанное, и
			// узнать об этом можно только по трафику.
			name:   "max_partition_bytes больше max_bytes",
			mutate: func(c *ConsumerConfig) { c.MaxPartitionBytes = c.MaxBytes + 1 },
			want:   "must not exceed Consumer.MaxBytes=1048576, got 1048577",
		},
		{
			// Равенство — законная граница: одна партиция вправе занять ответ
			// целиком.
			name:   "max_partition_bytes равен max_bytes",
			mutate: func(c *ConsumerConfig) { c.MaxPartitionBytes = c.MaxBytes },
		},
	}
}

// TestConfigValidateFetchSizesReportSeparately — нулевой max_bytes не порождает
// вторую претензию к паре границ.
//
// Проверка «max_partition_bytes не больше max_bytes» осмысленна только при
// положительной верхней границе, иначе одна опечатка (max_bytes=0) даёт две
// ошибки об одном и том же поле, и список претензий перестаёт быть списком
// того, что надо править.
func TestConfigValidateFetchSizesReportSeparately(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t)
	cfg.Consumer.MaxBytes = 0

	errs := cfgUnwrapJoined(t, cfg.validateConsumer(behavior{}))
	if len(errs) != 1 {
		t.Fatalf("получено %d ошибок, ожидалась одна: %v", len(errs), errs)
	}

	cfgWantErr(t, errs[0], cfgLabel("Consumer.MaxBytes")+" must be positive")
}

// TestConfigValidateSessionTimeoutReportsOnce — нулевой session_timeout не
// тянет за собой претензию к heartbeat.
//
// Тот же принцип, что и у пары байтовых границ выше: правило «heartbeat не
// больше трети сессии» осмысленно только при положительной сессии, иначе одна
// забытая строка конфигурации порождает две ошибки, из которых вторая
// указывает на поле, где всё в порядке. Список претензий читают как список
// того, что надо править, и лишняя строка в нём стоит перезапуска.
//
// Guard, который это обеспечивает, не проверялся ничем: его снятие оставляло
// набор зелёным, потому что первая — верная — претензия никуда не девается.
func TestConfigValidateSessionTimeoutReportsOnce(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t)
	cfg.Consumer.SessionTimeout = 0

	err := cfg.validateConsumer(behavior{})
	cfgWantErr(t, err, cfgLabel("Consumer.SessionTimeout")+" must be positive")

	if strings.Contains(err.Error(), "third of Consumer.SessionTimeout") {
		t.Errorf("нулевой session_timeout породил вторую претензию — к исправному heartbeat:\n%v", err)
	}

	if got := len(cfgUnwrapJoined(t, err)); got != 1 {
		t.Errorf("получено %d ошибок, ожидалась одна: %v", got, err)
	}
}

// TestValidationErrorNamesEnvVariableLiterally — суффикс «(env KAFKAX_…)»
// закреплён в тексте ошибки литералом.
//
// Остальные подтесты валидации собирают ожидаемую подстроку через cfgLabel, то
// есть через ту же функцию, которая текст и производит. Пока проверяемое и
// ожидаемое приезжают из одного вызова, они совпадут при любом его содержимом:
// cfgField.String(), переставший добавлять имя переменной окружения, оставлял
// набор зелёным целиком (находка С3, docs/audit/09-mutation-sweep.md). Литерал
// здесь не дубль cfgLabel, а единственный свидетель со стороны.
//
// Одного якоря хватает, и размножать его на все два десятка подтестов незачем.
// cfgField — одна функция на весь пакет, через неё проходит каждая претензия,
// поэтому пропажа суффикса краснеет здесь независимо от того, о каком поле шла
// речь. Правильность самих имён переменных — отдельный вопрос и отдельный тест:
// TestEnvNamesMatchStructTags сверяет envName с тегами всех полей структуры.
// Сорок литералов рядом с ним дали бы не вторую проверку, а вторую копию тегов,
// которую пришлось бы править на каждое переименование поля.
//
// Полей всё же два: вложенное показывает и точку, ставшую подчёркиванием, и
// разрыв слова внутри имени, а поле верхнего уровня — что аббревиатура не
// рассыпается, а префикс не приклеивается к несуществующей секции.
func TestValidationErrorNamesEnvVariableLiterally(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{
			name:   "вложенное поле",
			mutate: func(c *Config) { c.Producer.MessageTimeout = 0 },
			want:   "Producer.MessageTimeout (env KAFKAX_PRODUCER_MESSAGE_TIMEOUT)",
		},
		{
			name:   "поле верхнего уровня",
			mutate: func(c *Config) { c.ClientID = "" },
			want:   "ClientID (env KAFKAX_CLIENT_ID)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := testConfig(t)
			tt.mutate(&cfg)

			cfgWantErr(t, cfg.Validate(), tt.want)
		})
	}
}

func TestConfigValidateConsumerFields(t *testing.T) {
	t.Parallel()

	for _, tt := range consumerFieldCases() {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := testConfig(t)
			tt.mutate(&cfg.Consumer)

			err := cfg.validateConsumer(behavior{})
			if tt.want == "" {
				cfgWantNoErr(t, err)

				return
			}

			cfgWantErr(t, err, tt.want)
			// Секция Consumer не должна волновать продюсер.
			cfgWantNoErr(t, cfg.validateProducer(behavior{}))
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
			want:    cfgLabel("Producer.MessageTimeout") + " must be at least 1s",
			kgoWant: "record timeout",
		},
		{
			name:    "ack_timeout ниже 100ms",
			mutate:  func(c *Config) { c.Producer.AckTimeout = 50 * time.Millisecond },
			want:    cfgLabel("Producer.AckTimeout") + " must be at least 100ms",
			kgoWant: "produce timeout",
		},
		{
			name:    "linger больше минуты",
			mutate:  func(c *Config) { c.Producer.Linger = 2 * time.Minute },
			want:    cfgLabel("Producer.Linger") + " must not exceed 1m",
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
			want:     cfgLabel("Consumer.SessionTimeout") + " must be at least 100ms",
			kgoWant:  "session timeout",
			consumer: true,
		},
		{
			name:     "rebalance_timeout ниже 100ms",
			mutate:   func(c *Config) { c.Consumer.RebalanceTimeout = 50 * time.Millisecond },
			want:     cfgLabel("Consumer.RebalanceTimeout") + " must be at least 100ms",
			kgoWant:  "rebalance timeout",
			consumer: true,
		},
		{
			name:     "commit_interval ниже 100ms",
			mutate:   func(c *Config) { c.Consumer.CommitInterval = 50 * time.Millisecond },
			want:     cfgLabel("Consumer.CommitInterval") + " must be at least 100ms",
			kgoWant:  "autocommit interval",
			consumer: true,
		},
		{
			name:     "max_wait ниже 10ms",
			mutate:   func(c *Config) { c.Consumer.MaxWait = 5 * time.Millisecond },
			want:     cfgLabel("Consumer.MaxWait") + " must be at least 10ms",
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
				cfgWantErr(t, cfg.validateConsumer(behavior{}), tt.want)
				cfgWantNoErr(t, cfg.validateProducer(behavior{}))
			} else {
				cfgWantErr(t, cfg.validateProducer(behavior{}), tt.want)
				cfgWantNoErr(t, cfg.validateConsumer(behavior{}))
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
		opts, err = cfg.consumerOpts(testBehavior(t), []string{testTopic}, rebalanceCallbacks{})
	} else {
		opts, err = cfg.producerOpts(testBehavior(t))
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

	err := cfg.validateConsumer(behavior{})

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

// Имя переменной окружения в тексте ошибки выводится из Go-пути поля, а не
// берётся из тега. Тест сверяет вывод с тегами всей структуры Config.
//
// Без него envName был бы обещанием: первое же поле, названное не по правилу
// (или переименованная переменная), заставило бы ошибку валидации советовать
// несуществующий KAFKAX_*, причём молча — компилятор о расхождении строки с
// тегом не знает. Дефект такого рода стоит дороже обычной опечатки: его читают
// в момент, когда сервис не поднялся, и советом из ошибки пользуются не глядя.
func TestEnvNamesMatchStructTags(t *testing.T) {
	t.Parallel()

	walkConfigFields(t, reflect.TypeFor[Config](), "")
}

func walkConfigFields(t *testing.T, typ reflect.Type, prefix string) {
	t.Helper()

	for f := range typ.Fields() {
		path := prefix + f.Name

		// Вложенные секции (SASL, TLS, Producer, Consumer) собственного тега
		// env не имеют — имя переменной складывается из пути целиком.
		if f.Type.Kind() == reflect.Struct && f.Tag.Get("env") == "" {
			walkConfigFields(t, f.Type, path+".")

			continue
		}

		want, ok := f.Tag.Lookup("env")
		if !ok {
			continue
		}

		if got := envName(path); got != want {
			t.Errorf("envName(%q) = %q, а тег env поля — %q", path, got, want)
		}
	}
}
