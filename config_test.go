package kafkax

import (
	"strings"
	"testing"
)

// TestBuildProducerKafkaConfig проверяет прямой маппинг Config в kafka.ConfigMap,
// включая условное включение SASL-параметров по протоколу.
func TestBuildProducerKafkaConfig(t *testing.T) {
	t.Parallel()

	t.Run("PLAINTEXT не добавляет SASL-ключи", func(t *testing.T) {
		t.Parallel()

		cfg := testConfig()
		cfg.SecurityProtocol = SecurityProtocolPlaintext

		got := buildProducerKafkaConfig(cfg)

		if _, ok := got["sasl.mechanisms"]; ok {
			t.Fatal("buildProducerKafkaConfig() добавил sasl.mechanisms для PLAINTEXT")
		}

		if got["compression.type"] != cfg.Producer.CompressionType {
			t.Fatalf("compression.type=%v, ожидалось %v", got["compression.type"], cfg.Producer.CompressionType)
		}

		if got["linger.ms"] != int(cfg.Producer.Linger.Milliseconds()) {
			t.Fatalf("linger.ms=%v, ожидалось %v", got["linger.ms"], int(cfg.Producer.Linger.Milliseconds()))
		}

		if got["bootstrap.servers"] != strings.Join(cfg.Brokers, ",") {
			t.Fatalf("bootstrap.servers=%v, ожидалось %v", got["bootstrap.servers"], strings.Join(cfg.Brokers, ","))
		}
	})

	t.Run("SASL_SSL добавляет SASL-ключи", func(t *testing.T) {
		t.Parallel()

		cfg := testConfig()
		cfg.SecurityProtocol = SecurityProtocolSASLSSL
		cfg.SASL = SASL{Username: testSASLUser, Password: testSASLPassword, Mechanism: "PLAIN"}

		got := buildProducerKafkaConfig(cfg)

		if got["sasl.mechanisms"] != "PLAIN" {
			t.Fatalf("sasl.mechanisms=%v, ожидалось PLAIN", got["sasl.mechanisms"])
		}

		if got["sasl.username"] != testSASLUser || got["sasl.password"] != testSASLPassword {
			t.Fatalf("sasl.username/password=%v/%v, ожидалось user/secret", got["sasl.username"], got["sasl.password"])
		}
	})

	t.Run("незаданный TLS.IdentificationAlgorithm не отключает проверку hostname в реальном ConfigMap", func(t *testing.T) {
		t.Parallel()

		// security: buildProducerKafkaConfig — точка, где TLS.endpointIdentAlgorithm()
		// реально попадает в kafka.ConfigMap, передаваемый librdkafka. Юнит-теста
		// самого endpointIdentAlgorithm() недостаточно — регрессия могла бы
		// произойти и здесь (например, если кто-то захардкодит "none" при рефакторинге).
		cfg := testConfig()
		cfg.SecurityProtocol = SecurityProtocolSASLSSL
		cfg.SASL = SASL{Username: testSASLUser, Password: testSASLPassword, Mechanism: "PLAIN"}

		got := buildProducerKafkaConfig(cfg)

		if got["ssl.endpoint.identification.algorithm"] != tlsIdentAlgorithmHTTPS {
			t.Fatalf("ssl.endpoint.identification.algorithm=%v, ожидалось %q (secure by default)",
				got["ssl.endpoint.identification.algorithm"], tlsIdentAlgorithmHTTPS)
		}
	})
}

// TestBuildConsumerKafkaConfig проверяет прямой маппинг Config в kafka.ConfigMap
// для консьюмера, включая group.id и условное включение SASL-параметров.
func TestBuildConsumerKafkaConfig(t *testing.T) {
	t.Parallel()

	t.Run("PLAINTEXT не добавляет SASL-ключи", func(t *testing.T) {
		t.Parallel()

		cfg := testConfig()
		cfg.SecurityProtocol = SecurityProtocolPlaintext

		got := buildConsumerKafkaConfig(cfg)

		if _, ok := got["sasl.mechanisms"]; ok {
			t.Fatal("buildConsumerKafkaConfig() добавил sasl.mechanisms для PLAINTEXT")
		}

		if got["group.id"] != cfg.Consumer.Group {
			t.Fatalf("group.id=%v, ожидалось %v", got["group.id"], cfg.Consumer.Group)
		}

		if got["enable.auto.commit"] != cfg.Consumer.EnableAutoCommit {
			t.Fatalf("enable.auto.commit=%v, ожидалось %v", got["enable.auto.commit"], cfg.Consumer.EnableAutoCommit)
		}

		if got["auto.offset.reset"] != cfg.Consumer.InitialOffset {
			t.Fatalf("auto.offset.reset=%v, ожидалось %v", got["auto.offset.reset"], cfg.Consumer.InitialOffset)
		}
	})

	t.Run("SASL_PLAINTEXT добавляет SASL-ключи", func(t *testing.T) {
		t.Parallel()

		cfg := testConfig()
		cfg.SecurityProtocol = SecurityProtocolSASLPlaintext
		cfg.SASL = SASL{Username: testSASLUser, Password: testSASLPassword, Mechanism: "SCRAM-SHA-256"}

		got := buildConsumerKafkaConfig(cfg)

		if got["sasl.mechanisms"] != "SCRAM-SHA-256" {
			t.Fatalf("sasl.mechanisms=%v, ожидалось SCRAM-SHA-256", got["sasl.mechanisms"])
		}

		if got["sasl.username"] != testSASLUser || got["sasl.password"] != testSASLPassword {
			t.Fatalf("sasl.username/password=%v/%v, ожидалось user/secret", got["sasl.username"], got["sasl.password"])
		}
	})
}

// configValidateTestCases вынесен из TestConfig_Validate отдельной функцией,
// чтобы тело теста укладывалось в лимит funlen — сама таблица случаев не несёт
// тестовой логики и не нуждается в t.Run/t.Parallel.
func configValidateTestCases() []struct {
	name        string
	config      Config
	wantErr     bool
	errContains string
} {
	return []struct {
		name        string
		config      Config
		wantErr     bool
		errContains string
	}{
		{
			name: "PLAINTEXT не требует SASL-учётных данных",
			config: Config{
				Brokers:          []string{testInvalidBroker},
				ClientID:         testInvalidClientID,
				SecurityProtocol: SecurityProtocolPlaintext,
			},
		},
		{
			name: "SSL не требует SASL-учётных данных",
			config: Config{
				Brokers:          []string{testInvalidBroker},
				ClientID:         testInvalidClientID,
				SecurityProtocol: "SSL",
			},
		},
		{
			name: "пустой SecurityProtocol валиден (нет SASL-проверки)",
			config: Config{
				Brokers:  []string{testInvalidBroker},
				ClientID: testInvalidClientID,
			},
		},
		{
			name: "SASL_PLAINTEXT без username возвращает ошибку",
			config: Config{
				Brokers:          []string{testInvalidBroker},
				ClientID:         testInvalidClientID,
				SecurityProtocol: SecurityProtocolSASLPlaintext,
				SASL:             SASL{Password: testSASLPassword},
			},
			wantErr:     true,
			errContains: envKeySASLUsername,
		},
		{
			name: "SASL_PLAINTEXT без password возвращает ошибку",
			config: Config{
				Brokers:          []string{testInvalidBroker},
				ClientID:         testInvalidClientID,
				SecurityProtocol: SecurityProtocolSASLPlaintext,
				SASL:             SASL{Username: testSASLUser},
			},
			wantErr:     true,
			errContains: envKeySASLPassword,
		},
		{
			name: "SASL_SSL без credentials возвращает ошибку",
			config: Config{
				Brokers:          []string{testInvalidBroker},
				ClientID:         testInvalidClientID,
				SecurityProtocol: SecurityProtocolSASLSSL,
			},
			wantErr:     true,
			errContains: envKeySASLUsername,
		},
		{
			name: "SASL_PLAINTEXT с полными credentials валиден",
			config: Config{
				Brokers:          []string{testInvalidBroker},
				ClientID:         testInvalidClientID,
				SecurityProtocol: SecurityProtocolSASLPlaintext,
				SASL:             SASL{Username: testSASLUser, Password: testSASLPassword},
			},
		},
		{
			name: "SASL_SSL с полными credentials валиден",
			config: Config{
				Brokers:          []string{testInvalidBroker},
				ClientID:         testInvalidClientID,
				SecurityProtocol: SecurityProtocolSASLSSL,
				SASL:             SASL{Username: testSASLUser, Password: testSASLPassword},
			},
		},
		{
			name: "SecurityProtocol регистронезависим (sasl_plaintext)",
			config: Config{
				Brokers:          []string{testInvalidBroker},
				ClientID:         testInvalidClientID,
				SecurityProtocol: "sasl_plaintext",
				SASL:             SASL{Username: testSASLUser, Password: testSASLPassword},
			},
		},
		{
			name: "SecurityProtocol регистронезависим (Sasl_Ssl)",
			config: Config{
				Brokers:          []string{testInvalidBroker},
				ClientID:         testInvalidClientID,
				SecurityProtocol: "Sasl_Ssl",
				SASL:             SASL{Username: testSASLUser, Password: testSASLPassword},
			},
		},
	}
}

func TestConfig_Validate(t *testing.T) {
	t.Parallel()

	for _, tc := range configValidateTestCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := tc.config.Validate()

			if tc.wantErr {
				if err == nil {
					t.Fatalf("Validate() вернул nil, ожидалась ошибка, содержащая %q", tc.errContains)
				}

				if tc.errContains != "" && !strings.Contains(err.Error(), tc.errContains) {
					t.Fatalf("Validate() error=%q не содержит ожидаемую подстроку %q", err.Error(), tc.errContains)
				}

				t.Logf("получена ожидаемая ошибка валидации: %v", err)

				return
			}

			if err != nil {
				t.Fatalf("Validate() вернул неожиданную ошибку: %v", err)
			}

			t.Log("Validate() вернул nil — конфиг валиден")
		})
	}
}

func TestTLS_EndpointIdentAlgorithm(t *testing.T) {
	t.Parallel()

	t.Run("InsecureSkipVerify=true переопределяет алгоритм в none", func(t *testing.T) {
		t.Parallel()

		tls := TLS{IdentificationAlgorithm: tlsIdentAlgorithmHTTPS, InsecureSkipVerify: true}

		got := tls.endpointIdentAlgorithm()

		if got != "none" {
			t.Fatalf("endpointIdentAlgorithm()=%q, ожидалось %q", got, "none")
		}

		t.Logf("InsecureSkipVerify=true: %q → none (переопределяет %q)", tls.IdentificationAlgorithm, got)
	})

	t.Run("InsecureSkipVerify=false возвращает IdentificationAlgorithm", func(t *testing.T) {
		t.Parallel()

		tls := TLS{IdentificationAlgorithm: tlsIdentAlgorithmHTTPS, InsecureSkipVerify: false}

		got := tls.endpointIdentAlgorithm()

		if got != tlsIdentAlgorithmHTTPS {
			t.Fatalf("endpointIdentAlgorithm()=%q, ожидалось %q", got, tlsIdentAlgorithmHTTPS)
		}

		t.Logf("InsecureSkipVerify=false: IdentificationAlgorithm=%q возвращён без изменений", got)
	})

	t.Run("пустой IdentificationAlgorithm возвращает https (secure by default)", func(t *testing.T) {
		t.Parallel()

		tls := TLS{InsecureSkipVerify: false}

		got := tls.endpointIdentAlgorithm()

		// security: пустое значение НЕ должно молча отключать проверку hostname
		// (CWE-295) — см. docs/security-audit.md. "https" совпадает с
		// собственным умолчанием librdkafka для ssl.endpoint.identification.algorithm.
		if got != tlsIdentAlgorithmHTTPS {
			t.Fatalf("endpointIdentAlgorithm()=%q, ожидалось %q (secure-by-default, как в librdkafka)", got, tlsIdentAlgorithmHTTPS)
		}

		t.Log("пустой IdentificationAlgorithm → \"https\" (secure by default) ✓")
	})

	t.Run("InsecureSkipVerify=true возвращает none даже при пустом IdentificationAlgorithm", func(t *testing.T) {
		t.Parallel()

		tls := TLS{InsecureSkipVerify: true}

		got := tls.endpointIdentAlgorithm()

		if got != "none" {
			t.Fatalf("endpointIdentAlgorithm()=%q, ожидалось %q", got, "none")
		}

		t.Log("InsecureSkipVerify=true явно отключает проверку hostname ✓")
	})
}
