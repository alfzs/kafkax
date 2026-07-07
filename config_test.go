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
		cfg.SecurityProtocol = "PLAINTEXT"

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
		cfg.SecurityProtocol = "SASL_SSL"
		cfg.SASL = SASL{Username: "user", Password: "secret", Mechanism: "PLAIN"}

		got := buildProducerKafkaConfig(cfg)

		if got["sasl.mechanisms"] != "PLAIN" {
			t.Fatalf("sasl.mechanisms=%v, ожидалось PLAIN", got["sasl.mechanisms"])
		}
		if got["sasl.username"] != "user" || got["sasl.password"] != "secret" {
			t.Fatalf("sasl.username/password=%v/%v, ожидалось user/secret", got["sasl.username"], got["sasl.password"])
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
		cfg.SecurityProtocol = "PLAINTEXT"

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
		cfg.SecurityProtocol = "SASL_PLAINTEXT"
		cfg.SASL = SASL{Username: "user", Password: "secret", Mechanism: "SCRAM-SHA-256"}

		got := buildConsumerKafkaConfig(cfg)

		if got["sasl.mechanisms"] != "SCRAM-SHA-256" {
			t.Fatalf("sasl.mechanisms=%v, ожидалось SCRAM-SHA-256", got["sasl.mechanisms"])
		}
		if got["sasl.username"] != "user" || got["sasl.password"] != "secret" {
			t.Fatalf("sasl.username/password=%v/%v, ожидалось user/secret", got["sasl.username"], got["sasl.password"])
		}
	})
}

func TestConfig_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		config      Config
		wantErr     bool
		errContains string
	}{
		{
			name: "PLAINTEXT не требует SASL-учётных данных",
			config: Config{
				Brokers:          []string{"localhost:9092"},
				ClientID:         "test",
				SecurityProtocol: "PLAINTEXT",
			},
		},
		{
			name: "SSL не требует SASL-учётных данных",
			config: Config{
				Brokers:          []string{"localhost:9092"},
				ClientID:         "test",
				SecurityProtocol: "SSL",
			},
		},
		{
			name: "пустой SecurityProtocol валиден (нет SASL-проверки)",
			config: Config{
				Brokers:  []string{"localhost:9092"},
				ClientID: "test",
			},
		},
		{
			name: "SASL_PLAINTEXT без username возвращает ошибку",
			config: Config{
				Brokers:          []string{"localhost:9092"},
				ClientID:         "test",
				SecurityProtocol: "SASL_PLAINTEXT",
				SASL:             SASL{Password: "secret"},
			},
			wantErr:     true,
			errContains: "KAFKAX_SASL_USERNAME",
		},
		{
			name: "SASL_PLAINTEXT без password возвращает ошибку",
			config: Config{
				Brokers:          []string{"localhost:9092"},
				ClientID:         "test",
				SecurityProtocol: "SASL_PLAINTEXT",
				SASL:             SASL{Username: "user"},
			},
			wantErr:     true,
			errContains: "KAFKAX_SASL_PASSWORD",
		},
		{
			name: "SASL_SSL без credentials возвращает ошибку",
			config: Config{
				Brokers:          []string{"localhost:9092"},
				ClientID:         "test",
				SecurityProtocol: "SASL_SSL",
			},
			wantErr:     true,
			errContains: "KAFKAX_SASL_USERNAME",
		},
		{
			name: "SASL_PLAINTEXT с полными credentials валиден",
			config: Config{
				Brokers:          []string{"localhost:9092"},
				ClientID:         "test",
				SecurityProtocol: "SASL_PLAINTEXT",
				SASL:             SASL{Username: "user", Password: "secret"},
			},
		},
		{
			name: "SASL_SSL с полными credentials валиден",
			config: Config{
				Brokers:          []string{"localhost:9092"},
				ClientID:         "test",
				SecurityProtocol: "SASL_SSL",
				SASL:             SASL{Username: "user", Password: "secret"},
			},
		},
		{
			name: "SecurityProtocol регистронезависим (sasl_plaintext)",
			config: Config{
				Brokers:          []string{"localhost:9092"},
				ClientID:         "test",
				SecurityProtocol: "sasl_plaintext",
				SASL:             SASL{Username: "user", Password: "secret"},
			},
		},
		{
			name: "SecurityProtocol регистронезависим (Sasl_Ssl)",
			config: Config{
				Brokers:          []string{"localhost:9092"},
				ClientID:         "test",
				SecurityProtocol: "Sasl_Ssl",
				SASL:             SASL{Username: "user", Password: "secret"},
			},
		},
	}

	for _, tc := range tests {
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
		tls := TLS{IdentificationAlgorithm: "https", InsecureSkipVerify: true}

		got := tls.endpointIdentAlgorithm()

		if got != "none" {
			t.Fatalf("endpointIdentAlgorithm()=%q, ожидалось %q", got, "none")
		}
		t.Logf("InsecureSkipVerify=true: %q → none (переопределяет %q)", tls.IdentificationAlgorithm, got)
	})

	t.Run("InsecureSkipVerify=false возвращает IdentificationAlgorithm", func(t *testing.T) {
		t.Parallel()
		tls := TLS{IdentificationAlgorithm: "https", InsecureSkipVerify: false}

		got := tls.endpointIdentAlgorithm()

		if got != "https" {
			t.Fatalf("endpointIdentAlgorithm()=%q, ожидалось %q", got, "https")
		}
		t.Logf("InsecureSkipVerify=false: IdentificationAlgorithm=%q возвращён без изменений", got)
	})

	t.Run("пустой IdentificationAlgorithm возвращает none (librdkafka запрещает пустое значение)", func(t *testing.T) {
		t.Parallel()
		tls := TLS{InsecureSkipVerify: false}

		got := tls.endpointIdentAlgorithm()

		if got != "none" {
			t.Fatalf("endpointIdentAlgorithm()=%q, ожидалось %q (librdkafka запрещает пустую строку)", got, "none")
		}
		t.Log("пустой IdentificationAlgorithm → \"none\" (безопасное умолчание) ✓")
	})
}
