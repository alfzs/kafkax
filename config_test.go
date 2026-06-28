package kafkax

import (
	"strings"
	"testing"
)

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
