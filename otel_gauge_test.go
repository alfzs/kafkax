package kafkax

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

var errForcedGaugeRegistration = errors.New("forced gauge registration failure")

// failingGaugeMeter оборачивает noop.Meter, форсируя ошибку регистрации
// Int64ObservableGauge — единственный способ детерминированно воспроизвести
// путь ошибки регистрации gauge в NewKafkaProducer/NewKafkaConsumer, не трогая
// остальные инструменты (newProducerMetrics/newConsumerMetrics их игнорируют).
type failingGaugeMeter struct {
	noop.Meter
}

func (failingGaugeMeter) Int64ObservableGauge(string, ...metric.Int64ObservableGaugeOption) (metric.Int64ObservableGauge, error) {
	return nil, errForcedGaugeRegistration
}

type failingGaugeMeterProvider struct {
	noop.MeterProvider
}

func (failingGaugeMeterProvider) Meter(string, ...metric.MeterOption) metric.Meter {
	return failingGaugeMeter{}
}

// otel.SetMeterProvider делегирует ошибку/провайдер один раз и НАВСЕГДА для
// всего процесса (go.opentelemetry.io/otel/internal/global: delegateMeterOnce
// sync.Once) — любой Meter, полученный через otel.Meter(...) до или после
// восстановления "оригинального" провайдера, продолжит делегировать в
// подменённый провайдер. Поэтому подмену нельзя делать в общем тестовом
// процессе (сломает все остальные тесты, создающие Producer/Consumer) — сценарий
// форсированной ошибки регистрации gauge запускается в отдельном subprocess'е
// (классический паттерн Go для тестирования необратимого глобального состояния,
// как в тестах os.Exit).
const gaugeFailureSubprocessEnv = "KAFKAX_GAUGE_FAILURE_SUBPROCESS"

func runGaugeFailureSubprocess(t *testing.T, testName string) {
	t.Helper()

	//nolint:gosec // os.Args[0] — путь к собранному тестовому бинарнику, не внешний ввод
	cmd := exec.CommandContext(context.Background(), os.Args[0], "-test.run=^"+testName+"$", "-test.v")
	cmd.Env = append(os.Environ(), gaugeFailureSubprocessEnv+"="+testName)

	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("subprocess %s завершился с ошибкой: %v\nвывод:\n%s", testName, err, out)
	}

	t.Logf("subprocess %s вывод:\n%s", testName, out)
}

// TestNewKafkaProducer_GaugeRegistrationError проверяет путь ошибки регистрации
// queue-depth gauge: producer.Close() должен быть вызван, а конструктор — вернуть
// обёрнутую ошибку.
func TestNewKafkaProducer_GaugeRegistrationError(t *testing.T) {
	t.Parallel()

	if os.Getenv(gaugeFailureSubprocessEnv) != "TestNewKafkaProducer_GaugeRegistrationError" {
		runGaugeFailureSubprocess(t, "TestNewKafkaProducer_GaugeRegistrationError")
		return
	}

	otel.SetMeterProvider(failingGaugeMeterProvider{})

	_, err := NewKafkaProducer(context.Background(), testConfig())
	if err == nil {
		fmt.Fprintln(os.Stderr, "NewKafkaProducer() с MeterProvider, форсирующим ошибку gauge, вернул nil, ожидалась ошибка")
		os.Exit(1)
	}

	if !strings.Contains(err.Error(), "registering queue depth gauge") {
		fmt.Fprintf(os.Stderr, "NewKafkaProducer() error=%q не содержит ожидаемый контекст про gauge\n", err.Error())
		os.Exit(1)
	}

	fmt.Printf("NewKafkaProducer() корректно вернул ошибку регистрации gauge: %v\n", err)
}

// TestNewKafkaConsumer_GaugeRegistrationError — консьюмерный аналог, проверяет
// errors.Join(err, consumer.Close()) в NewKafkaConsumer.
func TestNewKafkaConsumer_GaugeRegistrationError(t *testing.T) {
	t.Parallel()

	if os.Getenv(gaugeFailureSubprocessEnv) != "TestNewKafkaConsumer_GaugeRegistrationError" {
		runGaugeFailureSubprocess(t, "TestNewKafkaConsumer_GaugeRegistrationError")
		return
	}

	otel.SetMeterProvider(failingGaugeMeterProvider{})

	_, err := NewKafkaConsumer(testConfig())
	if err == nil {
		fmt.Fprintln(os.Stderr, "NewKafkaConsumer() с MeterProvider, форсирующим ошибку gauge, вернул nil, ожидалась ошибка")
		os.Exit(1)
	}

	if !strings.Contains(err.Error(), "registering queue depth gauge") {
		fmt.Fprintf(os.Stderr, "NewKafkaConsumer() error=%q не содержит ожидаемый контекст про gauge\n", err.Error())
		os.Exit(1)
	}

	fmt.Printf("NewKafkaConsumer() корректно вернул ошибку регистрации gauge: %v\n", err)
}
