package kafkax

import (
	"runtime/debug"
	"testing"

	"go.opentelemetry.io/otel/metric"
)

// Instrumentation scope доменных метрик: версия и её отсутствие.
//
// Класс дефекта здесь тихий: scope не влияет ни на имена метрик, ни на
// атрибуты, поэтому неверная или пустая instrumentation.version не роняет
// ничего и не видна ни в одном функциональном тесте. Видна она в бэкенде —
// когда после релиза графики строятся по двум scope сразу или, наоборот, по
// чужому.

// testModuleVersion — версия пакета в подставном build info. Значение
// произвольное; важно только, что оно не совпадает с версией «приложения» в
// тех же случаях, иначе поиск по чужому пути прошёл бы за верный.
const testModuleVersion = "v2.4.1"

// TestMeterOptionsOmitUnknownVersion — при неизвестной версии опций нет вовсе.
//
// Мутация, от которой тест защищает: убрать проверку на пустую строку и
// подставлять версию всегда. Отказ при этом молчаливый — meter создаётся,
// метрики пишутся, а в экспорте у scope стоит instrumentation.version="",
// которую бэкенд честно считает отдельной версией библиотеки.
func TestMeterOptionsOmitUnknownVersion(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		version string
		want    string
	}{
		"версия известна":    {version: testModuleVersion, want: testModuleVersion},
		"версии нет":         {version: "", want: ""},
		"псевдоверсия":       {version: "v2.0.1-0.20240101000000-abcdef123456", want: "v2.0.1-0.20240101000000-abcdef123456"},
		"(devel) — не мусор": {version: "(devel)", want: "(devel)"},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			opts := meterOptionsFor(tt.version)

			if tt.want == "" && len(opts) != 0 {
				t.Fatalf("опций при пустой версии = %d, want 0", len(opts))
			}

			cfg := metric.NewMeterConfig(opts...)
			if got := cfg.InstrumentationVersion(); got != tt.want {
				t.Errorf("instrumentation.version = %q, want %q", got, tt.want)
			}

			// WithSchemaURL пакет не объявляет намеренно: доменные метрики
			// отклоняются от messaging semantic conventions (topic/status
			// вместо messaging.destination.name), и схемо-осведомлённый бэкенд,
			// поверив объявлению, переименовал бы атрибуты по чужим правилам.
			// Решение стоит одной строки и отменяется тоже одной, поэтому
			// зафиксировано ассертом, а не только комментарием в otel.go.
			if got := cfg.SchemaURL(); got != "" {
				t.Errorf("scope объявил схему %q — см. «Что осознанно не делаем» в docs/audit/03-observability.md", got)
			}
		})
	}
}

// TestModuleVersionFindsPackageInBuildInfo — версия ищется по обоим местам, где
// модуль пакета может оказаться в build info, и только по совпадению пути.
//
// Оба места настоящие: главный модуль — это сборка тестов и примеров самого
// пакета, зависимость — единственный способ, которым его видит приложение.
// Мутации, от которых тест защищает: выбросить цикл по Deps (в приложении
// версия исчезнет), не сверять Main.Path (в scope уедет версия приложения —
// самый неприятный исход, потому что выглядит правдоподобно).
func TestModuleVersionFindsPackageInBuildInfo(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		info *debug.BuildInfo
		ok   bool
		want string
	}{
		"build info недоступно": {
			// go run по файлам, сборка без модуля: не отказ, просто версии нет.
			ok: false,
		},
		"пакет — главный модуль": {
			info: &debug.BuildInfo{
				Main: debug.Module{Path: instrumentationModule, Version: testModuleVersion},
			},
			ok:   true,
			want: testModuleVersion,
		},
		"пакет — зависимость": {
			info: &debug.BuildInfo{
				Main: debug.Module{Path: "example.com/service", Version: "v1.7.0"},
				Deps: []*debug.Module{
					{Path: "github.com/twmb/franz-go", Version: "v1.19.0"},
					{Path: instrumentationModule, Version: testModuleVersion},
				},
			},
			ok:   true,
			want: testModuleVersion,
		},
		"пакета в сборке нет": {
			// Версия приложения не должна выдаваться за версию библиотеки.
			info: &debug.BuildInfo{
				Main: debug.Module{Path: "example.com/service", Version: "v1.7.0"},
				Deps: []*debug.Module{{Path: "github.com/twmb/franz-go", Version: "v1.19.0"}},
			},
			ok: true,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if got := moduleVersion(tt.info, tt.ok); got != tt.want {
				t.Errorf("moduleVersion = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestMeterOptionsUseRealBuildInfo — на настоящем build info тестового бинаря
// meterOptions отдаёт непустую версию.
//
// Половина, которую подставные структуры проверить не могут: что
// instrumentationModule совпадает с путём модуля из go.mod. Разойдись они —
// обе ветки поиска промахнутся, и scope останется без версии навсегда, ни на
// чём не упав.
func TestMeterOptionsUseRealBuildInfo(t *testing.T) {
	t.Parallel()

	got := metric.NewMeterConfig(meterOptions()...).InstrumentationVersion()
	if got == "" {
		t.Errorf("версия scope пуста: instrumentationModule = %q разошёлся с путём модуля в go.mod",
			instrumentationModule)
	}
}
