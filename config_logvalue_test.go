package kafkax

import (
	"log/slog"
	"reflect"
	"strings"
	"testing"
)

// Тест состава Config.LogValue.
//
// Редакция пароля в той же записи — предмет config_redaction_test.go: там
// проверяется, чего в логе быть не должно, здесь — чего в нём не должно не
// быть. Дефект тихий в обе стороны, но по-разному: утечка видна хотя бы тому,
// кто её ищет, а пропавшее поле не ломает ни сборку, ни чтение записи — она
// остаётся валидной, просто беднее на одну настройку.

// configOpaqueLogFields — поля Config, значение которых в лог не помещается, и
// ключ, которым LogValue заменяет каждое из них; пустой ключ означает «в
// записи отсутствует полностью».
//
// Список ручной намеренно: он и есть решение «это поле в лог не идёт». Новое
// поле с yaml:"-" в нём не окажется, и TestConfigLogValueCoversEveryField
// потребует принять решение явно, вместо того чтобы унаследовать умолчание
// «пропустить».
var configOpaqueLogFields = map[string]string{
	"Logger":           "",
	"TLSConfig":        "tls_config_set",
	"ExtraOpts":        "extra_opts",
	"OnPanic":          "on_panic_set",
	"OnMessageSkipped": "on_message_skipped_set",
}

// TestConfigLogValueCoversEveryField — в записи Config.LogValue есть каждое
// поле структуры.
//
// Класс дефекта — ручной список, молча разъезжающийся со структурой; тот же,
// от которого заведён TestEnvNamesMatchStructTags, и лечится он тем же
// приёмом: источник истины — сама структура, а не второй список рядом с ней.
// Ровно так пропал KafkaLogLevel (находка Д2, docs/audit/09-mutation-sweep.md):
// поле меняет наблюдаемое поведение — порог логов franz-go, — а godoc метода
// обещает конфигурацию целиком. Проверки состава не существовало вовсе, так
// что удаление ещё и graceful_timeout набор тоже не заметил.
//
// Ключ выводится из тега yaml, а не берётся из третьего списка: yaml-ключ —
// то же имя настройки, что в конфигурационном файле, и запись в логе,
// названная иначе, заставляла бы читателя переводить одно в другое.
func TestConfigLogValueCoversEveryField(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t)
	cfg.KafkaLogLevel = KafkaLogWarn

	attrs := configLogAttrs(t, cfg)

	for field := range reflect.TypeFor[Config]().Fields() {
		yamlKey, _, _ := strings.Cut(field.Tag.Get("yaml"), ",")

		if yamlKey != "" && yamlKey != "-" {
			if _, ok := attrs[yamlKey]; !ok {
				t.Errorf("поле %s не попало в Config.LogValue: ожидался ключ %q", field.Name, yamlKey)
			}

			continue
		}

		key, decided := configOpaqueLogFields[field.Name]
		if !decided {
			t.Errorf("поле %s не размечено: решите, идёт ли оно в LogValue, "+
				"и внесите его в configOpaqueLogFields", field.Name)

			continue
		}

		if key == "" {
			continue
		}

		if _, ok := attrs[key]; !ok {
			t.Errorf("признак наличия %q для поля %s пропал из Config.LogValue", key, field.Name)
		}
	}

	// Ключ на месте, а значения нет: LogValue, отдающая пустую строку вместо
	// порога, прошла бы проверку состава и осталась бы бесполезной ровно там,
	// где в неё смотрят.
	if got := attrs["kafka_log_level"].String(); got != KafkaLogWarn {
		t.Errorf("kafka_log_level = %q, ожидалось %q", got, KafkaLogWarn)
	}
}

// configLogAttrs раскладывает Config.LogValue в отображение «ключ → значение».
func configLogAttrs(t *testing.T, cfg Config) map[string]slog.Value {
	t.Helper()

	value := cfg.LogValue()
	if value.Kind() != slog.KindGroup {
		t.Fatalf("Config.LogValue вернул %v, ожидалась группа атрибутов", value.Kind())
	}

	group := value.Group()

	attrs := make(map[string]slog.Value, len(group))
	for _, attr := range group {
		attrs[attr.Key] = attr.Value
	}

	return attrs
}
