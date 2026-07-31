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
// Ключ выводится из тега yaml, а не берётся из второго списка: yaml-ключ —
// то же имя настройки, что в конфигурационном файле, и запись в логе,
// названная иначе, заставляла бы читателя переводить одно в другое.
//
// Списка исключений больше нет, и это часть проверки: после разделения данных
// и поведения в Config не осталось ни одного поля с yaml:"-", а godoc
// структуры обещает, что и не появится. Поле, добавленное с таким тегом,
// провалит тест здесь же — вместе с требованием унести его в Option.
func TestConfigLogValueCoversEveryField(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t)
	cfg.KafkaLogLevel = KafkaLogWarn

	attrs := configLogAttrs(t, cfg)

	for field := range reflect.TypeFor[Config]().Fields() {
		yamlKey, _, _ := strings.Cut(field.Tag.Get("yaml"), ",")

		if yamlKey == "" || yamlKey == "-" {
			t.Errorf("у поля %s нет yaml-ключа (тег %q): Config — только сериализуемые данные, "+
				"а поведение задаётся опциями конструктора", field.Name, field.Tag.Get("yaml"))

			continue
		}

		if _, ok := attrs[yamlKey]; !ok {
			t.Errorf("поле %s не попало в Config.LogValue: ожидался ключ %q", field.Name, yamlKey)
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
