package kafkax

import (
	"errors"
	"reflect"
	"strconv"
	"testing"
	"time"
)

// TestDefaultConfigMatchesStructTags — DefaultConfig и теги env-default не
// могут разъехаться молча.
//
// DefaultConfig дублирует значения, которые cleanenv подставляет из тегов. Это
// осознанное дублирование: вытаскивать умолчания рефлексией в рантайме значило
// бы платить парсингом строк за каждое поле и отдавать вызывающему ошибку
// разбора собственного тега. Плата за дубль — этот тест: он проходит по всем
// полям Config и сверяет обе стороны, поэтому добавленное поле с тегом, но без
// строки в DefaultConfig, валит сборку тестов, а не продакшен потребителя.
//
// Проверка двусторонняя. Поле БЕЗ env-default обязано остаться нулевым: если бы
// DefaultConfig подставил, например, Consumer.Group, два разных сервиса молча
// оказались бы в одной группе — а тег на это поле никто не поставил именно
// потому, что осмысленного умолчания у него нет.
func TestDefaultConfigMatchesStructTags(t *testing.T) {
	t.Parallel()

	checked := cfgWalkDefaults(t, reflect.ValueOf(DefaultConfig()), "Config")

	// Страховка от тихой поломки самого обхода: если рефлексия перестанет
	// заходить внутрь секций, тест выродится в проверку двух полей и останется
	// зелёным. Число намеренно приблизительное снизу — точное пришлось бы
	// править на каждое новое поле, а смысл границы в том, чтобы обход не
	// схлопнулся.
	if checked < 25 {
		t.Fatalf("сверено полей с env-default: %d — обход по структуре сломан", checked)
	}
}

// cfgWalkDefaults рекурсивно сверяет значения v с тегами env-default и
// возвращает число сверенных полей с тегом.
func cfgWalkDefaults(t *testing.T, v reflect.Value, path string) int {
	t.Helper()

	var checked int

	typ := v.Type()

	for i := range typ.NumField() {
		field := typ.Field(i)
		value := v.Field(i)
		name := path + "." + field.Name

		def, hasDefault := field.Tag.Lookup("env-default")

		// Секции конфигурации (Producer, Consumer, SASL, TLS) сами тега не
		// несут — в них надо зайти. time.Duration тоже Struct'ом не является,
		// так что специального случая для него здесь не нужно.
		if value.Kind() == reflect.Struct && !hasDefault {
			checked += cfgWalkDefaults(t, value, name)

			continue
		}

		if !hasDefault {
			if !value.IsZero() {
				t.Errorf("%s: тега env-default нет, а DefaultConfig подставил %v — "+
					"умолчание, о котором не знает cleanenv", name, value.Interface())
			}

			continue
		}

		want := cfgParseDefault(t, name, field.Type, def)
		if got := value.Interface(); !reflect.DeepEqual(got, want) {
			t.Errorf("%s = %v, тег env-default обещает %v", name, got, want)
		}

		checked++
	}

	return checked
}

// cfgParseDefault превращает строку тега в значение поля.
func cfgParseDefault(t *testing.T, name string, typ reflect.Type, def string) any {
	t.Helper()

	// time.Duration проверяется до Kind: её базовый тип — int64, и общая ветка
	// разобрала бы "3m" как число и упала.
	if typ == reflect.TypeFor[time.Duration]() {
		d, err := time.ParseDuration(def)
		if err != nil {
			t.Fatalf("%s: тег env-default=%q не разбирается как длительность: %v", name, def, err)
		}

		return d
	}

	switch typ.Kind() { //nolint:exhaustive // остальные типы в Config с env-default не встречаются; default их и ловит
	case reflect.String:
		return def
	case reflect.Bool:
		b, err := strconv.ParseBool(def)
		if err != nil {
			t.Fatalf("%s: тег env-default=%q не разбирается как bool: %v", name, def, err)
		}

		return b
	case reflect.Int, reflect.Int32, reflect.Int64:
		n, err := strconv.ParseInt(def, 10, 64)
		if err != nil {
			t.Fatalf("%s: тег env-default=%q не разбирается как число: %v", name, def, err)
		}

		return reflect.ValueOf(n).Convert(typ).Interface()
	default:
		t.Fatalf("%s: тип %s с тегом env-default не поддержан тестом", name, typ)

		return nil
	}
}

// TestDefaultConfigIsUsableAfterFillingRequiredFields — база из DefaultConfig
// плюс три обязательных поля даёт валидный конфиг.
//
// Ровно тот сценарий, ради которого DefaultConfig и заведён: собранный
// литералом нулевой Config даёт около полутора десятков ошибок валидации, и
// без этой функции каждый вызывающий обязан знать все умолчания наизусть.
func TestDefaultConfigIsUsableAfterFillingRequiredFields(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.Brokers = []string{"localhost:9092"}
	cfg.ClientID = "kafkax-test"
	cfg.Consumer.Group = "kafkax-test-group"

	cfgWantNoErr(t, cfg.Validate())
}

// TestDefaultConfigWithoutRequiredFieldsFailsValidation — умолчания не
// подставляют идентичность.
//
// Brokers, ClientID и Consumer.Group специально оставлены пустыми: значение по
// умолчанию у группы означало бы, что два сервиса, забывших её задать, молча
// делят один assignment.
func TestDefaultConfigWithoutRequiredFieldsFailsValidation(t *testing.T) {
	t.Parallel()

	cfgWantErr(t, DefaultConfig().Validate(), "brokers", "client_id", "consumer.group")
}

// TestValidationErrorsCarryInvalidConfigSentinel — ошибку валидации можно
// опознать, не читая текст.
//
// Без сентинела вызывающий отличает «я неправильно настроил» от «брокер
// недоступен» только через strings.Contains, а errors.go прямым текстом
// объявляет тексты сообщений не частью контракта.
func TestValidationErrorsCarryInvalidConfigSentinel(t *testing.T) {
	t.Parallel()

	cases := map[string]error{
		"Validate": Config{}.Validate(),
		"validateProducer": func() error {
			_, err := NewKafkaProducer(Config{})

			return err
		}(),
		"validateConsumer": func() error {
			_, err := NewKafkaConsumer(Config{})

			return err
		}(),
	}

	for name, err := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("%v не опознаётся как ErrInvalidConfig", err)
			}

			// Конструкторы обязаны отдавать агрегат как есть. Обёртка через
			// fmt.Errorf подменила бы Unwrap() []error на Unwrap() error, и
			// документированный разбор списка перестал бы работать ровно там,
			// где он нужен — на ошибке конструктора.
			list := cfgUnwrapJoined(t, err)
			if len(list) < 2 {
				t.Fatalf("развернулось %d ошибок, ожидался список претензий к полям", len(list))
			}

			// Сентинел в списке не лежит: код, печатающий претензии
			// пользователю, иначе выводил бы «invalid configuration» первой
			// строкой перечня полей.
			for _, e := range list {
				if errors.Is(e, ErrInvalidConfig) {
					t.Fatalf("сентинел попал в список претензий: %v", e)
				}
			}
		})
	}
}

// TestValidConfigProducesNoError — агрегат не подменяет nil непустой ошибкой.
//
// Тип-агрегат легко ошибиться так, что пустой список станет ненулевой ошибкой
// «ничего не сломано»: возвращается указатель, а nil-указатель в интерфейсе
// ошибки не nil. Тогда ни один валидный конфиг не пройдёт конструктор.
func TestValidConfigProducesNoError(t *testing.T) {
	t.Parallel()

	if err := newConfigError("config", nil); err != nil {
		t.Fatalf("newConfigError(пустой список) = %v, want nil", err)
	}
}
