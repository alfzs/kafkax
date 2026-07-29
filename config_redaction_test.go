package kafkax

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
)

// Тесты редакции пароля SASL.
//
// Редакция держится на трёх методах — LogValue, String и redactedOrEmpty, —
// и каждый из них снимается одной строкой, не роняя ни одного другого теста
// пакета: пароль просто начнёт печататься. Дефект такого рода не обнаруживает
// себя ни падением, ни ошибкой — только записью в чужом лог-хранилище, часто
// спустя месяцы. Отсюда форма ассертов: искать подстроку самого пароля во всём
// выводе, а не сравнивать вывод с эталоном. Эталон ловит только тот путь
// утечки, который автор теста предвидел; поиск канарейки ловит любой — включая
// новое поле, которое кто-нибудь добавит в SASL завтра.
//
// Часть тестов здесь была написана перевёрнутой: пока находка У1
// (docs/audit/05-security.md) оставалась открытой, они утверждали НАЛИЧИЕ
// утечки через %#v и json.Marshal — чтобы дыра была видна, а её закрытие стало
// заметным событием. Находка закрыта (SASL.GoString, SASL.MarshalJSON,
// Config.LogValue), и те же тесты теперь утверждают обратное. Форма ассертов не
// изменилась: ищется подстрока самого пароля.

const (
	// redactionCanary — пароль-канарейка: строка, которую невозможно
	// спутать ни с одним другим фрагментом вывода (ни с именем поля, ни с
	// маркером редакции), поэтому её присутствие в буфере всегда означает
	// именно утечку.
	redactionCanary = "s3cr3t-p@ssw0rd"
	// redactionMarker — чем SASL замещает непустой пароль.
	redactionMarker = "[REDACTED]"
)

// redactionSASL — секция SASL с канарейкой в пароле.
func redactionSASL() SASL {
	return SASL{
		Mechanism: SASLMechanismPlain,
		Username:  "svc-user",
		Password:  redactionCanary,
	}
}

// redactionWantSafe требует, чтобы канарейки в выводе не было, а маркер
// редакции — был.
//
// Второе условие не менее важно первого: вывод, где пароль просто пропущен,
// от вывода с редакцией неотличим по первой проверке, но по логу тогда нельзя
// понять, был ли пароль вообще задан — а это ровно тот вопрос, ради которого
// в лог смотрят при разборе отказа аутентификации.
func redactionWantSafe(t *testing.T, where, got string) {
	t.Helper()

	if strings.Contains(got, redactionCanary) {
		t.Errorf("%s: пароль утёк в вывод целиком:\n%s", where, got)
	}

	if !strings.Contains(got, redactionMarker) {
		t.Errorf("%s: в выводе нет маркера %q, непонятно, задан ли пароль:\n%s", where, redactionMarker, got)
	}
}

// TestSASLLogValueRedactsPassword — slog.LogValuer срабатывает на обоих
// штатных хендлерах stdlib.
//
// Хендлеры проверяются оба, потому что путь до значения у них разный: Text
// печатает атрибуты группы как есть, JSON сериализует их сам. Реализация
// LogValue общая, а вот сломать её так, чтобы упал только один из двух, вполне
// можно — например вернув строку вместо GroupValue.
func TestSASLLogValueRedactsPassword(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		handler func(*bytes.Buffer) slog.Handler
	}{
		{
			name:    "TextHandler",
			handler: func(b *bytes.Buffer) slog.Handler { return slog.NewTextHandler(b, nil) },
		},
		{
			name:    "JSONHandler",
			handler: func(b *bytes.Buffer) slog.Handler { return slog.NewJSONHandler(b, nil) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer

			slog.New(tt.handler(&buf)).Info("sasl configured", slog.Any("sasl", redactionSASL()))
			redactionWantSafe(t, tt.name, buf.String())
		})
	}
}

// TestSASLLogValueMarksUnsetPasswordDifferently — redactedOrEmpty отличает
// «пароль не задан» от «пароль есть».
//
// Если бы обе ситуации давали [REDACTED], лог перестал бы отвечать на главный
// вопрос при разборе отказа аутентификации: приехал ли пароль из окружения
// вообще. Пустая строка здесь — не небрежность, а сигнал, поэтому ассерт
// требует именно её и запрещает маркер.
func TestSASLLogValueMarksUnsetPasswordDifferently(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	unset := redactionSASL()
	unset.Password = ""

	slog.New(slog.NewTextHandler(&buf, nil)).Info("sasl configured", slog.Any("sasl", unset))

	got := buf.String()
	if !strings.Contains(got, `sasl.password=""`) {
		t.Errorf("незаданный пароль не виден как пустой:\n%s", got)
	}

	if strings.Contains(got, redactionMarker) {
		t.Errorf("незаданный пароль показан как %q — по логу не отличить «нет пароля» от «есть»:\n%s",
			redactionMarker, got)
	}
}

// TestRedactedOrEmptyContract — тот же контракт на уровне самой функции.
//
// Проверка через логи выше идёт сквозь два слоя чужого кода (slog + хендлер) и
// потому отвечает на вопрос «работает ли редакция целиком». Здесь же
// зафиксирован сам возврат: при рефакторинге, который перестроит вывод логов,
// именно эти два значения должны остаться неизменными.
func TestRedactedOrEmptyContract(t *testing.T) {
	t.Parallel()

	if got := redactedOrEmpty(""); got != "" {
		t.Errorf("redactedOrEmpty(\"\") = %q, want \"\" (пустое не является секретом)", got)
	}

	if got := redactedOrEmpty(redactionCanary); got != redactionMarker {
		t.Errorf("redactedOrEmpty(пароль) = %q, want %q", got, redactionMarker)
	}
}

// TestSASLStringRedactsAcrossVerbs — %v, %s и %+v не печатают пароль ни на
// самом SASL, ни на вложенном в Config.
//
// Вложенный случай — не дубль: fmt применяет Stringer к полям структуры сам, но
// только пока метод объявлен на значении. Смена приёмника String на *SASL
// компилируется, проходит все прочие тесты и молча возвращает пароль в вывод
// `%+v` от Config — а Config печатают куда чаще, чем SASL отдельно.
func TestSASLStringRedactsAcrossVerbs(t *testing.T) {
	t.Parallel()

	sasl := redactionSASL()

	cfg := testConfig(t)
	cfg.SASL = sasl

	cases := map[string]string{
		"SASL %v":    fmt.Sprintf("%v", sasl),
		"SASL %+v":   fmt.Sprintf("%+v", sasl),
		"Config %v":  fmt.Sprintf("%v", cfg),
		"Config %+v": fmt.Sprintf("%+v", cfg),
	}

	//nolint:staticcheck // S1025 предлагает вызвать String() напрямую — но проверяется именно то,
	// что до String доходит сам глагол %s, а не то, что String умеет редактировать.
	cases["SASL %s"] = fmt.Sprintf("%s", sasl)

	for where, got := range cases {
		redactionWantSafe(t, where, got)
	}
}

// TestSASLStringShowsUnsetPassword — String, как и LogValue, оставляет пустое
// место пустым.
//
// Сравнение с полной строкой, а не поиск подстроки: формат String — часть того,
// что читает человек в тексте ошибки, и «Password:» без значения должно
// остаться отличимым от «Password:[REDACTED]» посимвольно.
func TestSASLStringShowsUnsetPassword(t *testing.T) {
	t.Parallel()

	unset := redactionSASL()
	unset.Password = ""

	if got, want := unset.String(),
		"SASL{Mechanism:PLAIN Username:svc-user Password: AllowPlaintext:false}"; got != want {
		t.Errorf("String() без пароля = %q, want %q", got, want)
	}

	if got, want := redactionSASL().String(),
		"SASL{Mechanism:PLAIN Username:svc-user Password:[REDACTED] AllowPlaintext:false}"; got != want {
		t.Errorf("String() с паролем = %q, want %q", got, want)
	}
}

// TestSASLGoStringRedactsPassword — перевёрнутый сенсор У1: %#v больше не
// печатает пароль.
//
// Тест был написан наоборот и утверждал утечку, пока находка была открыта. Она
// закрыта методом GoString, и причина, по которой он понадобился отдельно от
// String, никуда не делась: fmt при флаге # спрашивает ТОЛЬКО GoStringer и
// Stringer игнорирует полностью. Снимут GoString — `log.Printf("%#v", cfg)` в
// чужом отладочном коде снова вынесет пароль наружу мимо всей остальной защиты.
//
// Config проверяется наравне с SASL: %#v обходит поля рекурсивно и спрашивает
// GoStringer у каждого, но держится это на приёмнике-значении — смена его на
// *SASL компилируется и молча возвращает пароль в вывод от Config.
func TestSASLGoStringRedactsPassword(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t)
	cfg.SASL = redactionSASL()

	for where, got := range map[string]string{
		"SASL %#v":   fmt.Sprintf("%#v", cfg.SASL),
		"Config %#v": fmt.Sprintf("%#v", cfg),
	} {
		redactionWantSafe(t, where, got)
	}
}

// TestSASLJSONMarshalRedactsPassword — вторая половина У1, тоже перевёрнутая:
// json.Marshal(SASL) отдаёт маркер вместо пароля.
//
// До MarshalJSON пароль уходил в JSON как есть: encoding/json не знает ни о
// Stringer, ни о LogValuer, а json-тегов у SASL нет. Значимость не
// теоретическая — `json.Marshal(cfg.SASL)` это обычный способ положить
// конфигурацию в ответ отладочной ручки или в дамп состояния.
//
// Проверяется и вложенный случай: encoding/json ищет Marshaler у каждого поля,
// но только пока метод объявлен на значении, — а Config сериализуют чаще, чем
// одну секцию.
func TestSASLJSONMarshalRedactsPassword(t *testing.T) {
	t.Parallel()

	got, err := json.Marshal(redactionSASL())
	if err != nil {
		t.Fatalf("json.Marshal(SASL): %v", err)
	}

	redactionWantSafe(t, "json.Marshal(SASL)", string(got))

	nested, err := json.Marshal(struct {
		SASL SASL `json:"sasl"`
	}{redactionSASL()})
	if err != nil {
		t.Fatalf("json.Marshal(вложенный SASL): %v", err)
	}

	redactionWantSafe(t, "json.Marshal(вложенный SASL)", string(nested))
}

// TestConfigJSONMarshalDoesNotDependOnFuncFields — пароль не утекает через
// json.Marshal(Config), и держится это больше не на случайности.
//
// Раньше тест назывался ...FailsOnFuncFields и проверял ровно причину:
// UnsupportedTypeError на типе поля-функции. Причина верна и сегодня —
// encoding/json по-прежнему спотыкается об OnPanic, OnMessageSkipped и
// TLSConfig.Time, и ошибка возникает из ТИПА поля, а не из значения, так что
// нулевые хуки не помогают. Но вывод «это единственное, что защищает пароль»
// после появления SASL.MarshalJSON неверен, и тест переписан под новый вывод.
//
// Отсюда вторая половина: Config без полей-функций собрать нельзя, поэтому
// сценарий «хуки переехали в интерфейс, Marshal начал проходить» проверяется на
// структуре-двойнике с тем же вложенным SASL. Ассерт на саму ошибку сохранён —
// но теперь как констатация, а не как гарантия.
func TestConfigJSONMarshalDoesNotDependOnFuncFields(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t)
	cfg.SASL = redactionSASL()

	//nolint:staticcheck // SA1026 сообщает, что этот Marshal заведомо провалится на поле-функции.
	// Это и есть проверяемое утверждение; молчание staticcheck здесь означало бы, что тест устарел.
	got, err := json.Marshal(cfg)

	if strings.Contains(string(got), redactionCanary) {
		t.Fatalf("json.Marshal(Config) вернул пароль:\n%s", got)
	}

	// Ошибка ожидается, но её отсутствие само по себе больше не дефект: если
	// хуки однажды уедут в интерфейс, Marshal пройдёт — и обязан отдать
	// отредактированный SASL. Ровно это проверяет двойник ниже, поэтому здесь
	// достаточно зафиксировать, на чём именно спотыкается encoding/json сейчас.
	if unsupported, ok := errors.AsType[*json.UnsupportedTypeError](err); ok {
		if !strings.HasPrefix(unsupported.Type.String(), "func(") {
			t.Errorf("Marshal провалился не на поле-функции, а на %s — причина изменилась",
				unsupported.Type)
		}
	}

	// Двойник Config без полей-функций: то, чем Config станет, если хуки
	// когда-нибудь перестанут блокировать сериализацию. Пароль обязан остаться
	// отредактированным и в этом случае.
	twin := struct {
		ClientID string `json:"client_id"`
		SASL     SASL   `json:"sasl"`
	}{cfg.ClientID, cfg.SASL}

	marshalled, err := json.Marshal(twin)
	if err != nil {
		t.Fatalf("json.Marshal(двойник Config): %v", err)
	}

	redactionWantSafe(t, "json.Marshal(двойник Config без полей-функций)", string(marshalled))
}

// TestConfigLoggedWholeRedactsPassword — обещание godoc у LogValue
// («при логировании Config целиком») выполняется на обоих штатных хендлерах.
//
// Раньше JSONHandler был отдельным тестом с перевёрнутым ассертом: у Config не
// было LogValue, хендлер пытался сериализовать структуру целиком, спотыкался о
// поля-функции и писал «!ERROR:json: unsupported type: func(...)». Пароль при
// этом не утекал, но и конфигурации в логе не было. С Config.LogValue оба
// хендлера идут одним путём, так что и проверяются вместе.
//
// Сценарий самый частый на практике: приложение пишет конфигурацию в лог один
// раз на старте, целиком.
func TestConfigLoggedWholeRedactsPassword(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		handler func(*bytes.Buffer) slog.Handler
	}{
		{
			name:    "TextHandler",
			handler: func(b *bytes.Buffer) slog.Handler { return slog.NewTextHandler(b, nil) },
		},
		{
			name:    "JSONHandler",
			handler: func(b *bytes.Buffer) slog.Handler { return slog.NewJSONHandler(b, nil) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer

			cfg := testConfig(t)
			cfg.SASL = redactionSASL()

			slog.New(tt.handler(&buf)).Info("kafka configured", slog.Any("config", cfg))

			got := buf.String()
			redactionWantSafe(t, tt.name+" + Config", got)

			// Маркер ошибки хендлера проверяется отдельно от редакции, потому
			// что это разные диагнозы с одинаковым симптомом: запись без
			// пароля бывает и такой, где вместо конфигурации лежит
			// «!ERROR:json: unsupported type». redactionWantSafe об этом
			// сообщит отсутствием [REDACTED], но не скажет почему.
			if strings.Contains(got, "!ERROR") {
				t.Errorf("%s не смог записать Config — похоже, Config.LogValue перестал работать "+
					"и хендлер снова сериализует структуру целиком:\n%s", tt.name, got)
			}

			// Config.LogValue обязан отдавать конфигурацию, а не один лишь
			// отредактированный SASL: запись, из которой нельзя узнать адреса
			// брокеров и client_id, диагностической ценности не имеет.
			if !strings.Contains(got, testClientID) {
				t.Errorf("%s: в записи нет client_id — LogValue отдаёт не всю конфигурацию:\n%s", tt.name, got)
			}
		})
	}
}
