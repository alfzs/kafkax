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
// Часть тестов здесь фиксирует НЕбезопасное поведение (%#v и json.Marshal —
// находка У1 в docs/audit/05-security.md). Они намеренно утверждают утечку:
// пока дыра открыта, тест держит её видимой, а когда её закроют — упадёт и
// потребует переписать себя на противоположное утверждение.

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

	if got, want := unset.String(), "SASL{Mechanism:PLAIN Username:svc-user Password:}"; got != want {
		t.Errorf("String() без пароля = %q, want %q", got, want)
	}

	if got, want := redactionSASL().String(),
		"SASL{Mechanism:PLAIN Username:svc-user Password:[REDACTED]}"; got != want {
		t.Errorf("String() с паролем = %q, want %q", got, want)
	}
}

// TestSASLGoStringLeaksPassword фиксирует ДЕЙСТВУЮЩУЮ УТЕЧКУ: %#v печатает
// пароль открытым текстом (находка У1 в docs/audit/05-security.md).
//
// fmt при флаге # спрашивает только GoStringer и полностью игнорирует
// Stringer, а GoString у SASL нет. Достаточно одного `log.Printf("%#v", cfg)`
// в чужом отладочном коде, чтобы вся защита оказалась ни при чём.
//
// Ассерт утверждает утечку намеренно: тест — не одобрение поведения, а датчик.
// Пока дыра открыта, он держит её задокументированной; как только появится
// GoString или Format, тест упадёт и потребует переписать себя на
// redactionWantSafe.
func TestSASLGoStringLeaksPassword(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t)
	cfg.SASL = redactionSASL()

	for where, got := range map[string]string{
		"SASL %#v":   fmt.Sprintf("%#v", cfg.SASL),
		"Config %#v": fmt.Sprintf("%#v", cfg),
	} {
		if !strings.Contains(got, redactionCanary) {
			t.Errorf("%s больше не печатает пароль — похоже, добавили GoString/Format. "+
				"Находка У1 закрыта: перепиши ассерт на redactionWantSafe.\n%s", where, got)
		}
	}
}

// TestSASLJSONMarshalLeaksPassword фиксирует вторую половину У1: у SASL нет ни
// json-тегов, ни MarshalJSON, поэтому пароль уходит в JSON как есть.
//
// Значимость не теоретическая: `json.Marshal(cfg.SASL)` — обычный способ
// положить конфигурацию в отладочный ответ ручки /debug или в дамп состояния.
// Ассерт, как и в тесте на %#v, утверждает утечку, чтобы её закрытие стало
// заметным событием, а не тихим изменением.
func TestSASLJSONMarshalLeaksPassword(t *testing.T) {
	t.Parallel()

	//nolint:gosec // G117 («сериализуется поле, похожее на секрет») — не побочный эффект, а ровно
	// тот дефект, который тест удерживает задокументированным.
	got, err := json.Marshal(redactionSASL())
	if err != nil {
		t.Fatalf("json.Marshal(SASL): %v", err)
	}

	if !strings.Contains(string(got), redactionCanary) {
		t.Errorf("json.Marshal(SASL) больше не печатает пароль — похоже, появился MarshalJSON. "+
			"Находка У1 закрыта: перепиши ассерт на redactionWantSafe.\n%s", got)
	}
}

// TestConfigJSONMarshalFailsOnFuncFields — пароль не утекает через
// json.Marshal(Config) только потому, что encoding/json спотыкается о поля-функции.
//
// Это не редакция, а случайность, и тест устроен так, чтобы это было видно из
// его падения. Проверяется не «пароля нет», а конкретная причина, по которой
// его нет: UnsupportedTypeError на типе функции. Ошибка возникает от типа поля,
// а не от значения — OnPanic и OnMessageSkipped здесь nil, и это не помогает.
//
// Если однажды хуки переедут в интерфейс или обзаведутся тегом `json:"-"`,
// Marshal начнёт проходить и вынесет пароль наружу. Упадёт при этом именно этот
// тест — и именно там, где написано, что делать.
func TestConfigJSONMarshalFailsOnFuncFields(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t)
	cfg.SASL = redactionSASL()

	//nolint:staticcheck // SA1026 сообщает, что этот Marshal заведомо провалится на поле-функции.
	// Это и есть проверяемое утверждение; молчание staticcheck здесь означало бы, что тест устарел.
	got, err := json.Marshal(cfg)

	unsupported, ok := errors.AsType[*json.UnsupportedTypeError](err)
	if !ok {
		t.Fatalf("json.Marshal(Config) = (%s, %v), ожидалась UnsupportedTypeError. "+
			"Поля-функции перестали блокировать сериализацию — единственное, что до сих пор "+
			"удерживало пароль от попадания в JSON. Нужен SASL.MarshalJSON (находка У1).", got, err)
	}

	// Тип из ошибки проверяется отдельно: UnsupportedTypeError на чём-нибудь
	// другом (канал, комплексное число) означал бы, что срабатывает уже иная
	// случайность, и вывод «пароль защищён полями-функциями» перестал быть верным.
	if !strings.HasPrefix(unsupported.Type.String(), "func(") {
		t.Errorf("Marshal провалился не на поле-функции, а на %s — причина отсутствия утечки изменилась",
			unsupported.Type)
	}

	if strings.Contains(string(got), redactionCanary) {
		t.Errorf("json.Marshal(Config) вернул пароль вместе с ошибкой:\n%s", got)
	}
}

// TestConfigLoggedWholeRedactsPassword — обещание godoc у LogValue
// («при логировании Config целиком») выполняется на TextHandler.
//
// Держится оно не на Config: LogValue у Config нет, TextHandler печатает
// незнакомое значение через %+v, и редактирует пароль всё тот же SASL.String().
// Тест закрывает самый вероятный сценарий на практике — приложение пишет
// конфигурацию в лог один раз на старте, целиком.
func TestConfigLoggedWholeRedactsPassword(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	cfg := testConfig(t)
	cfg.SASL = redactionSASL()

	slog.New(slog.NewTextHandler(&buf, nil)).Info("kafka configured", slog.Any("config", cfg))
	redactionWantSafe(t, "TextHandler + Config", buf.String())
}

// TestConfigLoggedWholeOnJSONHandlerFailsInsteadOfRedacting — на JSONHandler то
// же самое обещание не выполняется: вместо конфигурации в лог уезжает
// «!ERROR:json: unsupported type: func(...)».
//
// Пароль не утёк, но и конфигурации в логе нет — защита здесь та же
// случайность, что и в TestConfigJSONMarshalFailsOnFuncFields, только
// последствие другое: диагностическая запись бесполезна. Ассерт требует
// присутствия маркера ошибки, потому что исчезнуть он может двумя разными
// путями — добавили Config.LogValue (хорошо) или убрали поля-функции (утечка), —
// и отличать их обязан человек, читающий падение.
func TestConfigLoggedWholeOnJSONHandlerFailsInsteadOfRedacting(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	cfg := testConfig(t)
	cfg.SASL = redactionSASL()

	slog.New(slog.NewJSONHandler(&buf, nil)).Info("kafka configured", slog.Any("config", cfg))

	got := buf.String()
	if strings.Contains(got, redactionCanary) {
		t.Fatalf("пароль утёк в JSON-лог целого Config:\n%s", got)
	}

	if !strings.Contains(got, "!ERROR:json: unsupported type") {
		t.Errorf("JSONHandler больше не спотыкается о поля-функции Config. Проверь, что появился "+
			"Config.LogValue, а не просто исчезли хуки — во втором случае это утечка (находка У1).\n%s", got)
	}
}
