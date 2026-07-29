package kafkax

import (
	"errors"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

// testHeaderTenant — образцовый пользовательский заголовок: имя не
// зарезервировано и не пустое, поэтому годится и для проверки валидации, и для
// round-trip, и для дубликатов ключа.
const testHeaderTenant = "tenant"

// hdrEqual сравнивает Headers поэлементно, включая порядок.
//
// Порядок — часть контракта: Headers повторяет список заголовков протокола
// Kafka, где дубликаты ключей законны и различаются только позицией.
func hdrEqual(a, b Headers) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i].Key != b[i].Key || string(a[i].Value) != string(b[i].Value) {
			return false
		}
	}

	return true
}

func TestValidateHeaders(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		headers Headers
		wantErr error
		wantIn  string
	}{
		{name: "nil", headers: nil},
		{name: "пустой список", headers: Headers{}},
		{
			name:    "обычные заголовки",
			headers: Headers{{Key: testHeaderTenant, Value: []byte("42")}, {Key: "x-request-id", Value: nil}},
		},
		{
			// Поведение читателя при пустом имени зависит от клиента, поэтому
			// такой заголовок вообще не отправляется.
			name:    "пустое имя",
			headers: Headers{{Key: "ok", Value: []byte("v")}, {Key: "", Value: []byte("v")}},
			wantErr: ErrEmptyHeaderKey,
			wantIn:  "header 1",
		},
		{
			// traceparent/tracestate/baggage пишет OTel-propagator внутри
			// kotel.RecordCarrier.Set — молча перезаписывая пользовательское
			// значение. Отсюда отказ на границе API, а не тихая потеря.
			name:    "traceparent",
			headers: Headers{{Key: headerKeyTraceparent, Value: []byte("v")}},
			wantErr: ErrReservedHeaderKey,
			wantIn:  `header 0 ("traceparent")`,
		},
		{
			name:    "tracestate",
			headers: Headers{{Key: headerKeyTracestate}},
			wantErr: ErrReservedHeaderKey,
		},
		{
			name:    "baggage",
			headers: Headers{{Key: headerKeyBaggage}},
			wantErr: ErrReservedHeaderKey,
		},
		{
			// Имена заголовков W3C Trace Context определены в нижнем регистре,
			// и propagator ищет их побайтово. "Traceparent" в перезапись не
			// попадёт, поэтому и запрещать его незачем: проверка регистро-
			// зависимая осознанно, а не по недосмотру.
			name:    "регистр не совпадает с зарезервированным",
			headers: Headers{{Key: "Traceparent", Value: []byte("v")}},
		},
		{
			// Первая ошибка выигрывает: validateHeaders отвечает на вопрос
			// «отправлять ли», а не «сколько дефектов».
			name:    "несколько дефектов — сообщается первый",
			headers: Headers{{Key: ""}, {Key: headerKeyBaggage}},
			wantErr: ErrEmptyHeaderKey,
			wantIn:  "header 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateHeaders(tt.headers)

			if tt.wantErr == nil {
				if err != nil {
					t.Fatalf("validateHeaders = %v, want nil", err)
				}

				return
			}

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("validateHeaders = %v, want errors.Is(%v)", err, tt.wantErr)
			}

			// Индекс в тексте — то, ради чего ошибка оборачивается: без него
			// в списке из тридцати заголовков виноватого не найти.
			if tt.wantIn != "" && !strings.Contains(err.Error(), tt.wantIn) {
				t.Errorf("текст %q не содержит %q", err, tt.wantIn)
			}
		})
	}
}

func TestHeadersGet(t *testing.T) {
	t.Parallel()

	headers := Headers{
		{Key: "dup", Value: []byte("first")},
		{Key: "empty", Value: nil},
		{Key: "dup", Value: []byte("second")},
	}

	t.Run("первый из дубликатов", func(t *testing.T) {
		t.Parallel()

		// Дубликаты ключей законны в протоколе Kafka, и Get обязан быть
		// предсказуем: всегда первый, а не «какой попадётся».
		got, ok := headers.Get("dup")
		if !ok || string(got) != "first" {
			t.Fatalf("Get(dup) = %q, %v; want \"first\", true", got, ok)
		}
	})

	t.Run("отсутствующий ключ", func(t *testing.T) {
		t.Parallel()

		got, ok := headers.Get("missing")
		if ok || got != nil {
			t.Fatalf("Get(missing) = %q, %v; want nil, false", got, ok)
		}
	})

	t.Run("nil-значение отличается от отсутствия", func(t *testing.T) {
		t.Parallel()

		// Заголовок с пустым значением существует, и второй результат должен
		// это показывать: иначе «флаг выставлен пустой строкой» неотличим от
		// «флага нет».
		got, ok := headers.Get("empty")
		if !ok || got != nil {
			t.Fatalf("Get(empty) = %q, %v; want nil, true", got, ok)
		}
	})

	t.Run("nil-получатель", func(t *testing.T) {
		t.Parallel()

		var h Headers

		if _, ok := h.Get("any"); ok {
			t.Error("Get на nil Headers нашёл ключ")
		}
	})
}

func TestToRecordHeaders(t *testing.T) {
	t.Parallel()

	t.Run("пусто даёт nil", func(t *testing.T) {
		t.Parallel()

		// Именно nil, а не пустой слайс: kgo.Record с пустым непустым слайсом
		// заголовков всё равно кодируется как «заголовков нет», и лишняя
		// аллокация на каждое сообщение не нужна.
		if got := toRecordHeaders(nil); got != nil {
			t.Errorf("toRecordHeaders(nil) = %#v, want nil", got)
		}

		if got := toRecordHeaders(Headers{}); got != nil {
			t.Errorf("toRecordHeaders(пустые) = %#v, want nil", got)
		}
	})

	t.Run("порядок и дубликаты сохраняются", func(t *testing.T) {
		t.Parallel()

		in := Headers{
			{Key: "a", Value: []byte("1")},
			{Key: "b", Value: nil},
			{Key: "a", Value: []byte("2")},
		}

		got := toRecordHeaders(in)
		if len(got) != len(in) {
			t.Fatalf("длина %d, want %d", len(got), len(in))
		}

		for i := range in {
			if got[i].Key != in[i].Key || string(got[i].Value) != string(in[i].Value) {
				t.Errorf("заголовок %d = %+v, want %+v", i, got[i], in[i])
			}
		}
	})
}

func TestFromRecordHeaders(t *testing.T) {
	t.Parallel()

	t.Run("пусто даёт nil", func(t *testing.T) {
		t.Parallel()

		if got := fromRecordHeaders(nil); got != nil {
			t.Errorf("fromRecordHeaders(nil) = %#v, want nil", got)
		}

		if got := fromRecordHeaders([]kgo.RecordHeader{}); got != nil {
			t.Errorf("fromRecordHeaders(пустые) = %#v, want nil", got)
		}
	})

	t.Run("порядок сохраняется", func(t *testing.T) {
		t.Parallel()

		in := []kgo.RecordHeader{
			{Key: headerKeyTraceparent, Value: []byte("00-...")},
			{Key: testHeaderTenant, Value: []byte("42")},
		}

		got := fromRecordHeaders(in)

		want := Headers{
			// Зарезервированные имена запрещены только на отправку: пришедшие
			// от брокера заголовки конвертируются как есть, иначе консьюмер
			// терял бы trace context.
			{Key: headerKeyTraceparent, Value: []byte("00-...")},
			{Key: testHeaderTenant, Value: []byte("42")},
		}

		if !hdrEqual(got, want) {
			t.Errorf("fromRecordHeaders = %+v, want %+v", got, want)
		}
	})
}

func TestHeadersRoundTrip(t *testing.T) {
	t.Parallel()

	// Продюсер и консьюмер живут по разные стороны протокола, и конвертация
	// должна быть обратимой: заголовок, отправленный с дубликатом ключа и
	// пустым значением, обязан вернуться таким же.
	in := Headers{
		{Key: testHeaderTenant, Value: []byte("42")},
		{Key: testHeaderTenant, Value: []byte("43")},
		{Key: "flag", Value: []byte{}},
	}

	got := fromRecordHeaders(toRecordHeaders(in))
	if !hdrEqual(got, in) {
		t.Fatalf("после round-trip = %+v, want %+v", got, in)
	}

	if err := validateHeaders(got); err != nil {
		t.Fatalf("результат round-trip не проходит валидацию: %v", err)
	}
}
