package encoding

import (
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestUnmarshalProto_Success(t *testing.T) {
	t.Parallel()

	want := wrapperspb.String("hello")
	data, err := proto.Marshal(want)
	if err != nil {
		t.Fatalf("proto.Marshal() вернул неожиданную ошибку: %v", err)
	}

	got, err := UnmarshalProto[wrapperspb.StringValue](data)
	if err != nil {
		t.Fatalf("UnmarshalProto() вернул неожиданную ошибку: %v", err)
	}
	if got.GetValue() != want.GetValue() {
		t.Fatalf("UnmarshalProto() = %q, ожидалось %q", got.GetValue(), want.GetValue())
	}
}

func TestUnmarshalProto_InvalidBytes(t *testing.T) {
	t.Parallel()

	_, err := UnmarshalProto[wrapperspb.StringValue]([]byte{0xFF, 0xFF, 0xFF})

	if err == nil {
		t.Fatal("UnmarshalProto() с невалидными байтами вернул nil, ожидалась ошибка")
	}
	t.Logf("получена ожидаемая ошибка: %v ✓", err)
}

func TestUnmarshalProto_EmptyInput(t *testing.T) {
	t.Parallel()

	for _, data := range [][]byte{nil, {}} {
		got, err := UnmarshalProto[wrapperspb.StringValue](data)
		if err != nil {
			t.Fatalf("UnmarshalProto(%v) вернул неожиданную ошибку: %v", data, err)
		}
		if got.GetValue() != "" {
			t.Fatalf("UnmarshalProto(%v) = %q, ожидалось пустое значение", data, got.GetValue())
		}
	}
}
