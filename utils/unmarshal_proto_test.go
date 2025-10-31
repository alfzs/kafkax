package utils_test

import (
	"testing"

	"github.com/alfzs/kitmed/internal/delivery/event/utils"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// TestUnmarshalProto_Success проверяет корректное разбирание валидного protobuf-сообщения
func TestUnmarshalProto_Success(t *testing.T) {
	original := wrapperspb.String("test value")
	data, err := proto.Marshal(original)
	if err != nil {
		t.Fatalf("Failed to marshal test proto: %v", err)
	}

	template := &wrapperspb.StringValue{}
	result, err := utils.UnmarshalProto(data, template)
	if err != nil {
		t.Errorf("UnmarshalProto returned unexpected error: %v", err)
	}

	if result.GetValue() != original.GetValue() {
		t.Errorf("Expected %q, got %q", original.GetValue(), result.GetValue())
	}
}

// TestUnmarshalProto_EmptyPayload проверяет обработку пустого payload
func TestUnmarshalProto_EmptyPayload(t *testing.T) {
	template := &wrapperspb.StringValue{}
	_, err := utils.UnmarshalProto([]byte{}, template)
	if err == nil {
		t.Error("Expected error for empty payload, got nil")
	}

	expectedErr := "proto unmarshal failed: empty payload"
	if err.Error() != expectedErr {
		t.Errorf("Expected error %q, got %q", expectedErr, err.Error())
	}
}

// TestUnmarshalProto_NilPayload проверяет обработку nil payload
func TestUnmarshalProto_NilPayload(t *testing.T) {
	template := &wrapperspb.StringValue{}
	_, err := utils.UnmarshalProto(nil, template)
	if err == nil {
		t.Error("Expected error for nil payload, got nil")
	}

	expectedErr := "proto unmarshal failed: empty payload"
	if err.Error() != expectedErr {
		t.Errorf("Expected error %q, got %q", expectedErr, err.Error())
	}
}

// TestUnmarshalProto_InvalidData проверяет обработку невалидных protobuf данных
func TestUnmarshalProto_InvalidData(t *testing.T) {
	invalidData := []byte("not a valid protobuf")
	template := &wrapperspb.StringValue{}

	_, err := utils.UnmarshalProto(invalidData, template)
	if err == nil {
		t.Error("Expected error for invalid protobuf data, got nil")
	}

	expectedErrPrefix := "proto unmarshal failed:"
	if err.Error()[:len(expectedErrPrefix)] != expectedErrPrefix {
		t.Errorf("Expected error starting with %q, got %q", expectedErrPrefix, err.Error())
	}
}

// TestUnmarshalProto_WithDifferentProtoType проверяет работу с другим типом protobuf-сообщения
func TestUnmarshalProto_WithDifferentProtoType(t *testing.T) {
	original := wrapperspb.Int32(42)
	data, err := proto.Marshal(original)
	if err != nil {
		t.Fatalf("Failed to marshal test proto: %v", err)
	}

	template := &wrapperspb.Int32Value{}
	result, err := utils.UnmarshalProto(data, template)
	if err != nil {
		t.Errorf("UnmarshalProto returned unexpected error: %v", err)
	}

	if result.GetValue() != original.GetValue() {
		t.Errorf("Expected %d, got %d", original.GetValue(), result.GetValue())
	}
}

// TestUnmarshalProto_WithBoolValue проверяет работу с boolean типом
func TestUnmarshalProto_WithBoolValue(t *testing.T) {
	original := wrapperspb.Bool(true)
	data, err := proto.Marshal(original)
	if err != nil {
		t.Fatalf("Failed to marshal test proto: %v", err)
	}

	template := &wrapperspb.BoolValue{}
	result, err := utils.UnmarshalProto(data, template)
	if err != nil {
		t.Errorf("UnmarshalProto returned unexpected error: %v", err)
	}

	if result.GetValue() != original.GetValue() {
		t.Errorf("Expected %t, got %t", original.GetValue(), result.GetValue())
	}
}
