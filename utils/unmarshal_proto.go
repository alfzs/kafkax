package utils

import (
	"fmt"

	"google.golang.org/protobuf/proto"
)

func UnmarshalProto[T proto.Message](data []byte, template T) (T, error) {
	var emptyMsg T

	if len(data) == 0 {
		return emptyMsg, fmt.Errorf("proto unmarshal failed: empty payload")
	}

	protoMsg := proto.Clone(template).(T)
	if err := proto.Unmarshal(data, protoMsg); err != nil {
		return emptyMsg, fmt.Errorf("proto unmarshal failed: %w", err)
	}
	return protoMsg, nil
}
