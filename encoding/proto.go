package encoding

import (
	"fmt"

	"google.golang.org/protobuf/proto"
)

// UnmarshalProto десериализует wire-формат proto в конкретный тип T.
// Тип передаётся через параметр типа, экземпляр создаётся внутри — шаблон не нужен:
//
//	msg, err := encoding.UnmarshalProto[pb.OrderCreated](data)
func UnmarshalProto[T any, PT interface {
	proto.Message
	*T
}](data []byte) (PT, error) {
	msg := PT(new(T))
	if err := proto.Unmarshal(data, msg); err != nil {
		return nil, fmt.Errorf("proto unmarshal: %w", err)
	}
	return msg, nil
}
