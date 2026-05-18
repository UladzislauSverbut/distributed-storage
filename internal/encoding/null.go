package encoding

type NullEncoder struct{}

func (NullEncoder) Encode() []byte      { return []byte{} }
func (NullEncoder) Decode(_ []byte) int { return 0 }

var Null = NullEncoder{}
