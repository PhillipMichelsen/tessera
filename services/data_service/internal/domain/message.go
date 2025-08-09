package domain

type Encoding string

const (
	EncodingJSON     Encoding = "json"
	EncodingProtobuf Encoding = "protobuf"
)

type Message struct {
	Identifier Identifier
	Payload    []byte
	Encoding   Encoding
}
