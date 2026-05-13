package rabbitmq

import (
	"encoding/json"
	"fmt"
)

// Serializer is the interface for message serialization/deserialization.
type Serializer interface {
	ContentType() string
	Marshal(v interface{}) ([]byte, error)
	Unmarshal(data []byte, v interface{}) error
}

// JSONSerializer implements Serializer using encoding/json.
type JSONSerializer struct{}

func (JSONSerializer) ContentType() string {
	return "application/json"
}

func (JSONSerializer) Marshal(v interface{}) ([]byte, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("rabbitmq: json marshal error: %w", err)
	}
	return data, nil
}

func (JSONSerializer) Unmarshal(data []byte, v interface{}) error {
	if err := json.Unmarshal(data, v); err != nil {
		return fmt.Errorf("rabbitmq: json unmarshal error: %w", err)
	}
	return nil
}

// RawSerializer passes bytes through without transformation.
type RawSerializer struct{}

func (RawSerializer) ContentType() string {
	return "application/octet-stream"
}

func (RawSerializer) Marshal(v interface{}) ([]byte, error) {
	switch val := v.(type) {
	case []byte:
		return val, nil
	case string:
		return []byte(val), nil
	default:
		return nil, fmt.Errorf("rabbitmq: raw serializer expects []byte or string, got %T", v)
	}
}

func (RawSerializer) Unmarshal(data []byte, v interface{}) error {
	switch target := v.(type) {
	case *[]byte:
		*target = data
		return nil
	case *string:
		*target = string(data)
		return nil
	default:
		return fmt.Errorf("rabbitmq: raw deserializer expects *[]byte or *string, got %T", v)
	}
}
