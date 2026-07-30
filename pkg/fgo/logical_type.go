package fgo

import (
	"encoding/json"
	"fmt"
)

// LogicalType is the Fluss 0.9.1 schema JSON type representation. It supports
// parameterized and nested roots without exposing protobuf implementation details.
type LogicalType struct {
	Root      string         `json:"type"`
	Nullable  bool           `json:"nullable,omitempty"`
	Length    int            `json:"length,omitempty"`
	Precision int            `json:"precision,omitempty"`
	Scale     int            `json:"scale,omitempty"`
	Element   *LogicalType   `json:"element_type,omitempty"`
	Key       *LogicalType   `json:"key_type,omitempty"`
	Value     *LogicalType   `json:"value_type,omitempty"`
	Fields    []LogicalField `json:"fields,omitempty"`
}

// LogicalField describes one nested field of a ROW logical type.
type LogicalField struct {
	Name        string      `json:"name"`
	Type        LogicalType `json:"field_type"`
	Description string      `json:"description,omitempty"`
	ID          int         `json:"field_id"`
}

type logicalTypeJSON struct {
	Root      string         `json:"type"`
	Nullable  *bool          `json:"nullable,omitempty"`
	Length    int            `json:"length,omitempty"`
	Precision int            `json:"precision,omitempty"`
	Scale     int            `json:"scale,omitempty"`
	Element   *LogicalType   `json:"element_type,omitempty"`
	Key       *LogicalType   `json:"key_type,omitempty"`
	Value     *LogicalType   `json:"value_type,omitempty"`
	Fields    []LogicalField `json:"fields,omitempty"`
}

// MarshalJSON encodes the Fluss logical-type JSON representation.
func (t LogicalType) MarshalJSON() ([]byte, error) {
	encoded := logicalTypeJSON{
		Root: t.Root, Length: t.Length, Precision: t.Precision, Scale: t.Scale,
		Element: t.Element, Key: t.Key, Value: t.Value, Fields: t.Fields,
	}
	if !t.Nullable {
		encoded.Nullable = new(bool)
	}
	return json.Marshal(encoded)
}

// UnmarshalJSON decodes the Fluss logical-type JSON representation.
func (t *LogicalType) UnmarshalJSON(data []byte) error {
	var encoded logicalTypeJSON
	if err := json.Unmarshal(data, &encoded); err != nil {
		return err
	}
	nullable := true
	if encoded.Nullable != nil {
		nullable = *encoded.Nullable
	}
	*t = LogicalType{
		Root: encoded.Root, Nullable: nullable, Length: encoded.Length,
		Precision: encoded.Precision, Scale: encoded.Scale,
		Element: encoded.Element, Key: encoded.Key, Value: encoded.Value, Fields: encoded.Fields,
	}
	return nil
}

// JSON validates and encodes the logical type.
func (t LogicalType) JSON() ([]byte, error) {
	if err := t.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(t)
}

// ParseLogicalTypeJSON decodes and validates a logical type.
func ParseLogicalTypeJSON(data []byte) (LogicalType, error) {
	var logicalType LogicalType
	if err := json.Unmarshal(data, &logicalType); err != nil {
		return LogicalType{}, fmt.Errorf("%w: %v", ErrInvalidSchema, err)
	}
	if err := logicalType.Validate(); err != nil {
		return LogicalType{}, err
	}
	return logicalType, nil
}

// Validate checks root-specific parameters and nested types.
func (t LogicalType) Validate() error {
	switch t.Root {
	case "BOOLEAN", "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "FLOAT", "DOUBLE", "DATE", "STRING", "BYTES":
		return nil
	case "CHAR", "BINARY":
		return t.validateLength()
	case "DECIMAL":
		return t.validateDecimal()
	case "TIME_WITHOUT_TIME_ZONE", "TIMESTAMP_WITHOUT_TIME_ZONE", "TIMESTAMP_WITH_LOCAL_TIME_ZONE":
		return t.validateTemporal()
	case "ARRAY":
		return t.validateArray()
	case "MAP":
		return t.validateMap()
	case "ROW":
		return t.validateRow()
	default:
		return fmt.Errorf("%w: unsupported logical type %q", ErrInvalidSchema, t.Root)
	}
}

func (t LogicalType) validateLength() error {
	if t.Length < 1 {
		return fmt.Errorf("%w: %s length must be positive", ErrInvalidSchema, t.Root)
	}
	return nil
}

func (t LogicalType) validateDecimal() error {
	if t.Precision < 1 || t.Precision > 38 || t.Scale < 0 || t.Scale > t.Precision {
		return fmt.Errorf("%w: invalid DECIMAL precision or scale", ErrInvalidSchema)
	}
	return nil
}

func (t LogicalType) validateTemporal() error {
	if t.Precision < 0 || t.Precision > 9 {
		return fmt.Errorf("%w: invalid %s precision", ErrInvalidSchema, t.Root)
	}
	return nil
}

func (t LogicalType) validateArray() error {
	if t.Element == nil {
		return fmt.Errorf("%w: ARRAY element type is required", ErrInvalidSchema)
	}
	return t.Element.Validate()
}

func (t LogicalType) validateMap() error {
	if t.Key == nil || t.Value == nil {
		return fmt.Errorf("%w: MAP key and value types are required", ErrInvalidSchema)
	}
	if err := t.Key.Validate(); err != nil {
		return err
	}
	return t.Value.Validate()
}

func (t LogicalType) validateRow() error {
	if len(t.Fields) == 0 {
		return fmt.Errorf("%w: ROW fields are required", ErrInvalidSchema)
	}
	for _, field := range t.Fields {
		if field.Name == "" {
			return fmt.Errorf("%w: ROW field name is required", ErrInvalidSchema)
		}
		if err := field.Type.Validate(); err != nil {
			return err
		}
	}
	return nil
}
