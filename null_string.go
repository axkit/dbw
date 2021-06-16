package dbw

import (
	"database/sql/driver"
	"fmt"
)

// NullString defines types for nullable varchar column without additional "Valid bool" attributes.
type NullString string

// Value implements interface sql.Valuer
func (ns NullString) Value() (driver.Value, error) {
	if !ns.Valid() {
		return nil, nil
	}

	return []byte(ns), nil
}

// Valid return true if value is not empty or false, if len==0.
func (ns NullString) Valid() bool {
	return len(ns) > 0
}

// Scan implements database/sql Scanner interface.
func (ns *NullString) Scan(value interface{}) error {
	if value == nil {
		*ns = ""
		return nil
	}

	v, ok := value.(string)
	if !ok {
		return fmt.Errorf("Date.Scan: expected dbw.NullString, got %T (%q)", value, value)
	}

	*ns = NullString(v)
	return nil
}

// String returns string representation of value.
func (ns NullString) String() string {
	return string(ns)
}

// UnmarshalJSON implements encoding/json Unmarshaller interface.
func (ns *NullString) UnmarshalJSON(b []byte) error {

	if len(b) == 0 {
		*ns = ""
		return nil
	}

	*ns = NullString(string(b))
	return nil
}

// UnmarshalText implements encoding/text TextUnmarshaller interface.
func (ns *NullString) UnmarshalText(b []byte) error {
	return ns.UnmarshalJSON(b)
}

// MarshalText implements encoding/text TextMarshaller interface.
func (ns NullString) MarshalText(b []byte) ([]byte, error) {
	return ns.MarshalJSON()
}

// MarshalJSON implements encoding/json Marshaller interface.
func (ns NullString) MarshalJSON() ([]byte, error) {
	if !ns.Valid() {
		return nil, nil
	}
	return []byte(ns), nil
}
