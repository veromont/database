package types

import (
	"fmt"
	"strconv"
	"time"
)

// mysql types
type DbType rune

const (
	// String types
	Char_t DbType = iota + 1000
	Varchar_t
	Binary_t
	Text_t
	Blob_t

	// Numeric types
	Int_t
	Tinyint_t
	Smallint_t
	Bigint_t
	Float_t
	Double_t
	Decimal_t

	// Date and time types
	Date_t
	Datetime_t
	Timestamp_t
	Time_t
	Year_t

	NO_TYPE
)

type FieldValue struct {
	ID        int
	ValueType DbType
	Value     interface{}
}

var DbTypes = [...]string{
	// String types
	"char",
	"varchar",
	"binary",
	"text",
	"blob",

	// Numeric types
	"int",
	"tinyint",
	"smallint",
	"bigint",
	"float",
	"double",
	"decimal",

	// Date and time types
	"date",
	"datetime",
	"timestamp",
	"time",
	"year",
}

var SizeSpecifiedTypes = [...]string{
	"char",
	"varchar",
	"binary",
	"text",
}

var DbTypeDefaultSize = map[DbType]int32{
	Char_t:    1,
	Varchar_t: 255,
	Binary_t:  255,
	Text_t:    65535,
	Blob_t:    65535,

	Int_t:      4, // Assuming a 32-bit integer
	Tinyint_t:  1,
	Smallint_t: 2,
	Bigint_t:   8,  // Assuming a 64-bit integer
	Float_t:    4,  // Assuming a 32-bit float
	Double_t:   8,  // Assuming a 64-bit double
	Decimal_t:  16, // Assuming a 128-bit decimal

	Date_t:      3, // Assuming a compact date representation
	Datetime_t:  8, // Assuming a combined date and time representation
	Timestamp_t: 4, // Assuming a 32-bit timestamp
	Time_t:      3, // Assuming a compact time representation
	Year_t:      1, // Assuming a single-byte representation for the year
}

var DbTypeMap = map[string]DbType{
	// String types
	"char":    Char_t,
	"varchar": Varchar_t,
	"binary":  Binary_t,
	"text":    Text_t,
	"blob":    Blob_t,

	// Numeric types
	"int":      Int_t,
	"tinyint":  Tinyint_t,
	"smallint": Smallint_t,
	"bigint":   Bigint_t,
	"float":    Float_t,
	"double":   Double_t,
	"decimal":  Decimal_t,

	// Date and time types
	"date":      Date_t,
	"datetime":  Datetime_t,
	"timestamp": Timestamp_t,
	"time":      Time_t,
	"year":      Year_t,
}

func ArrayContains(array []string, t string) bool {
	for _, a := range array {
		if a == t {
			return true
		}
	}
	return false
}

func ParseFieldValue(fieldValue *FieldValue, value string) error {
	switch fieldValue.ValueType {
	case Int_t, Tinyint_t, Smallint_t:
		parsedValue, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		fieldValue.Value = parsedValue
	case Bigint_t:
		parsedValue, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return err
		}
		fieldValue.Value = parsedValue
	case Float_t:
		parsedValue, err := strconv.ParseFloat(value, 32)
		if err != nil {
			return err
		}
		fieldValue.Value = float32(parsedValue)
	case Double_t:
		parsedValue, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return err
		}
		fieldValue.Value = parsedValue
	case Decimal_t:
		parsedValue, err := strconv.ParseFloat(value, 64) // Simplified for example
		if err != nil {
			return err
		}
		fieldValue.Value = parsedValue
	case Date_t, Datetime_t, Timestamp_t, Time_t:
		parsedValue, err := time.Parse(time.RFC3339, value)
		if err != nil {
			return err
		}
		fieldValue.Value = parsedValue
	case Year_t:
		parsedValue, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		fieldValue.Value = parsedValue
	case Char_t, Varchar_t, Binary_t, Text_t, Blob_t:
		fieldValue.Value = value
	default:
		return fmt.Errorf("unsupported ValueType: %d", fieldValue.ValueType)
	}
	return nil
}
