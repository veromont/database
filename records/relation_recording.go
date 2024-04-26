package recording

import (
	"encoding/binary"
	"fmt"
	"os"
	"task1/types"
)

func WriteRelationRecord(file *os.File,  objectFieldValues []types.FieldValue, offset64 int64) {
	file.Seek(offset64, 0)
	for _, field := range objectFieldValues {
		writeField(field, file)
    }
}

func writeField(field types.FieldValue, file *os.File){
	switch field.ValueType {
	case (types.Char_t | types.Varchar_t | types.Binary_t | types.Text_t):
		if v, ok := field.Value.(string); ok {
			binary.Write(file, binary.LittleEndian, int32(len(v)))
			binary.Write(file, binary.LittleEndian, []byte(v))
		} else {
			fmt.Printf("Wrong assumed format error")
			break
		}
	case types.Tinyint_t:
		if v, ok := field.Value.(int8); ok {
			err := binary.Write(file, binary.LittleEndian, v)
			if err != nil {
				panic(err)
			}
		} else {
			fmt.Printf("Wrong assumed format error")
			break
		}
	
	case types.Smallint_t:
		if v, ok := field.Value.(int16); ok {
			err := binary.Write(file, binary.LittleEndian, v)
			if err != nil {
				panic(err)
			}
		} else {
			fmt.Printf("Wrong assumed format error")
			break
		}
	
	case types.Int_t:
		if v, ok := field.Value.(int32); ok {
			err := binary.Write(file, binary.LittleEndian, v)
			if err != nil {
				panic(err)
			}
		} else {
			fmt.Printf("Wrong assumed format error")
			break
		}

	case types.Float_t, types.Double_t:
		if v, ok := field.Value.(float64); ok {
			if err := binary.Write(file, binary.LittleEndian, v); err != nil {
				panic(err)
			}
		} else {
			fmt.Printf("Wrong assumed format error for floating-point types")
			break
		}
	// TODO: implement
	case types.Decimal_t:
	
	case types.Date_t:

	case types.Datetime_t:
	
	case types.Timestamp_t:

	case types.Time_t:

	case types.Year_t:

	
	}
}

func InsertRelationRecord(){

}

func AlterRelationRecord(){

}

func DeleteRelationRecord(){

}
