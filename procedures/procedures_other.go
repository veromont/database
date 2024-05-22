package procedures

/*
table rows are stored in binary files as follows:
nextRowAddress fieldId nextFieldAddress fieldValue ...
int32          int32   int32            ...
*/

import (
	"encoding/binary"
	"io"
	"myDb/types"
	"os"
	"strconv"
)

// NOTE: i can just use size instead of relListElem

// CALCNUMB PROCEDURE
func CalcRow(table *types.Relation, kv string, relListElem *types.RelationListElement) int32 {

	filename := table.DataFileName

	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	var pkName types.FieldName
	for _, fieldName := range table.Fields {
		if fieldName.Key == types.PrimaryKey {
			pkName = fieldName
		}
	}
	var pkType types.FieldType
	for _, fieldType := range relListElem.Type.Fields {
		if fieldType.FieldId == pkName.FieldId {
			pkType = fieldType
		}
	}

	// this is while true but i keep track of index of element
	BIGNUMBER := 999999999
	for i := 0; i < BIGNUMBER; i++ {
		var nextRowAddress int32
		if nextRowAddress == -1 {
			return -1
		}
		binary.Read(file, binary.LittleEndian, &nextRowAddress)
		var rowKV string
		for j := 0; j < len(table.Fields); j++ {
			var id int32
			binary.Read(file, binary.LittleEndian, &id)

			if id == pkName.FieldId {
				data := make([]byte, pkType.Size)
				file.Read(data)
				rowKV = string(data)
				break
			}

			var nextFieldAddress int32
			binary.Read(file, binary.LittleEndian, &nextFieldAddress)
			file.Seek(int64(nextFieldAddress), io.SeekStart)
		}
		if rowKV == kv {
			return int32(i)
		}
		file.Seek(int64(nextRowAddress), io.SeekStart)
	}
	return -1
}

// for test and demonstration purpose, OUTDATED
func saveAllRelationsTxt(relationListElements []types.RelationListElement, filename string) {
	//open file
	file, err := os.OpenFile(filename, os.O_RDWR, 0666)
	panicError(err)
	defer file.Close()

	const DELIMETER = "\n"
	var offset int = 0
	for _, relationListElement := range relationListElements {
		relationListElementString := relationListElement.ToString(DELIMETER)
		offset += len(relationListElementString) + 4
		relationListElementString = strconv.Itoa(offset) + DELIMETER + relationListElementString
		file.WriteString(relationListElementString)
	}
}

func panicError(err error) {
	if err != nil {
		panic(err)
	}
}
