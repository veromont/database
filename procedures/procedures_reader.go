package procedures

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	SysCatalog "task1/system_catalog"
	"task1/types"
)

func readFixedSizeString(file *os.File, stringLen int32) string {
	stringBytes := make([]byte, stringLen)
	if _, err := io.ReadFull(file, stringBytes); err != nil {
		panicError(err)
	}
	return string(stringBytes)
}

func readInt32(file *os.File) int32 {
	var integer int32
	if err := binary.Read(file, binary.LittleEndian, &integer); err != nil {
		panicError(err)
	}
	return integer
}

func LoadRelationListElements(filename string) []types.RelationListElement {
	file, err := os.Open(filename)
	panicError(err)
	defer file.Close()

	result := make([]types.RelationListElement, 0)
	var next int32
	for next != -1 {
		var relationListElement types.RelationListElement

		next = readInt32(file)
		relationListElement.Type.Id = readInt32(file)
		relationListElement.Type.Size = readInt32(file)

		fieldsCount := readInt32(file)

		relationListElement.Type.Fields = make([]types.FieldType, fieldsCount)
		for i := range relationListElement.Type.Fields {
			relationListElement.Type.Fields[i].FieldId = readInt32(file)
			relationListElement.Type.Fields[i].Type = types.DbType(readInt32(file))
			relationListElement.Type.Fields[i].Size = readInt32(file)
		}

		relationsCount := readInt32(file)
		relationListElement.Relations = make([]types.Relation, relationsCount)
		for i := range relationListElement.Relations {
			readRelation(file, &relationListElement, i)
		}
		result = append(result, relationListElement)
	}
	return result
}

func LoadDatasets(filename string) []types.DsListElement {
	file, err := os.Open(filename)
	if err != nil {
		fmt.Printf("Error encountered: %s", err.Error())
		return nil
	}
	defer file.Close()

	result := make([]types.DsListElement, 0)
	var next int32
	for next != -1 {
		var dsListElem types.DsListElement

		next = readInt32(file)

		nameLen := readInt32(file)
		dsListElem.Name = readFixedSizeString(file, nameLen)

		ownerTableNameLen := readInt32(file)
		ownerTableName := readFixedSizeString(file, ownerTableNameLen)
		ownerRelation := SysCatalog.GetRelationByName(ownerTableName)
		if ownerRelation == nil {
			fmt.Printf("relation with name %s doesn`t exist", ownerTableName)
			return nil
		}

		memberTableNameLen := readInt32(file)
		memberTableName := readFixedSizeString(file, memberTableNameLen)
		memberRelation := SysCatalog.GetRelationByName(memberTableName)
		if memberRelation == nil {
			fmt.Printf("relation with name %s doesn`t exist", memberTableName)
			return nil
		}

		dsListElem.OwnerTableInfo.Table = ownerRelation
		dsListElem.MemberTableInfo.Table = memberRelation
		ownerKeyCount := readInt32(file)
		dsListElem.Datasets = make([]types.Dataset, ownerKeyCount)
		// for i := range dsListElem.Datasets {
		// 	readDataset(file, &dsListElem, i)
		// }

		result = append(result, dsListElem)
	}
	return result
}

// func readDataset(file *os.File, dsListElem *types.DsListElement, i int) {
// 	kvLen := readInt32(file)
// 	dsListElem.Datasets[i].OwnerKV = readFixedSizeString(file, kvLen)

// 	memKvCount := readInt32(file)
// 	dsListElem.Datasets[i].MemberKVs = make([]types.MemberKV, memKvCount)
// 	for j := range dsListElem.Datasets[i].MemberKVs {
// 		mkvLen := readInt32(file)
// 		dsListElem.Datasets[i].MemberKVs[j].KValues = readFixedSizeString(file, mkvLen)
// 	}
// }

func readRelation(file *os.File, relationListElement *types.RelationListElement, i int) {
	nameLength := readInt32(file)
	relationListElement.Relations[i].Name = readFixedSizeString(file, nameLength)

	filenameLen := readInt32(file)
	relationListElement.Relations[i].DataFileName = readFixedSizeString(file, filenameLen)

	relationListElement.Relations[i].RecordsCount = readInt32(file)

	fieldsCount := readInt32(file)

	relationListElement.Relations[i].Fields = make([]types.FieldName, fieldsCount)
	for j := range relationListElement.Relations[i].Fields {
		relationListElement.Relations[i].Fields[j].FieldId = readInt32(file)
		nameLen := readInt32(file)
		relationListElement.Relations[i].Fields[j].Name = readFixedSizeString(file, nameLen)
		relationListElement.Relations[i].Fields[j].Key = readInt32(file)
	}
}
