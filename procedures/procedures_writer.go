package procedures

import (
	"encoding/binary"
	"io"
	"os"
	"task1/types"
)

func SaveAllRelationsBin(relationListElements []types.RelationListElement, filename string) {
	CreateFileIfNotExist(filename)
	file, err := os.OpenFile(filename, os.O_RDWR, 0666)
	panicError(err)
	defer file.Close()

	var offset int32 = 0
	for i, relListElem := range relationListElements {
		isLast := i == len(relationListElements)-1
		offset = writeRelationToFile(file, offset, relListElem, isLast)
	}
}

func InsertRelation(filePath string) {
	
}

func SaveAllDatasetsBin(dsRecords []types.DsListElement, filename string) {
	CreateFileIfNotExist(filename)
	file, err := os.OpenFile(filename, os.O_RDWR, 0666)
	panicError(err)
	defer file.Close()

	var offset int32 = 0
	for i, dsRecord := range dsRecords {
		isLast := i == len(dsRecords)-1
		offset = writeDSToFile(file, offset, dsRecord, isLast)
	}
}

func InsertDs(filePath string) {

}

func writeRelationToFile(file *os.File, offset int32, relListElem types.RelationListElement, isLast bool) int32 {
	offset64 := int64(offset)
	_, err := file.Seek(offset64, 0)
	panicError(err)

	binary.Write(file, binary.LittleEndian, offset)

	binary.Write(file, binary.LittleEndian, relListElem.Type.Id)
	binary.Write(file, binary.LittleEndian, int32(relListElem.Type.Size))

	binary.Write(file, binary.LittleEndian, int32(len(relListElem.Type.Fields)))
	for _, field := range relListElem.Type.Fields {
		binary.Write(file, binary.LittleEndian, int32(field.FieldId))
		binary.Write(file, binary.LittleEndian, field.Type)
		binary.Write(file, binary.LittleEndian, int32(field.Size))
	}

	binary.Write(file, binary.LittleEndian, int32(len(relListElem.Relations)))
	for _, table := range relListElem.Relations {
		binary.Write(file, binary.LittleEndian, int32(len(table.Name)))
		binary.Write(file, binary.LittleEndian, []byte(table.Name))
		binary.Write(file, binary.LittleEndian, int32(len(table.DataFileName)))
		binary.Write(file, binary.LittleEndian, []byte(table.DataFileName))
		binary.Write(file, binary.LittleEndian, int32(table.RecordsCount))

		binary.Write(file, binary.LittleEndian, int32(len(table.Fields)))
		for _, field := range table.Fields {
			binary.Write(file, binary.LittleEndian, int32(field.FieldId))
			binary.Write(file, binary.LittleEndian, int32(len(field.Name)))
			binary.Write(file, binary.LittleEndian, []byte(field.Name))
			binary.Write(file, binary.LittleEndian, field.Key)
		}

	}
	length, err := file.Seek(0, io.SeekCurrent)
	panicError(err)

	panicError(err)

	newOffset := int32(length)
	if isLast {
		newOffset = -1
	}
	file.Seek(offset64, 0)
	binary.Write(file, binary.LittleEndian, newOffset)
	return newOffset
}

func writeDSToFile(file *os.File, offset int32, dsRecord types.DsListElement, isLast bool) int32 {
	offset64 := int64(offset)
	_, err := file.Seek(offset64, 0)
	panicError(err)

	binary.Write(file, binary.LittleEndian, offset)

	binary.Write(file, binary.LittleEndian, int32(len(dsRecord.Name)))
	binary.Write(file, binary.LittleEndian, []byte(dsRecord.Name))

	binary.Write(file, binary.LittleEndian, int32(len(dsRecord.OwnerTableInfo.Table.Name)))
	binary.Write(file, binary.LittleEndian, []byte(dsRecord.OwnerTableInfo.Table.Name))
	
	binary.Write(file, binary.LittleEndian, int32(len(dsRecord.MemberTableInfo.Table.Name)))
	binary.Write(file, binary.LittleEndian, []byte(dsRecord.MemberTableInfo.Table.Name))

	binary.Write(file, binary.LittleEndian, int32(len(dsRecord.Datasets)))
	// for _, ds := range dsRecord.Datasets {
	// 	binary.Write(file, binary.LittleEndian, int32(len(ds.OwnerKV)))
	// 	binary.Write(file, binary.LittleEndian, []byte(ds.OwnerKV))
	// 	binary.Write(file, binary.LittleEndian, int32(len(ds.MemberKVs)))
	// 	for _, mkv := range ds.MemberKVs {
	// 		binary.Write(file, binary.LittleEndian, int32(len(mkv.RecordKV)))
	// 		binary.Write(file, binary.LittleEndian, []byte(mkv.RecordKV))
	// 	}
	// }

	length, err := file.Seek(0, io.SeekCurrent)
	panicError(err)

	newOffset := int32(length)
	if isLast {
		newOffset = -1
	}
	file.Seek(offset64, 0)
	binary.Write(file, binary.LittleEndian, newOffset)
	return newOffset
}
