package utility

import (
	"myDb/types"
	"os"
	"regexp"
)

func FlattenWhitespaces(input string) string {
	whitespacePattern := regexp.MustCompile(`\s+`)
	res := whitespacePattern.ReplaceAllString(input, " ")
	return res
}

func RemoveWhitespaces(input string) string {
	whitespacePattern := regexp.MustCompile(`\s+`)
	res := whitespacePattern.ReplaceAllString(input, "")
	return res
}

func CreateFileIfNotExists(filename string) {
	_, err := os.Stat(filename)
	if os.IsNotExist(err) {
		file, err := os.Create(filename)
		if err != nil {
			panic(err) // Handle error appropriately
		}
		file.Close()
	}
}

func ProcessInsertion(values []map[string]string, table *types.Relation, rle *types.RelationListElement) ([][]types.FieldValue, error) {
	result := make([][]types.FieldValue, len(values))
	for i := range result {
		result[i] = make([]types.FieldValue, len(table.Fields))
	}

	fieldIndexMap := make(map[string]int)
	for i, field := range table.Fields {
		fieldIndexMap[field.Name] = i
	}

	for rowIndex, valueEntry := range values {
		for nameKey, value := range valueEntry {
			fieldIndex, ok := fieldIndexMap[nameKey]
			if !ok {
				continue
			}

			field := table.Fields[fieldIndex]
			fieldType := rle.GetFieldTypeById(field.FieldId).Type

			result[rowIndex][fieldIndex].ID = int(field.FieldId)
			result[rowIndex][fieldIndex].ValueType = types.DbType(fieldType)

			err := types.ParseFieldValue(&result[rowIndex][fieldIndex], value)
			if err != nil {
				return nil, err
			}
		}
	}

	return result, nil
}
