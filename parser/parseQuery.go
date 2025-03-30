package parser

import (
	"fmt"
	SysCatalog "myDb/system_catalog"
	"myDb/types"
	"myDb/utility"
	"strconv"
	"strings"
)

/*
<Створення таблиці> ::= CREATE RELATION <Ім’я таблиці> (

	{ <ім'я поля> тип поля, }

)
якщо поле primary key в кінець ім'я додається #. В таблиці лише 1 primary key

<Створення кортежу> ::= INSERT INTO <Ім’я таблиці> ({ <ім'я поля>, })
VALUES ({ <значення поля>, })
*/

/*
INSERT INTO DS_NAME OWNER(KV) MEMBER(KV, KV, KV ...)
*/
func ParseInsertDatasetQuery(insertQuery string) (string, []string, error) {
	if !isQueryCorrect(Query{Text: insertQuery, Type: InsertDatasetQuery_t}) {
		return "", nil, fmt.Errorf("query '%s' is incorrect", insertQuery)
	}
	tokens, err := getStringsOfRegex(insertQuery, tokenRegex)
	if err != nil || len(tokens) < 6 {
		return "", nil, fmt.Errorf("error when parsing query '%s'", insertQuery)
	}
	dsName := tokens[2]
	removeBrackets(&tokens[4])
	removeBrackets(&tokens[6])
	ownerKv := tokens[4]

	mkvList := utility.RemoveWhitespaces(tokens[6])
	mkvs := strings.Split(mkvList, ",")

	kValues := make([]string, len(mkvs)+1)
	kValues[0] = ownerKv
	for i, mkv := range mkvs {
		kValues[i+1] = mkv
	}

	return dsName, kValues, nil
}

func ParseInsertRecordQuery(insertQuery string) (string, []map[string]string, error) {
	// if !isQueryCorrect(Query{Text: insertQuery, Type: InsertRecordQuery_t}) {
	// 	return "", nil, fmt.Errorf("query '%s' is incorrect", insertQuery)
	// }
	tokens, err := getStringsOfRegex(insertQuery, tokenRegex)
	if err != nil || len(tokens) < 6 {
		return "", nil, fmt.Errorf("error when parsing query '%s'", insertQuery)
	}

	// INSERT INTO <tablename> => <tablename> index 2
	tableName := tokens[2]

	fieldsBracket := tokens[3]
	removeBrackets(&fieldsBracket)

	fieldNames := strings.Split(utility.RemoveWhitespaces(fieldsBracket), ",")

	var tuples []map[string]string
	for i := 5; i < len(tokens); i++ {
		tuple, err := parseInsertTupleBrackets(fieldNames, tokens[i])
		if err != nil {
			return "", nil, err
		}
		tuples = append(tuples, tuple)
	}
	return tableName, tuples, nil
}

// SyntInsertR PROCEDURE related
func ParseCreateRelationQuery(createQuery string) (*types.RelationListElement, error) {
	if !isQueryCorrect(Query{Text: createQuery, Type: CreateQuery_t}) {
		return nil, fmt.Errorf("query '%s' is incorrect", createQuery)
	}
	relationListElement := types.NewRelationListElement()
	relation := types.NewRelation()

	tokens, err := getStringsOfRegex(createQuery, greedyTokenRegex)
	if err != nil || len(tokens) < 4 {
		return nil, fmt.Errorf("error when parsing query '%s'", createQuery)
	}

	relation.Name = tokens[2]
	fieldTokens, err := getFieldsFromQuery(createQuery)
	if err != nil {
		return nil, err
	}

	for i, fieldToken := range fieldTokens {
		fieldType, fieldName, err := parseFieldToken(fieldToken, int32(i))
		if err != nil {
			return nil, err
		}
		relation.Fields = append(relation.Fields, *fieldName)
		relationListElement.Type.Fields = append(relationListElement.Type.Fields, *fieldType)
	}
	relation.RecordsCount = 0
	relation.DataFileName = relation.Name + "_table.json"

	relationListElement.Relations = append(relationListElement.Relations, *relation)
	return relationListElement, nil
}

/*
<Визначення набору даних> ::= SET <Ім’я набору даних> IS
OWNER [SINGLE] <Ім’я сутності>
MEMBER [SINGLE] <Ім’я сутності>
*/

// SyntInsertDS PROCEDURE related
func ParseCreateDatasetQuery(createQuery string) (*types.Dataset, error) {
	ds := types.Dataset{}
	ds.OwnerTableInfo.IsSingle = false
	ds.MemberTableInfo.IsSingle = false
	tokens, err := getStringsOfRegex(createQuery, identifierRegex)
	if err != nil {
		return nil, err
	}

	var ownerName string
	var memberName string

	for i, token := range tokens {

		if token == "SET" {
			ds.Name = tokens[i+1]
		} else if token == "OWNER" {
			ds.OwnerTableInfo.IsSingle = tokens[i+1] == "SINGLE"
			if ds.OwnerTableInfo.IsSingle {
				i++
			}
			ownerName = tokens[i+1]
			_, ownerTable := SysCatalog.GetRelationByName(ownerName)
			ds.OwnerTableInfo.Table = ownerTable
		} else if token == "MEMBER" {
			ds.MemberTableInfo.IsSingle = tokens[i+1] == "SINGLE"
			if ds.MemberTableInfo.IsSingle {
				i++
			}
			memberName = tokens[i+1]
			_, memberTable := SysCatalog.GetRelationByName(memberName)

			ds.MemberTableInfo.Table = memberTable
		}
		if ds.DatasetElements == nil {
			t := make([]types.DatasetElement, 0)
			ds.DatasetElements = &t
		}
	}

	if ds.OwnerTableInfo.Table == nil {
		return nil, fmt.Errorf("таблицю %s не знайдено", ownerName)
	}
	if ds.MemberTableInfo.Table == nil {
		return nil, fmt.Errorf("таблицю %s не знайдено", memberName)
	}
	return &ds, nil
}

func parseFieldToken(fieldToken string, fieldId int32) (*types.FieldType, *types.FieldName, error) {
	fieldName := types.NewFieldName()
	fieldType := types.NewFieldType()
	fieldName.FieldId = fieldId
	fieldType.FieldId = fieldId
	tokens := strings.Split(fieldToken, " ")
	if tokens[0] == "" {
		tokens[0] = tokens[1]
		tokens[1] = tokens[2]
	}
	fieldName.Name = tokens[0]
	typeString := tokens[1]
	typeName, err := getStringsOfRegex(typeString, identifierRegex)
	typeName0 := typeName[0]
	if err != nil {
		return nil, nil, err
	}
	if fieldName.Name[len(fieldName.Name)-1] == '#' {
		fieldName.Key = 'P'
	} else {
		fieldName.Key = 'N'
	}

	if !strings.Contains(fieldToken, "(") && types.ArrayContains(types.DbTypes[:], typeName0) {
		fieldType.Type = types.DbTypeMap[typeName0]
		fieldType.Size = types.DbTypeDefaultSize[fieldType.Type]

		return fieldType, fieldName, nil
	}

	sizeBraket, _ := getStringsOfRegex(typeString, bracketRegex)

	if strings.Contains(typeName0, "(") && !types.ArrayContains(types.SizeSpecifiedTypes[:], typeName0) {
		return nil, nil, fmt.Errorf("field %s is not size specified", typeName0)
	}
	if !types.ArrayContains(types.DbTypes[:], typeName0) {
		return nil, nil, fmt.Errorf("failed to parse fields, unknown type: %s", typeName0)
	}

	removeBrackets(&sizeBraket[0])
	sizeString := sizeBraket[0]
	size, err := strconv.Atoi(sizeString)
	if err != nil {
		return nil, nil, err
	}

	fieldType.Type = types.DbTypeMap[typeName0]
	fieldType.Size = int32(size)

	return fieldType, fieldName, nil
}

func parseInsertTupleBrackets(fieldNames []string, tupleBracketString string) (map[string]string, error) {
	// tupleBracketString = utility.RemoveWhitespaces(tupleBracketString)
	values, err := getStringsOfRegex(tupleBracketString, fieldValueRegex)
	if err != nil {
		return nil, err
	}

	if len(values) != len(fieldNames) {
		return nil,
			fmt.Errorf("expected number of fields %d, got number of fields %d in string (%s)",
				len(fieldNames), len(values), tupleBracketString)
	}

	var res map[string]string = make(map[string]string)
	for i, v := range values {
		removeBrackets(&v)
		res[fieldNames[i]] = v
	}
	return res, nil
}
