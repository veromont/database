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

<Створення кортежу> ::= INSERT INTO <Ім’я таблиці> ({ <ім'я поля> тип поля, })
VALUES ({ <значення поля>, })
*/

func ParseInsertDatasetQuery(insertQuery string) {

}

func ParseInsertRecordQuery(insertQuery string) (string, []map[string]string, error) {
	if !isQueryCorrect(Query{Text: insertQuery, Type: InsertQuery_t}) {
		return "", nil, fmt.Errorf("query '%s' is incorrect", insertQuery)
	}
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

	tokens, err := getStringsOfRegex(createQuery, tokenRegex)
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
	relation.DataFileName = relation.Name + "_table"

	relationListElement.Relations = append(relationListElement.Relations, *relation)
	return relationListElement, nil
}

/*
<Визначення набору даних> ::= SET <Ім’я набору даних> IS
OWNER [SINGLE] <Ім’я сутності>
MEMBER [SINGLE] <Ім’я сутності>
*/

// SyntInsertDS PROCEDURE related
func ParseCreateDatasetQuery(createQuery string) (*types.DsListElement, error) {
	ds := types.DsListElement{}
	ds.OwnerTableInfo.IsSingle = false
	ds.MemberTableInfo.IsSingle = false
	tokens, err := getStringsOfRegex(createQuery, identifierRegex)
	if err != nil {
		return nil, err
	}
	i := 1
	if tokens[0] == "" {
		i++
	}
	ds.Name = tokens[i]
	i += 3
	if tokens[i] == "SINGLE" {
		ds.OwnerTableInfo.IsSingle = true
		i++
	}
	ownerTableName := tokens[i]
	i += 2
	if tokens[i] == "SINGLE" {
		ds.MemberTableInfo.IsSingle = true
		i++
	}
	memberTableName := tokens[i]
	_, owner := SysCatalog.GetRelationByName(ownerTableName)
	_, member := SysCatalog.GetRelationByName(memberTableName)
	ds.OwnerTableInfo.Table = owner
	ds.MemberTableInfo.Table = member
	if owner == nil {
		return nil, fmt.Errorf("table with name '%s' wasn`t found", ownerTableName)
	}
	if member == nil {
		return nil, fmt.Errorf("table with name '%s' wasn`t found", ownerTableName)
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
	removeBrackets(&tupleBracketString)
	tupleBracketString = utility.RemoveWhitespaces(tupleBracketString)
	values := strings.Split(tupleBracketString, ",")

	if len(values) != len(fieldNames) {
		return nil,
			fmt.Errorf("expected number of fields %d, got number of fields %d in string (%s)",
				len(values), len(fieldNames), tupleBracketString)
	}

	var res map[string]string = make(map[string]string)
	for i, v := range values {
		res[fieldNames[i]] = v
	}
	return res, nil
}
