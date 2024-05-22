package procedures

import (
	"fmt"
	"math/rand"
	"myDb/types"
	"myDb/utility"
	"testing"
)

func generateField() (types.FieldType, types.FieldName) {
	id := rand.Int() % 32
	fieldName := types.FieldName{FieldId: int32(id), Name: "FieldNameVarchar8", Key: 'N'}
	fieldType := types.FieldType{FieldId: int32(id), Type: types.Varchar_t, Size: 8}
	return fieldType, fieldName
}

func generateDummyRelation(fields []types.FieldName) types.Relation {
	var relation = types.NewRelation()
	i := rand.Int() % 13
	relation.Name = "relationName" + fmt.Sprint(i)
	relation.DataFileName = "dataFileName" + fmt.Sprint(i)
	relation.Fields = fields
	return *relation
}

// TODO: test
func TestRelationCreationAndReading(t *testing.T) {
	var relationListElements = make([]types.RelationListElement, 0)
	var relations = make([]types.Relation, 0)

	ft, fn := generateField()
	ft1, fn1 := generateField()
	fieldNames := make([]types.FieldName, 0)
	fieldTypes := make([]types.FieldType, 0)
	fieldNames = append(fieldNames, fn, fn1)
	fieldTypes = append(fieldTypes, ft, ft1)
	relations = append(relations, generateDummyRelation(fieldNames))
	relations = append(relations, generateDummyRelation(fieldNames))

	var element types.RelationListElement

	element.Relations = relations
	element.Type = types.RelationType{Size: 4, Fields: fieldTypes}
	element.Type.Id = 17
	relationListElements = append(relationListElements, element)
	utility.CreateFileIfNotExists("relations_test.bin")
	utility.CreateFileIfNotExists("relations_test.txt")

	el2 := element.Copy()
	el2.Type.Id = 3
	el3 := element.Copy()
	el3.Type.Id = 5
	relationListElements = append(relationListElements, *el2)
	relationListElements = append(relationListElements, *el3)

	saveAllRelationsTxt(relationListElements, "relations_test.txt")
	SaveAllRelationsBin(relationListElements, "relations_test.bin")

	res := LoadRelationListElements("relations_test.bin")
	if len(res) != len(relationListElements) {
		t.Fail()
	}
	if res[1].Type.Id != el2.Type.Id {
		t.Fail()
	}
}

func TestDatasetCreationAndReading(t *testing.T) {
	utility.CreateFileIfNotExists("datasets_test.bin")

	//var dsListElements = make([]types.DsListElement, 0)
	//types.TableInfo
	//var dsListElem0 = {}
	//dsListElements = append(dsListElements, )
}
