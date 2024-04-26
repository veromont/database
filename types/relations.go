package types

import (
	"fmt"
	"sort"
)

type FieldType struct {
	FieldId int32
	Type    DbType
	Size    int32
}

type FieldSign = rune

const (
	PrimaryKey FieldSign = 'P'
	ForeignKey FieldSign = 'F'
	Nothing    FieldSign = 'N'
)

type KeyType = rune
const (
	P KeyType = 'P'
	F KeyType = 'F'
	N KeyType = 'N'
)
type FieldName struct {
	FieldId int32
	Name    string
	Key KeyType
}

type RelationType struct {
	Id int32
	Size   int32
	Fields []FieldType
}

type Relation struct {
	Name         string
	RecordsCount int32
	Fields       []FieldName
	DataFileName string
}

type RelationListElement struct {
	Type      RelationType
	Relations []Relation
}


func (relListElem *RelationListElement) ToString(delimeter string) string {
	result := ""
	result += "id: " + string(relListElem.Type.Id) + delimeter
	result += "relation list element type size: " + fmt.Sprint(relListElem.Type.Size) + delimeter
	
	result += "number of fields: " + fmt.Sprint(len(relListElem.Type.Fields)) + delimeter
	for _, field := range(relListElem.Type.Fields) {
		result += "field id: " +  fmt.Sprint(field.FieldId) + delimeter
		result += "field type: " + string(field.Type) + delimeter
		result += "field size: " + fmt.Sprint(field.Size) + delimeter
	}
	
	result += fmt.Sprint(len(relListElem.Relations)) + delimeter
	for _, table := range relListElem.Relations {
		result += table.ToString(delimeter) + delimeter
	}
	return result
}

func (relation *Relation) ToString(delimeter string) string {
	result := ""
	result += "relation name length: " + fmt.Sprint(len(relation.Name)) + delimeter
	result += "relation name: " + relation.Name + delimeter
	result += "data file name length: " + fmt.Sprint(len(relation.DataFileName)) + delimeter
	result += "data file name" + relation.DataFileName + delimeter
	result += "records count" + fmt.Sprint(relation.RecordsCount) + delimeter
	result += "number of fields: " + fmt.Sprint(len(relation.Fields)) + delimeter
	for _, field := range relation.Fields {
		result += "field id: " + fmt.Sprint(field.FieldId)	+ delimeter
		result += "field name length: " + fmt.Sprint(len(field.Name)) + delimeter
		result += "field name: " + field.Name + delimeter
		result += "field key type: " + string(field.Key) + delimeter
	}

	return result
}

func (relType *RelationType) SortFields() {
	sort.Slice(relType.Fields, func(i, j int) bool {
		return relType.Fields[i].Size < relType.Fields[j].Size
	})
}

func NewRelation() *Relation {
	return &Relation{Name: "", Fields: make([]FieldName, 0), DataFileName: ""}
}

func NewFieldName() *FieldName {
	return &FieldName{FieldId: -1, Name: "NO_NAME_ASSIGNED"}
}

func NewFieldType() *FieldType {
	return &FieldType{FieldId: -1, Type: NO_TYPE, Size: -1}
}

func NewRelationListElement() *RelationListElement {
	return &RelationListElement{Type: *NewRelationListElementType(), Relations: make([]Relation, 0)}
}

func NewRelationListElementType() *RelationType {
	return &RelationType{Id:-1, Size: 0, Fields: make([]FieldType, 0)}
}

func (relListElem *RelationListElement) Copy() *RelationListElement {
	return &RelationListElement{Type:relListElem.Type, Relations: relListElem.Relations}
}