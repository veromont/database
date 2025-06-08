package parser

import (
	"fmt"
	"testing"
)

func TestGetRegex(t *testing.T) {
	identifiers, err := getStringsOfRegex(`CREATE RELATION Persons (
		PersonID int,
		LastName varchar(255),
		FirstName varchar(255),
		Address varchar(255),
		City varchar(255)
	);`, identifierRegex)
	if err != nil {
		t.Errorf("Unexpected error: %s", err.Error())
	}
	if len(identifiers) != 13 {
		t.Errorf("Expected 13 identifiers, parser found %d", len(identifiers))
	}
	if len(identifiers) > 6 && identifiers[6] != "varchar" {
		t.Errorf("Expected 13 identifiers, parser found %d", len(identifiers))
	}
}

func TestParseRelationFieldsIntoTokens(t *testing.T) {
	v, err := getFieldsFromQuery(`CREATE RELATION Persons (
		PersonID# int,
		LastName varchar(255),
		FirstName varchar(255),
		Address varchar(255),
		City varchar(255)
	);`)
	if err != nil {
		t.Errorf("unexpected error: %s", err.Error())
	}
	if len(v) != 5 {
		t.Errorf("expected 4 tokens, got %d", len(v))
	}

}

func TestParseCreateQuery(t *testing.T) {
	relation, err := ParseCreateRelationQuery(`CREATE RELATION Persons (
		PersonID# int,
		LastName varchar(255),
		FirstName varchar(255),
		Address varchar(255),
		City varchar(255)
	);`)
	if err != nil {
		t.Errorf("Unexpected error: %s", err.Error())
	}
	r := relation.Relations[0]
	if r.Name != "Persons" {
		t.Errorf("Expected name Persons, got %s", r.Name)
	}
	for _, fieldName := range r.Fields {
		if fieldName.Name == "PersonID#" && fieldName.Key != 'P' {
			t.Errorf("Expected field PersonID# to be a primary key")
		}
	}
	if len(r.Fields) != 5 {
		t.Errorf("Expected number of fields: 5, got %d", len(r.Fields))

		for _, v := range r.Fields {
			fmt.Printf("field: %s, id: %d\n", v.Name, v.FieldId)
		}
	}
}

func TestRemoveBrakets(t *testing.T) {
	s1 := "(Tobacco Roads)"
	s2 := "in the year"
	removeBrackets(&s1)
	removeBrackets(&s2)

	if s1 != "Tobacco Roads" {
		t.Errorf("failed to remove brakets, result: %s", s1)
	}
	if s2 != "in the year" {
		t.Errorf("failed to not remove brakets, result: %s", s2)
	}
}

func TestParseInsertQuery(t *testing.T) {
	// s := `INSERT INTO doc (vic, dic, pic)
	// VALUES (12, "1234", 56 )`
	// tableName, fields, _ := ParseInsertRecordQuery(s)

	// // utility.ProcessInsertion(fields, tableName)

	// if tableName != "doc" {
	// 	//t.Errorf("expected 3 arguments")
	// }
	// if len(fields[0]) != 3 {
	// 	t.Errorf("expected 3 arguments")
	// }
	// s1 := `INSERT INTO doc (vic, dic, pic)
	// VALUES (12, "1234" )`
	// _, _, err := ParseInsertRecordQuery(s1)
	// if err == nil {
	// 	t.Errorf("error expected")
	// }
}
