package operations

import (
	"fmt"
	"strings"
)

type commandDef struct {
	Name      string
	Keywords  []string
	Example   string
	MinArgLen int32
}

var commandRegistry = map[string]commandDef{
	"ProjectR": {
		Name:      "ProjectR",
		Keywords:  []string{"into"},
		Example:   "ProjectR {relation} [{fieldName} {fieldName}] into {resultName}",
		MinArgLen: 4,
	},
	"AddElement": {
		Name:      "AddElement",
		Keywords:  []string{},
		Example:   "AddElement {DS} {pk1} {pk2}",
		MinArgLen: 3,
	},
	"AttachMember": {
		Name:      "AttachMember",
		Keywords:  []string{},
		Example:   "AttachMember {DS} {pk1} {fieldName} {fieldValue}",
		MinArgLen: 4,
	},
	"ComposeDS": {
		Name:      "ComposeDS",
		Keywords:  []string{},
		Example:   "ComposeDS {datasetName} {relation1} {relation2} {fieldname1} {fieldvalue1} {fieldname2} {fieldvalue2}",
		MinArgLen: 7,
	},
	"Reverse": {
		Name:      "Reverse",
		Keywords:  []string{},
		Example:   "Reverse {DS} {DatasetName}",
		MinArgLen: 4,
	},
	"UnionDS": {
		Name:      "UnionDS",
		Keywords:  []string{},
		Example:   "UnionDS {DS1} {DS2} {DatasetName}",
		MinArgLen: 3,
	},
	"IntersectDS": {
		Name:      "IntersectDS",
		Keywords:  []string{},
		Example:   "IntersectDS {DS1} {DS2} {DatasetName}",
		MinArgLen: 3,
	},
	"DeleteDS": {
		Name:      "DeleteDS",
		Keywords:  []string{},
		Example:   "DeleteDS {DS1}",
		MinArgLen: 1,
	},
	"CrossDS": {
		Name:      "CrossDS",
		Keywords:  []string{},
		Example:   "CrossDS {DS1} {DS2} {DatasetName}",
		MinArgLen: 3,
	},
	"ProjectDS": {
		Name:      "ProjectDS",
		Keywords:  []string{},
		Example:   "ProjectDS {DS1} [{AttributeName}]",
		MinArgLen: 3,
	},
	"BFilter": {
		Name:      "BFilter",
		Keywords:  []string{},
		Example:   "BFilter {DS1} {DS2} {DatasetName}",
		MinArgLen: 3,
	},
	"JoinDS": {
		Name:      "JoinDS",
		Keywords:  []string{},
		Example:   "JoinDS {DS1} {DS2} {DatasetName}",
		MinArgLen: 3,
	},
}

func PrintFunc() {
	fmt.Println("Available Commands:")
	for _, cmd := range commandRegistry {
		fmt.Printf("Name     : %s\n", cmd.Name)
		fmt.Printf("Example  : %s\n", cmd.Example)
		fmt.Printf("Min Args : %d\n", cmd.MinArgLen)
		fmt.Println(strings.Repeat("-", 40))
	}
}
