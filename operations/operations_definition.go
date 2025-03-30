package operations

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
	"UnionDS": {
		Name:      "UnionDS",
		Keywords:  []string{},
		Example:   "UnionDS {DS1} {DS2}",
		MinArgLen: 2,
	},
	"IntersectDS": {
		Name:      "IntersectDS",
		Keywords:  []string{},
		Example:   "IntersectDS {DS1} {DS2}",
		MinArgLen: 2,
	},
	"DeleteDS": {
		Name:     "DeleteDS",
		Keywords: []string{},
		Example:  "DeleteDS {DS1} {DS2}",
	},
}
