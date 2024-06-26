package parser

import (
	"errors"
	"fmt"
	"myDb/utility"
	"regexp"
	"strings"
)

type Query struct {
	Text string
	Type QueryType
}

type QueryType int32

const (
	CreateQuery_t QueryType = iota
	InsertRecordQuery_t
	InsertDatasetQuery_t
	Misc_t //Error type
)

const (
	createQueryRegex = iota
	insertRecordQueryRegex
	insertDatasetRegex
	identifierRegex
	bracketRegex
	tokenRegex
	greedyTokenRegex
	sizeOfTypeRegex
	fieldValueRegex
)

var actions = `(CREATE|ALTER|DELETE)`
var objects = `(RELATION|DATASET)`

var regexMap = map[int]string{
	createQueryRegex:       `(?im)` + actions + `(\s+)` + objects + `(\s+)[a-zA-Z]\w*(\s+)\((?s).*\)`,
	insertRecordQueryRegex: `INSERT\s+INTO\s+\w+\s*\(.+?\)\s+VALUES\s*\(.+?\)`,
	insertDatasetRegex:     `INSERT\s+INTO\s+\w+\s+OWNER\s*\(\w+\)\s+MEMBER\s*\(\w+(,\s*\w+)*\)`,
	identifierRegex:        `[a-zA-Z]\w*`,
	bracketRegex:           `\((?s).*\)`,
	tokenRegex:             `[a-zA-Z]\w*|\((?s).*?\)`,
	greedyTokenRegex:       `[a-zA-Z]\w*|\((?s).*\)`,
	sizeOfTypeRegex:        `\([1-9]\d*\)`,
	fieldValueRegex:        `\".*?\"`,
}

func isQueryCorrect(query Query) bool {
	strings, err := getStringsOfRegex(query.Text, int(query.Type))
	if err != nil {
		return false
	}
	return len(strings) == 1
}

func getStringsOfRegex(stringLiteral string, regexType int) ([]string, error) {
	regex, ok := regexMap[regexType]
	if !ok {
		return nil, fmt.Errorf("regex type %d doesn`t exist", regexType)
	}
	myRegex := regexp.MustCompile(regex)
	matchedResults := myRegex.FindAllStringSubmatch(stringLiteral, -1)

	var strings []string
	for _, match := range matchedResults {
		strings = append(strings, match[0])
	}

	return strings, nil
}

func removeBrackets(token *string) {
	if len(*token) >= 2 &&
		((*token)[0] == '(' && (*token)[len(*token)-1] == ')' ||
			(*token)[0] == '"' && (*token)[len(*token)-1] == '"') {
		*token = (*token)[1 : len(*token)-1]
	}
}

func getFieldsFromQuery(fullQuery string) ([]string, error) {
	bracketTokens, err := getStringsOfRegex(fullQuery, bracketRegex)
	if err != nil {
		return nil, err
	}
	if len(bracketTokens) == 0 {
		return nil, errors.New("no brackets found")
	}

	bracketToken := bracketTokens[0]
	removeBrackets(&bracketToken)

	bracketToken = utility.FlattenWhitespaces(bracketToken)
	fieldTokens := strings.Split(bracketToken, ",")

	return fieldTokens, nil
}
