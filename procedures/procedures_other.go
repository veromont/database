package procedures

import (
	"os"
	"strconv"
	"task1/types"
)

// ????
func CalcNumb(table *types.Relation, kv string) {
    
}

func CreateFileIfNotExist(filename string) {
	_, err := os.Stat(filename)
	if os.IsNotExist(err) {
		file, err := os.Create(filename)
		if err != nil {
			panic(err) // Handle error appropriately
		}
		file.Close()
	}
}

// for test and demonstration purpose, OUTDATED
func saveAllRelationsTxt(relationListElements []types.RelationListElement, filename string) {
	//open file
	file, err := os.OpenFile(filename, os.O_RDWR, 0666)
	panicError(err)
	defer file.Close()

	const DELIMETER = "\n"
	var offset int = 0
	for _, relationListElement := range relationListElements {
		relationListElementString := relationListElement.ToString(DELIMETER)
		offset += len(relationListElementString) + 4
		relationListElementString = strconv.Itoa(offset) + DELIMETER + relationListElementString
		file.WriteString(relationListElementString)
	}
}

func panicError(err error) {
	if err != nil {
		panic(err)
	}
}
