package CLI

import "strings"

type Command struct {
	Usage       string
	minArgCount int
}

const DefaultDatasetFilename = "system_catalog\\datasets.bin"
const DefaultRelationFilename = "system_catalog\\relations.bin"

var Commands = map[string]Command{
	"save": {
		Usage:       "SAVE DATASETS|RELATIONS [<FILENAME>]",
		minArgCount: 2,
	},
	"print": {
		Usage:       "PRINT DATASETS|RELATIONS",
		minArgCount: 2,
	},
	"load": {
		Usage:       "LOAD DATASETS|RELATIONS [<FILENAME>]",
		minArgCount: 2,
	},
	"create": {
		Usage:       "CREATE DATASET|RELATION <FILENAME>",
		minArgCount: 3,
	},
	"delete": {
		Usage:       "DELETE DATASET|RELATION <OBJECT NAME>",
		minArgCount: 3,
	},
	"list": {
		Usage:       "LIST",
		minArgCount: 1,
	},
	"set": {
		Usage:       "SET WORKDIR|SAVEDIR <PATH>",
		minArgCount: 3,
	},
	"savedir": {
		Usage:       "SAVEDIR",
		minArgCount: 1,
	},
	"workdir": {
		Usage:       "WORKDIR",
		minArgCount: 1,
	},
	"insert": {
		Usage:       "INSERT DATASET|RELATION <FILENAME>",
		minArgCount: 3,
	},
	"select": {
		Usage:       "SELECT DATASET|RELATION <OBJECT NAME>",
		minArgCount: 3,
	},
	"execute": {
		Usage:       "EXECUTE <FILENAME>",
		minArgCount: 2,
	},
	"listfunc": {
		Usage:       "LISTFUNC",
		minArgCount: 1,
	},
}

func IsUsageCorrect(userInput string) bool {
	args := strings.Split(userInput, " ")
	command := strings.ToLower(args[0])
	return len(args) >= Commands[command].minArgCount
}

func GetArgumentsFromCommand(userInput string) (string, string, string, []string) {
	tokens := strings.Split(userInput, " ")
	command := tokens[0]

	if len(tokens) < 2 {
		return command, "", "", nil
	}

	object := tokens[1]

	var filename string
	if object == "datasets" || object == "dataset" {
		filename = DefaultDatasetFilename
	} else if object == "relation" || object == "relations" {
		filename = DefaultRelationFilename
	} else {
		filename = ""
	}

	if len(tokens) > 2 {
		filename = tokens[2]
	}

	var args []string = nil
	if len(tokens) > 3 {
		args = tokens[3:]
	}

	return command, object, filename, args
}

func CommandExists(command string) bool {
	for key := range Commands {
		if key == command {
			return true
		}
	}
	return false
}
