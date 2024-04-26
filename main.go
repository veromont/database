package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"task1/parser"
	"task1/procedures"
	SysCatalog "task1/system_catalog"
)

func main() {
	// process some commands
	listCommands()
	launchProgram()

}

func acceptUserInput(message string) (string, error) {
	fmt.Print(message)
	var line string
	scanner := bufio.NewScanner(os.Stdin)
	if scanner.Scan() {
		line = scanner.Text()
		fmt.Printf("Input was: %q\n", line)
	}

	return line, nil
}

func launchProgram() {
	for {
		input, err := acceptUserInput(">")
		if err != nil {
			continue
		}
		args := strings.Split(input, " ")
		command := strings.ToLower(args[0])

		switch command {
		case "list":
			fmt.Println("Selected: List items")
			listCommands()
		case "create":
			if !isUsageCorrect(command, args) {
				fmt.Printf("Usage of command %s is: %s\n", command, commandsUsageExample[command])
				continue
			}
			filename := args[2]
			object := strings.ToLower(args[1])

			switch object {
			case "dataset":
				createDataset(filename)
			case "relation":
				createRelation(filename)
			}
			fmt.Printf("\nSelected: Execute %s query for %s file with path '%s'\n", command, object, filename)

		case "save":
			if !isUsageCorrect(command, args) {
				fmt.Printf("Usage of command %s is: %s\n", command, commandsUsageExample[command])
				continue
			}
			filename := args[2]
			object := strings.ToLower(args[1])

			switch object {
			case "datasets":
				procedures.SaveAllDatasetsBin(SysCatalog.Datasets, filename)
			case "relations":
				procedures.SaveAllRelationsBin(SysCatalog.Relations, filename)
			}
			fmt.Printf("Selected: Execute %s query for %s file with path '%s'\n", command, object, filename)

		case "print":
			if !isUsageCorrect(command, args) {
				fmt.Printf("Usage of command %s is: %s\n", command, commandsUsageExample[command])
				continue
			}
			object := strings.ToLower(args[1])

			switch object {
			case "datasets":
				if len(SysCatalog.Datasets) == 0 {
					fmt.Println("No datasets in memory yet, need to create or load some")
					continue
				}
				for _, dataset := range SysCatalog.Datasets {
					fmt.Print(dataset.ToString("\n"))
				}
			case "relations":
				if len(SysCatalog.Relations) == 0 {
					fmt.Println("No relations in memory yet, need to create or load some")
					continue
				}
				for _, relation := range SysCatalog.Relations {
					fmt.Print(relation.ToString("\n"))
				}
			}
			fmt.Printf("Selected: Execute %s query for %s\n", command, object)

		case "load":
			if !isUsageCorrect(command, args) {
				fmt.Printf("Usage of command %s is: %s\n", command, commandsUsageExample[command])
				continue
			}
			object := strings.ToLower(args[1])
			filename := args[2]

			switch object {
			case "datasets":
				SysCatalog.Relations = procedures.LoadRelationListElements(filename)
			case "relations":
				SysCatalog.Datasets = procedures.LoadDatasets(filename)
			}
			fmt.Printf("Selected: Execute %s query for %s file with path '%s'\n", command, object, filename)

		case "exit":
			fmt.Println("Selected: Exit the program")
			os.Exit(0)

		default:
			fmt.Println("Invalid choice. Please enter a valid option.")
		}
	}
}

func listCommands() {
	fmt.Println("LIST. List commands")
	fmt.Println("SAVE DATASETS|RELATIONS {FILENAME}. save objects to a given file")
	fmt.Println("PRINT DATASETS|RELATIONS. print all relations")
	fmt.Println("LOAD DATASETS|RELATIONS {FILENAME}. load all datasets from file")
	fmt.Println("CREATE DATASET|RELATION {FILENAME}")
	fmt.Println("EXIT. Exit the program")
}

func isUsageCorrect(command string, args []string) bool {
	return len(args) >= expectedArguments[command]
}

// TODO: turn into objects
var commandsUsageExample = map[string]string{
	"save":   "SAVE DATASETS|RELATIONS {FILENAME}",
	"print":  "PRINT DATASETS|RELATIONS",
	"load":   "LOAD DATASETS|RELATIONS {FILENAME}",
	"create": "CREATE DATASET|RELATION {FILENAME}",
}

var expectedArguments = map[string]int{
	"save":   3,
	"print":  2,
	"load":   2,
	"create": 3,
}

func createRelation(filename string) {
	query, err := os.ReadFile(filename)
	if err != nil {
		fmt.Print(err.Error())
		return
	}
	elem, err := parser.ParseCreateRelationQuery(string(query))
	if err != nil {
		fmt.Print(err.Error())
		return
	}

	name := elem.Relations[0].Name
	if SysCatalog.GetRelationByName(name) != nil {
		fmt.Printf("Relation with name %s already exists", name)
		return
	}
	SysCatalog.Relations = append(SysCatalog.Relations, *elem)
	fmt.Println("Relation was successfully created")
}

func createDataset(filename string) {
	query, err := os.ReadFile(filename)
	if err != nil {
		fmt.Print(err.Error())
		return
	}
	elem, err := parser.ParseCreateDatasetQuery(string(query))
	if err != nil {
		fmt.Print(err.Error())
		return
	}

	name := elem.Name
	if SysCatalog.GetDatasetByName(name) != nil {
		fmt.Printf("Dataset with name %s already exists", name)
		return
	}

	SysCatalog.Datasets = append(SysCatalog.Datasets, *elem)
	fmt.Println("Dataset was successfully created")
}
