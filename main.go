package main

import (
	"bufio"
	CLI "database/command_line_interface"
	"database/parser"
	"database/procedures"
	SysCatalog "database/system_catalog"
	"fmt"
	"os"
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
			fmt.Printf("Error encountered: %s", err.Error())
			continue
		}

		command, object, filename := CLI.GetArgumentsFromCommand(input)

		if !CLI.CommandExists(command) {
			fmt.Printf("Command %s is illegal, use something from this:", command)
			listCommands()
		}
		if !CLI.IsUsageCorrect(input) {
			fmt.Printf("Usage of command %s is: %s\n", command, CLI.Commands[command].Usage)
			continue
		}

		switch command {
		case "list":
			fmt.Println("Selected: List items")
			listCommands()
		case "create":
			switch object {
			case "dataset":
				createDataset(filename)
			case "relation":
				createRelation(filename)
			}
			fmt.Printf("\nSelected: Execute %s query for %s file with path '%s'\n", command, object, filename)

		case "save":
			switch object {
			case "datasets":
				procedures.SaveAllDatasetsBin(SysCatalog.Datasets, filename)
			case "relations":
				procedures.SaveAllRelationsBin(SysCatalog.Relations, filename)
			}
			fmt.Printf("Selected: Execute %s query for %s file with path '%s'\n", command, object, filename)

		case "print":
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