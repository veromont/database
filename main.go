package main

import (
	"bufio"
	"fmt"
	CLI "myDb/command_line_interface"
	"myDb/params"
	"myDb/parser"
	"myDb/procedures"
	SysCatalog "myDb/system_catalog"
	"os"
	"strings"
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
		command = strings.ToLower(command)
		object = strings.ToLower(object)
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
			filename = params.WorkDir + "\\" + filename
			switch object {
			case "dataset":
				createDataset(filename)
			case "relation":
				createRelation(filename)
			}
			fmt.Printf("\nSelected: Execute %s query for %s file with path '%s'\n", command, object, filename)

		case "save":
			filename = params.SaveDir + "\\" + filename
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
			filename = params.SaveDir + "\\" + filename
			switch object {
			case "datasets":
				SysCatalog.Relations = procedures.LoadRelationListElements(filename)
			case "relations":
				SysCatalog.Datasets = procedures.LoadDatasets(filename)
			}
			fmt.Printf("Selected: Execute %s query for %s file with path '%s'\n", command, object, filename)

		case "set":
			switch object {
			case "workdir":
				setWorkdir(filename)
			case "savedir":
				setSaveDir(filename)
			}
			fmt.Printf("Selected: Execute %s query for %s directory with path '%s'\n", command, object, filename)

		case "exit":
			fmt.Println("Selected: Exit the program")
			os.Exit(0)

		default:
			fmt.Println("Invalid choice. Please enter a valid option.")
		}
	}
}

// TODO: add commands SELECT WORKDIR AND SELECT SAVEDIR
func listCommands() {
	fmt.Println("LIST. List commands")
	fmt.Println("SAVE DATASETS|RELATIONS {FILENAME}. save objects to a given file")
	fmt.Println("PRINT DATASETS|RELATIONS. print all relations")
	fmt.Println("LOAD DATASETS|RELATIONS {FILENAME}. load all datasets from file")
	fmt.Println("CREATE DATASET|RELATION {FILENAME}")
	fmt.Println("SET SAVEDIR|WORKDIR {PATH}")
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

func insertRelation(filename string) {

}

func insertDataset(filename string) {

}

func setWorkdir(pathToDir string) {
	params.WorkDir = pathToDir
}

func setSaveDir(pathToDir string) {
	params.SaveDir = pathToDir
}
