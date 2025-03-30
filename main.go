package main

import (
	"bufio"
	"fmt"
	CLI "myDb/command_line_interface"
	"myDb/params"
	"myDb/procedures"
	recording "myDb/records"
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

		command, object, filename, _ := CLI.GetArgumentsFromCommand(input)

		command = strings.ToLower(command)
		object = strings.ToLower(object)
		if !CLI.CommandExists(command) {
			fmt.Printf("Команди %s не існує, спробуйте одну з цих:\n", command)
			listCommands()
		}
		if !CLI.IsUsageCorrect(input) {
			fmt.Printf("Некоректно використано команду %s, правильно так:\n %s\n", command, CLI.Commands[command].Usage)
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
			fmt.Printf("\nВибрано: Виконати %s запит для %s файлу зі шляхом '%s'\n", command, object, filename)

		case "save":
			filename = params.SaveDir + "\\" + filename
			switch object {
			case "datasets":
				procedures.SaveAllDatasetsBin(SysCatalog.Datasets, filename)
			case "relations":
				procedures.SaveAllRelationsBin(SysCatalog.Relations, filename)
			}
			fmt.Printf("\nВибрано: Виконати %s запит для %s файлу зі шляхом '%s'\n", command, object, filename)

		case "print":
			switch object {
			case "datasets":
				if len(SysCatalog.Datasets) == 0 {
					fmt.Printf("Кількість наборів даних: %d\n", len(SysCatalog.Datasets))
					fmt.Println("Жодного набору даних ще не створено, неможливо презентувати")
					continue
				}
				for _, dataset := range SysCatalog.Datasets {
					fmt.Print(dataset.ToString("\n"))
					fmt.Print("\n\n\n")
				}
			case "relations":
				if len(SysCatalog.Relations) == 0 {
					fmt.Println("Жодного набору даних ще не створено, неможливо презентувати")
					continue
				}
				fmt.Printf("Кількість таблиць: %d\n", len(SysCatalog.Relations))
				for _, relation := range SysCatalog.Relations {
					fmt.Print(relation.ToString("\n"))
					fmt.Print("\n\n\n")
				}
			}
			fmt.Printf("\nВибрано: Виконати %s запит для %s \n", command, object)
		case "load":
			filename = params.SaveDir + "\\" + filename
			switch object {
			case "datasets":
				SysCatalog.Datasets = procedures.LoadDatasets(filename)
			case "relations":
				SysCatalog.Relations = procedures.LoadRelationListElements(filename)
			}
			fmt.Printf("\nВибрано: Виконати %s запит для %s файлу зі шляхом '%s'\n", command, object, filename)

		case "insert":
			filename = params.WorkDir + "\\" + filename
			switch object {
			case "dataset":
				err := insertDataset(filename)
				if err == nil {
					fmt.Print("Запис додано успішно")
				} else {
					fmt.Printf("Сталася помилка: %s", err.Error())
				}
			case "relation":
				err := insertRelation(filename)
				if err != nil {
					fmt.Printf("Сталася помилка: %s", err.Error())
				}
			}

		case "delete":
			switch object {
			case "dataset":
				deleteDataset(filename)
			case "relation":
				deleteRelation(filename)
			}

		case "set":
			switch object {
			case "workdir":
				setWorkdir(filename)
			case "savedir":
				setSaveDir(filename)
			}
			fmt.Printf("\nВибрано: Виконати %s запит для %s файлу зі шляхом '%s'\n", command, object, filename)

		case "savedir":
			fmt.Printf("savedir: %s", params.SaveDir)

		case "workdir":
			fmt.Printf("workdir: %s", params.WorkDir)

		case "select":
			switch object {
			case "relation":
				_, relation := SysCatalog.GetRelationByName(filename)
				if relation == nil {
					fmt.Printf("relation " + filename + " not found")
					continue
				}
				recording.Relation = *relation
				recording.GetRecords()

			}
		case "execute":
			filename = params.WorkDir + "\\" + object
			executeFile(filename)
		case "exit":
			fmt.Println("Selected: Exit the program")
			os.Exit(0)

		default:
			fmt.Println("Некоректна команда, спробуйте іншу")
		}
		fmt.Println()
	}
}
