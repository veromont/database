package main

import (
	"bufio"
	"fmt"
	CLI "myDb/command_line_interface"
	"myDb/params"
	"myDb/parser"
	"myDb/procedures"
	recording "myDb/records"
	SysCatalog "myDb/system_catalog"
	"myDb/types"
	"myDb/utility"
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

		command, object, filename := CLI.GetArgumentsFromCommand(input)
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
				if err == nil {
					fmt.Print("Запис додано успішно")
				} else {
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

		case "exit":
			fmt.Println("Selected: Exit the program")
			os.Exit(0)

		default:
			fmt.Println("Некоректна команда, спробуйте іншу")
		}
		fmt.Println()
	}
}

func listCommands() {
	fmt.Println("LIST. Список команд")
	fmt.Println("SAVE DATASETS|RELATIONS {FILENAME}. зберегти об'єкти у вказаний файл")
	fmt.Println("PRINT DATASETS|RELATIONS. надрукувати всі відношення")
	fmt.Println("LOAD DATASETS|RELATIONS {FILENAME}. завантажити всі набори даних з файлу")
	fmt.Println("CREATE DATASET|RELATION {FILENAME}. створити набір даних або відношення")
	fmt.Println("SET SAVEDIR|WORKDIR {PATH}. встановити директорію для збереження або робочу директорію")
	fmt.Println("SAVEDIR|WORKDIR. показати відповідну встановлену директорію")
	fmt.Println("INSERT RELATION {FILENAME}. вставити відношення з файлу")
	fmt.Println("INSERT DATASET {FILENAME}. вставити набір даних з файлу")
	fmt.Println("EXIT. Вихід з програми")
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
	rle, relation := SysCatalog.GetRelationByName(name)
	if rle != nil || relation != nil {
		fmt.Printf("Таблиця '%s' уже існує", name)
		return
	}
	SysCatalog.Relations = append(SysCatalog.Relations, *elem)
	fmt.Printf("таблицю %s успішно створено, нова кількість таблиць - %d\n", name, len(SysCatalog.Relations))
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
		fmt.Printf("Набір даних під назвою '%s' уже існує\n", name)
		return
	}

	SysCatalog.Datasets = append(SysCatalog.Datasets, *elem)
	fmt.Printf("набір даних '%s' успішно створено, нова кількість наборів даних - %d\n", name, len(SysCatalog.Datasets))
}

func insertRelation(filename string) error {
	query, err := os.ReadFile(filename)
	if err != nil {
		return err
	}
	tableName, fieldValues, err := parser.ParseInsertRecordQuery(string(query))
	if err != nil {
		return err
	}
	rle, table := SysCatalog.GetRelationByName(tableName)
	tuples, err := utility.ProcessInsertion(fieldValues, table, rle)
	if err != nil {
		return err
	}
	filename = params.SaveDir + "\\" + table.DataFileName
	file, err := os.OpenFile(filename, os.O_RDWR, 0666)
	if err != nil {
		fmt.Println(err.Error())
		return err
	}
	for _, tuple := range tuples {
		recording.WriteRelationRecord(file, tuple, -1)
	}
	fmt.Printf("Додано %d записів\n", len(tuples))
	return nil
}

func insertDataset(filename string) error {
	query, err := os.ReadFile(filename)
	if err != nil {
		return err
	}
	dsName, kValues, err := parser.ParseInsertDatasetQuery(string(query))
	if err != nil {
		return err
	}
	ds := SysCatalog.GetDatasetByName(dsName)
	newDs := *types.NewDataset()
	newDs.OwnerKV = kValues[0]
	newDs.MemberKVs = kValues[1:]
	ds.Datasets = append(ds.Datasets, newDs)
	return nil
}

func deleteRelation(name string) {
	err := SysCatalog.DeleteRelationByName(name)
	if err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Printf("таблицю %s видалено успішно, нова кількість таблиць: %d", name, len(SysCatalog.Relations))
	}
}

func deleteDataset(name string) {
	err := SysCatalog.DeleteDatasetByName(name)
	if err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Printf("набір даних %s видалено успішно, нова кількість наборів даних: %d", name, len(SysCatalog.Relations))
	}
}

func setWorkdir(pathToDir string) {
	params.WorkDir = pathToDir
}

func setSaveDir(pathToDir string) {
	params.SaveDir = pathToDir
}
