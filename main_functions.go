package main

import (
	"fmt"

	"myDb/operations"
	"myDb/params"
	"myDb/parser"
	recording "myDb/records"
	SysCatalog "myDb/system_catalog"
	"myDb/types"
	"myDb/utility"
	"os"
)

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
	fmt.Println("SELECT DATASET|RELATION {{OBJECT NAME}. отримати всі записи вказаного об'єкту")
	fmt.Println("EXECUTE {FILENAME}. виконати команду з файлух")
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
	if rle == nil || table == nil {
		return fmt.Errorf("relation %s not found", tableName)
	}
	tuples, err := utility.ProcessInsertion(fieldValues, table, rle)
	if err != nil {
		return err
	}
	recording.Relation = *table
	for _, tuple := range tuples {
		err = recording.WriteRelationRecord(tuple)
		if err != nil {
			return err
		}
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
	slice := kValues[1:]
	newDs.MemberKVs = &slice
	*ds.DatasetElements = append(*ds.DatasetElements, newDs)
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

func executeFile(filename string) {
	err := operations.ProcessExecuteQuery(filename)
	if err != nil {
		fmt.Printf("Виконання файлу '%s' завершено з помилкою", filename)
		fmt.Printf("Помилка: '%s'", err.Error())
	}
}
