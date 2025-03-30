package recording

import (
	"encoding/json"
	"fmt"
	"io"
	"myDb/params"
	"myDb/types"
	"os"
	"path/filepath"
)

// var RelationListElement types.RelationListElement
var Relation types.Relation

func getFilename() string {
	return filepath.Join(params.SaveDir, Relation.DataFileName)
}

func ensureFileExists() error {
	filename := getFilename()
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		file, err := os.Create(filename)
		if err != nil {
			return fmt.Errorf("error creating file: %w", err)
		}
		file.Close()
	}
	return nil
}

func comparePK(record types.Record, pkValue interface{}) bool {
	for _, field := range record.Fields {
		if field.ID == int(Relation.GetPKField().FieldId) && int(field.Value.(float64)) == pkValue {
			return true
		}
	}
	return false
}

func WriteRelationRecord(objectFieldValues []types.FieldValue) error {
	var records []types.Record

	// Ensure the file exists
	filename := getFilename()
	if err := ensureFileExists(); err != nil {
		return err
	}

	file, err := os.OpenFile(filename, os.O_RDWR, 0666)
	if err != nil {
		return fmt.Errorf("error opening file: %w", err)
	}

	// Read existing records if file is not empty
	file.Seek(0, 0)
	data, err := io.ReadAll(file)
	if err == nil && len(data) > 0 {
		err = json.Unmarshal(data, &records)
		if err != nil {
			return fmt.Errorf("error unmarshaling existing records: %w", err)
		}
	}

	pkField := Relation.GetPKField()
	for _, field := range objectFieldValues {
		if field.ID == int(pkField.FieldId) {
			val, err := GetRecordByPK(field.Value)
			if err != nil {
				return err
			}
			if val != nil {
				return fmt.Errorf("pk %d is already taken", field.Value)
			}
		}
	}

	// Append new record
	records = append(records, types.Record{Fields: objectFieldValues})

	// Convert to JSON
	jsonData, err := json.MarshalIndent(records, "", "  ")
	if err != nil {
		return fmt.Errorf("error marshaling records to JSON: %w", err)
	}

	// Write back to file
	file.Truncate(0)
	file.Seek(0, 0)
	_, err = file.Write(jsonData)
	if err != nil {
		return fmt.Errorf("error writing JSON data: %w", err)
	}

	return nil
}

func GetRecords() ([]types.Record, error) {
	filename := getFilename()

	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %w", err)
	}
	defer file.Close()

	var records []types.Record
	data, err := io.ReadAll(file)
	if err == nil && len(data) > 0 {
		err = json.Unmarshal(data, &records)
		if err != nil {
			return nil, fmt.Errorf("error unmarshaling existing records: %w", err)
		}
	}
	return records, nil
}

func GetRecordByPK(pkValue interface{}) (*types.Record, error) {
	var filename = getFilename()
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %w", err)
	}
	defer file.Close()

	var records []types.Record
	data, err := io.ReadAll(file)
	if err == nil && len(data) > 0 {
		err = json.Unmarshal(data, &records)
		if err != nil {
			return nil, fmt.Errorf("error unmarshaling existing records: %w", err)
		}
	}

	for _, record := range records {
		if comparePK(record, pkValue) {
			return &record, nil
		}
	}
	return nil, nil
}

func DeleteRecordByPK(pkValue interface{}) error {
	filename := getFilename()
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("error opening file: %w", err)
	}
	defer file.Close()

	var records []types.Record
	data, err := io.ReadAll(file)
	if err == nil && len(data) > 0 {
		err = json.Unmarshal(data, &records)
		if err != nil {
			return fmt.Errorf("error unmarshaling existing records: %w", err)
		}
	}

	newRecords := []types.Record{}
	for _, record := range records {
		if !comparePK(record, pkValue) {
			newRecords = append(newRecords, record)
		}
	}

	jsonData, err := json.MarshalIndent(newRecords, "", "  ")
	if err != nil {
		return fmt.Errorf("error marshaling records to JSON: %w", err)
	}

	file, err = os.OpenFile(filename, os.O_RDWR|os.O_TRUNC, 0666)
	if err != nil {
		return fmt.Errorf("error opening file for writing: %w", err)
	}
	defer file.Close()

	_, err = file.Write(jsonData)
	if err != nil {
		return fmt.Errorf("error writing JSON data: %w", err)
	}

	return nil
}

func CheckPKAvailability(filename string, pkValue interface{}) (bool, error) {
	_, err := GetRecordByPK(pkValue)
	if err != nil {
		return true, nil
	}
	return false, nil
}
