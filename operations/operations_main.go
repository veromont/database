package operations

import (
	"fmt"
	"strings"
)

func ProcessExecuteQuery(filename string) error {
	query, err := ReadExecuteQuery(filename)
	if err != nil {
		return err
	}

	commands := strings.Split(query, ";")

	for _, rawCmd := range commands {
		rawCmd = strings.TrimSpace(rawCmd)
		if rawCmd == "" {
			continue
		}

		command, args, err := ParseExecuteQuery(rawCmd)
		if err != nil {
			return err
		}

		err = validateArgs(args, command)
		if err != nil {
			return err
		}

		err = ExecuteProcedure(command, args)
		if err != nil {
			return err
		}
	}

	return nil
}

func ExecuteProcedure(command string, args []string) error {
	switch command {
	case "AddElement":
		if len(args) < 3 {
			return fmt.Errorf("AddElement requires 3 arguments: dsName, pk1, pk2")
		}
		return operationAddElement(args[0], args[1], args[2])

	case "DeleteDS":
		if len(args) < 1 {
			return fmt.Errorf("DeleteDS requires 1 argument: dsName")
		}
		return operationDeleteDS(args[0])

	case "UnionDS":
		if len(args) < 3 {
			return fmt.Errorf("UnionDS requires 3 arguments: dsName1, dsName2, newDsName")
		}
		return operationUnionDS(args[0], args[1], args[2])

	case "IntersectDS":
		if len(args) < 3 {
			return fmt.Errorf("IntersectDS requires 3 arguments: dsName1, dsName2, newDsName")
		}
		return operationIntersectDS(args[0], args[1], args[2])

	case "CrossDS":
		if len(args) < 3 {
			return fmt.Errorf("CrossDS requires 3 arguments: dsName1, dsName2, newDsName")
		}
		return operationCrossDS(args[0], args[1], args[2])

	case "Reverse":
		if len(args) < 2 {
			return fmt.Errorf("Reverse requires 2 arguments: dsName, newDsName")
		}
		return operationReverseDS(args[0], args[1])

	case "ComposeDS":
		if len(args) < 7 {
			return fmt.Errorf("ComposeDS requires 7 arguments: datasetName, relation1, relation2, fieldName1, fieldValue1, fieldName2, fieldValue2")
		}
		return operationComposeDS(args[0], args[1], args[2], args[3], args[4], args[5], args[6])

	case "BFilter":
		if len(args) < 3 {
			return fmt.Errorf("BFilter requires 3 arguments: DS1 DS2 NewDatasetName")
		}
		return operationBFilter(args[0], args[1], args[2])

	case "JoinDS":
		if len(args) < 3 {
			return fmt.Errorf("JoinDS requires 3 arguments: DS1 DS2 NewDatasetName")
		}
		return operationJoinDS(args[0], args[1], args[2])
	}

	return nil
}
