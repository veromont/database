package operations

import (
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
		return operationAddElement(args[0], args[1], args[2])
	case "DeleteDS":
		operation()
	}
	return nil
}
