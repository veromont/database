package operations

import (
	"fmt"
	"os"
	"strings"
)

func ReadExecuteQuery(filename string) (string, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return "", err
	}

	content := string(data)
	if len(content) == 0 {
		return "", fmt.Errorf("file is empty")
	}

	return content, nil
}

func ParseExecuteQuery(commandStr string) (string, []string, error) {
	tokens := strings.Fields(commandStr)
	if len(tokens) == 0 {
		return "", nil, fmt.Errorf("no tokens found")
	}

	command := tokens[0]
	args := tokens[1:]

	if command == "" {
		return "", nil, fmt.Errorf("empty command")
	}

	_, ok := commandRegistry[command]
	if !ok {
		return "", nil, fmt.Errorf("unknown command: %s", command)
	}

	return command, args, nil
}

func validateArgs(args []string, command string) error {
	keywords := commandRegistry[command].Keywords
	if len(keywords) == 0 {
		return nil // nothing to validate
	}

	argIndex := 0
	for _, keyword := range keywords {
		found := false
		for argIndex < len(args) {
			if args[argIndex] == keyword {
				found = true
				argIndex++ // move past the matched keyword
				break
			}
			argIndex++
		}
		if !found {
			return fmt.Errorf("missing or misplaced keyword: '%s'", keyword)
		}
	}

	return nil
}
