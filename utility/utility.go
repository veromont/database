package utility

import (
	"regexp"
)

func FlattenWhitespaces(input string) string {
	whitespacePattern := regexp.MustCompile(`\s+`)
	res := whitespacePattern.ReplaceAllString(input, " ")
	return res
}

