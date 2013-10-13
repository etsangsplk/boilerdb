package db

import (
	"bytes"
	"fmt"
	"strings"
)

// Command struct is a command, its key, and its args
type Command struct {
	Command string
	Key     string
	Args    [][]byte
}

// NewCommand creates a new command object
func NewCommand(cmd, key string, args ...[]byte) *Command {
	return &Command{cmd, key, args}
}

// HasArg checks if a command has an argument (e.g. "WITHSCORES").
// case insensitive
func (cmd *Command) HasArg(s string) bool {
	for i := range cmd.Args {
		if s == strings.ToUpper(string(cmd.Args[i])) {
			return true
		}
	}

	return false

}

// ToString converts a Command to a string
func (cmd *Command) ToString() string {
	keyStr := ""
	argStr := ""

	if cmd.Key != "" {
		keyStr = fmt.Sprintf(" \"%s\"", cmd.Key)
	}
	if len(cmd.Args) > 0 {
		argStr = fmt.Sprintf(" \"%s\"", bytes.Join(cmd.Args, []byte("\" \"")))
	}

	return fmt.Sprintf("%s%s%s", cmd.Command, keyStr, argStr)
}
