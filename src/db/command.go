
package db

import (
	"strings"
	"fmt"
	"bytes"
)

//the command struct
type Command struct {
	Command string
	Key     string
	Args    [][]byte
}

// Check if a command has an argument (e.g. "WITHSCORES").
// case insensitive
func (cmd *Command) HasArg(s string) bool {
	for i := range cmd.Args {
		if s == strings.ToUpper(string(cmd.Args[i])) {
			return true
		}
	}

	return false

}

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

