/**
 * Created with IntelliJ IDEA.
 * User: dvirsky
 * Date: 11/23/12
 * Time: 2:51 AM
 * To change this template use File | Settings | File Templates.
 */
package db

import (
	"strings"
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

