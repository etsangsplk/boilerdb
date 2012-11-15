/**
 * Created with IntelliJ IDEA.
 * User: dvirsky
 * Date: 11/15/12
 * Time: 12:59 AM
 * To change this template use File | Settings | File Templates.
 */
package db

import (
	"fmt"
	"reflect"


)

type Arg string

//the command struct
type Command struct {

	Command string
	Key string
	Args []string

}


type Result struct {
	reflect.Value
}

func NewResult(i interface{})*Result {
	return &Result{reflect.ValueOf(i)}
}


const (

	T_NONE uint32 = 0
	T_STRING uint32 = 1
	T_INTEGER uint32 = 2
	T_LIST uint32 = 4

)


//Dictionary Entry struct
type Entry struct {

	Value interface{}
	Type uint32

}

//Command handler function signature.
//All command handlers should follow it
type HandlerFunc func(*Command, *Entry) *Result

//Wrapper of a function and the command name, used to register plugins and handlers
type CommandDescriptor struct {
	CommandName string
	//used to validate/parse query format. e.g. "<key> <*> <int> [LIMIT <%d> <%d>]" => /(\s) (\s) ([[:num]]+) (LIMIT ([[:num]]+) ([[:num]]+))?
	Format string
	Handler HandlerFunc
	Owner IPlugin
}


//The API for an abstract plugin, that creates data structs and registers handlers
type IPlugin interface {

	CreateObject() *Entry

	GetCommands() []CommandDescriptor

}


type DataBase struct {

	commands map[string]*CommandDescriptor
	dictionary map[string]*Entry

}

func NewDataBase() *DataBase {
	return &DataBase{make(map[string]*CommandDescriptor), make(map[string]*Entry) }
}

func (db *DataBase) registerCommand(cd CommandDescriptor) {
	db.commands[cd.CommandName] = &cd

}


func (db *DataBase) RegisterPlugins(plugins ...IPlugin) {

	totalCommands := 0
	for i := range plugins {
		plugin := plugins[i]
		fmt.Printf("Registering plugin %s\n", plugin)

		commands := plugin.GetCommands()
		for j := range commands {

			totalCommands++
			db.registerCommand(commands[j])
		}
	}
	fmt.Printf("Registered %d plugins and %d commands\n", len(plugins), totalCommands)

}


func (db *DataBase) HandleCommand(key string, cmd *Command) (*Result, error) {

	//get the right command handler for the command
	commandDesc := db.commands[cmd.Command]

	//if this is an unknown command - return error
	if commandDesc == nil {
		return NewResult("Error: Invalid Command"), fmt.Errorf("Could not find suitable command handler for %s", cmd.Command)

	}

	entry := db.dictionary[key]

	if entry == nil {

		entry = commandDesc.Owner.CreateObject()

		if entry != nil {

			db.dictionary[key] = entry
		}

	}

	//fmt.Println("Returning command for obj ", obj)
	return commandDesc.Handler(cmd, entry), nil


}


