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
	"log"
	"sync"
	"io"
//	"bufio"

)

type Arg string

//the command struct
type Command struct {

	Command string
	Key string
	Args [][]byte
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

type DataStruct interface {


	Serialize(io.Writer) (int64, error)

	Dserialize(io.Reader, int64) (int64, error)

}

//Dictionary Entry struct
type Entry struct {

	Value DataStruct
	Type uint32


}

//Command handler function signature.
//All command handlers should follow it
type HandlerFunc func(*Command, *Entry) *Result

const (
	CMD_WRITER = 1
	CMD_READER = 2
	)

//Wrapper of a function and the command name, used to register plugins and handlers
type CommandDescriptor struct {
	CommandName string
	//used to validate/parse query format. e.g. "<key> <*> <int> [LIMIT <%d> <%d>]" => /(\s) (\s) ([[:num]]+) (LIMIT ([[:num]]+) ([[:num]]+))?
	Format string
	Handler HandlerFunc
	Owner IPlugin
	ValidTypeMask uint32
	CommandType int


}


//The API for an abstract plugin, that creates data structs and registers handlers
type IPlugin interface {

	CreateObject() *Entry

	GetCommands() []CommandDescriptor

}


type DataBase struct {

	commands map[string]*CommandDescriptor
	dictionary map[string]*Entry
	lockedKeys map[string]*sync.RWMutex
	globalLock sync.Mutex
}

type KeyLock struct {

}


func NewDataBase() *DataBase {
	return &DataBase{
		commands: make(map[string]*CommandDescriptor),
		dictionary: make(map[string]*Entry),
		lockedKeys: make(map[string]*sync.RWMutex),
		globalLock: sync.Mutex{},

	}
}

func (db *DataBase) registerCommand(cd CommandDescriptor) {

	//make sure we don't double register a command
	if db.commands[cd.CommandName] != nil {
		log.Panicf("Cannot register command %s, Already taken by %s", cd.CommandName, db.commands[cd.CommandName].Owner)
	}
	db.commands[cd.CommandName] = &cd

}

//Lock a key for reading/writing
func (db *DataBase)LockKey(key string, mode int) {
	db.globalLock.Lock()
	defer func() { db.globalLock.Unlock() }()

	keyLock := db.lockedKeys[key]
	if keyLock == nil {
		keyLock = &sync.RWMutex{}
		db.lockedKeys[key] = keyLock

	}

	if mode == CMD_READER {
		keyLock.RLock()
	} else {
		keyLock.Lock()
	}
}


func (db *DataBase)UnlockKey(key string, mode int) {

	db.globalLock.Lock()
	defer func() { db.globalLock.Unlock() }()

	keyLock := db.lockedKeys[key]
	if keyLock == nil {
		return

	}

	if mode == CMD_READER {
		keyLock.RUnlock()
	} else {
		keyLock.Unlock()
	}

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


func (db *DataBase) HandleCommand(cmd *Command) (*Result, error) {

	//get the right command handler for the command

	commandDesc := db.commands[cmd.Command]

	//if this is an unknown command - return error
	if commandDesc == nil {
		return NewResult(Error{E_INVALID_COMMAND}), fmt.Errorf("Could not find suitable command handler for %s", cmd.Command)

	}

	if cmd.Key != "" {
		db.globalLock.Lock()

		entry := db.dictionary[cmd.Key]

		//if the entry does not exist - create it
		if entry == nil {

			entry = commandDesc.Owner.CreateObject()
			//if the entry is nil - we do nothing for the tree
			if entry != nil {

				db.dictionary[cmd.Key] = entry
			}
		}
		db.globalLock.Unlock()
		db.LockKey(cmd.Key, commandDesc.CommandType)

		//fmt.Println("Returning command for obj ", obj)

		ret := commandDesc.Handler(cmd, entry)

		db.UnlockKey(cmd.Key, commandDesc.CommandType)

		return ret, nil
	}

	return commandDesc.Handler(cmd, nil), nil
}


