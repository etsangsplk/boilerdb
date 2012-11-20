/**
 * Created with IntelliJ IDEA.
 * User: dvirsky
 * Date: 11/15/12
 * Time: 12:59 AM
 * To change this template use File | Settings | File Templates.
 */
package db

import (
	"bytes"
	"config"
	gob "encoding/gob"
	"fmt"
	"io"
	"log"
	"os"
	"reflect"
	"strings"
	"sync"

//	"bufio"

)

//the command struct
type Command struct {
	Command string
	Key     string
	Args    [][]byte
}

func (cmd *Command) HasArg(s string) bool {
	for i := range cmd.Args {
		if s == strings.ToUpper(string(cmd.Args[i])) {
			return true
		}
	}

	return false

}

type Result struct {
	reflect.Value
}

func NewResult(i interface{}) *Result {
	return &Result{reflect.ValueOf(i)}
}

const (
	T_NONE    uint32 = 0
	T_STRING  uint32 = 1
	T_INTEGER uint32 = 2
	T_LIST    uint32 = 4
)

type DataStruct interface {
	Serialize(*gob.Encoder) error
}

//Dictionary Entry struct
type Entry struct {
	Value DataStruct
	Type  uint32
}

type SerializedEntry struct {
	Bytes []byte
	Len   uint64
	Type  uint32
	Key   string
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
	MinArgs       int
	Handler       HandlerFunc
	Owner         IPlugin
	ValidTypeMask uint32
	CommandType   int
}

//The API for an abstract plugin, that creates data structs and registers handlers
type IPlugin interface {
	CreateObject() *Entry

	GetCommands() []CommandDescriptor

	GetTypes() []uint32

	LoadObject([]byte, uint32) *Entry
}

type DataBase struct {
	commands   map[string]*CommandDescriptor
	dictionary map[string]*Entry
	lockedKeys map[string]*KeyLock
	dictLock   sync.Mutex
	saveLock   sync.RWMutex
	types      map[uint32]*IPlugin
}

var DB *DataBase = nil

type KeyLock struct {
	sync.RWMutex
	refCount int
}

func InitGlobalDataBase() *DataBase {
	DB = &DataBase{
		commands:   make(map[string]*CommandDescriptor),
		dictionary: make(map[string]*Entry),
		lockedKeys: make(map[string]*KeyLock),
		dictLock:   sync.Mutex{},
		saveLock:   sync.RWMutex{},
		types:      make(map[uint32]*IPlugin),
	}
	return DB
}

func (db *DataBase) Lockdown() {
	db.saveLock.RUnlock()
	db.saveLock.Lock()
}
func (db *DataBase) UNLockdown() {
	db.saveLock.Unlock()
	db.saveLock.RLock()
}

func (db *DataBase) registerCommand(cd CommandDescriptor) {

	//make sure we don't double register a command
	if db.commands[cd.CommandName] != nil {
		log.Panicf("Cannot register command %s, Already taken by %s", cd.CommandName, db.commands[cd.CommandName].Owner)
	}
	db.commands[cd.CommandName] = &cd

}

//Lock a key for reading/writing
func (db *DataBase) LockKey(key string, mode int) {

	db.dictLock.Lock()

	keyLock := db.lockedKeys[key]
	if keyLock == nil {
		keyLock = &KeyLock{sync.RWMutex{}, 1}
		db.lockedKeys[key] = keyLock

	} else {
		keyLock.refCount++

	}

	db.dictLock.Unlock()

	if mode == CMD_READER {
		keyLock.RLock()
	} else {
		keyLock.Lock()
	}
}

func (db *DataBase) UnlockKey(key string, mode int) {

	db.dictLock.Lock()
	//defer func() { db.dictLock.Unlock() }()

	keyLock := db.lockedKeys[key]
	if keyLock == nil {
		log.Printf("Error: Empty lock! key %s", key)
		return

	}

	keyLock.refCount--
	//delete from the lock dictionary if no one else is holding this
	if keyLock.refCount == 0 {
		db.lockedKeys[key] = nil
	}
	db.dictLock.Unlock()

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

		types := plugin.GetTypes()
		for t := range types {
			log.Printf("Registering type %d to plugin %s", types[t], plugin)
			db.types[types[t]] = &plugin
		}
	}
	fmt.Printf("Registered %d plugins and %d commands\n", len(plugins), totalCommands)

}

func (db *DataBase) HandleCommand(cmd *Command) (*Result, error) {

	//lock the save mutex for reading so we won't access it while saving
	db.saveLock.RLock()
	defer func() { db.saveLock.RUnlock() }()
	//make all commands uppercase
	cmd.Command = strings.ToUpper(cmd.Command)

	//get the right command handler for the command
	commandDesc := db.commands[cmd.Command]

	//if this is an unknown command - return error
	if commandDesc == nil {
		return NewResult(&Error{E_INVALID_COMMAND}), fmt.Errorf("Could not find suitable command handler for %s", cmd.Command)

	}

	//validate a minimum amount of argumens
	if commandDesc.MinArgs > len(cmd.Args) {
		return NewResult(&Error{E_NOT_ENOUGH_PARAMS}),
			fmt.Errorf("Expected at least %d params for command %s, got %d",
				commandDesc.MinArgs,
				cmd.Command,
				len(cmd.Args))

	}

	if cmd.Key != "" {
		db.dictLock.Lock()

		entry := db.dictionary[cmd.Key]

		//if the entry does not exist - create it
		if entry == nil {

			//we create new entries just on writer functions
			if commandDesc.CommandType == CMD_WRITER {

				entry = commandDesc.Owner.CreateObject()
				//if the entry is nil - we do nothing for the tree
				if entry != nil {

					db.dictionary[cmd.Key] = entry
				}
			}

		}
		db.dictLock.Unlock()
		if entry != nil {

			db.LockKey(cmd.Key, commandDesc.CommandType)

			defer func() {

				db.UnlockKey(cmd.Key, commandDesc.CommandType)
			}()
		}
		ret := commandDesc.Handler(cmd, entry)

		return ret, nil
	}

	return commandDesc.Handler(cmd, nil), nil
}

func (db *DataBase) Dump() (int64, error) {

	//open the dump file for writing
	fp, err := os.Create(fmt.Sprintf("%s/%s", config.WORKING_DIRECTORY, "dump.bdb"))
	if err != nil {
		log.Printf("Could not save to file: %s", err)
		return 0, err
	}

	var buf bytes.Buffer

	globalEnc := gob.NewEncoder(fp)

	for k := range db.dictionary {

		enc := gob.NewEncoder(&buf)
		entry := db.dictionary[k]

		err := entry.Value.Serialize(enc)
		if err != nil {
			log.Printf("Could not serialize entry %s: %s", entry, err)
			buf.Truncate(0)
			continue
		}
		//fmt.Printf("Serialzed %s. err: %s\n", entry, err)

		serialized := SerializedEntry{buf.Bytes(), uint64(buf.Len()), entry.Type, k}
		globalEnc.Encode(&serialized)
		buf.Truncate(0)

	}

	fp.Close()
	return 0, nil
}

func (db *DataBase) LoadDump() error {

	fp, err := os.Open(fmt.Sprintf("%s/%s", config.WORKING_DIRECTORY, "dump.bdb"))
	if err != nil {
		log.Printf("Could not load file: %s", err)
		return err
	}
	dec := gob.NewDecoder(fp)
	var se SerializedEntry

	nLoaded := 0
	for err != io.EOF {
		err = dec.Decode(&se)

		if err == nil {

			creator := db.types[se.Type]
			if creator == nil {
				log.Panicf("Got invalid serializer type %d", se.Type)
			}

			entry := (*creator).LoadObject(se.Bytes, se.Type)
			if entry != nil {
				db.dictionary[se.Key] = entry
				nLoaded++
				if nLoaded%1000 == 0 {
					fmt.Println(nLoaded)
				}
			}
		}

	}

	log.Printf("Loaded %d objects from dump", nLoaded)
	return nil
}
