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
	//	"db/replication"
	gob "encoding/gob"
	"fmt"
	"io"
	"log"
	"os"
//	"reflect"
	"runtime/debug"
	"strings"
	"sync"
	"time"
	"util"

//	"runtime"

//	"bufio"

)

type Result struct {
	Value interface {}
}

func NewResult(i interface{}) *Result {
	return &Result{i}
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
	Value   DataStruct
	Type    uint32
	saveId  uint8
	expired bool
}

type SerializedEntry struct {
	Bytes []byte
	Len   uint64
	Type  uint32
	Key   string
}

//Command handler function signature.
//All command handlers should follow it
type HandlerFunc func(*Command, *Entry, *Session) *Result

const (
	CMD_WRITER = 1
	CMD_READER = 2
	CMD_SYSTEM = 4
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

////////////////////////////////////////////////////////////
// Command Sink
// Sinks are subscribers to commands sent to the database.
// e.g. a slave or active monitor
////////////////////////////////////////////////////////////
type CommandSink struct {
	Channel     chan *Command
	CommandType int
	lock        sync.Mutex
}

// safely push a command on the channel without risk of blocking on a full sink or pushing to a nil sink
func (s *CommandSink) PushCommand(cmd *Command) {

	s.lock.Lock()
	defer s.lock.Unlock()

	defer func() {
		e := recover()
		if e != nil {
			log.Printf("Error writing to sink: %s", e)

		}
	}()

	to := time.After(1000 * time.Nanosecond)
	select {
	case s.Channel <- cmd:
		break
	case <-to:
		log.Printf("Timeout writing to channel %p", s.Channel)
	}

}

//safely close the sink's channel
func (s *CommandSink) Close() {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.Channel != nil {
		close(s.Channel)
	}

}

////////////////////////////////////////////////////////////
//
// The core database itself
//
////////////////////////////////////////////////////////////


type DataBase struct {

	commands   map[string]*CommandDescriptor
	dictionary map[string]*Entry
	lockedKeys map[string]*KeyLock
	dictLock   sync.Mutex
	saveLock   sync.RWMutex
	types      map[uint32]*IPlugin

	//save status
	currentSaveId        uint8
	BGsaveInProgress     util.AtomicFlag
	LastSaveTime         time.Time
	changesSinceLastSave int64
	//tmp keys for bgsaving
	bgsaveTempKeys map[string]*SerializedEntry

	DataLoadInProgress util.AtomicFlag
	Running            util.AtomicFlag

	//expiring keys
	expiredKeys map[string]time.Time

	//slaves []replication.Slave

	sinkChan     chan *Command
	commandSinks map[string]*CommandSink

	Stats struct {
		//number of current sessions
		ActiveSessions     int
		CommandsProcessed  int64
		TotalSessions      int64
		RegisteredPlugins  int
		RegisteredCommands int
	}
}

var DB *DataBase = nil

type KeyLock struct {
	sync.RWMutex
	refCount int
}

// create the global database and initialize it
func InitGlobalDataBase() *DataBase {
	DB = &DataBase{

		commands:      make(map[string]*CommandDescriptor),
		dictionary:    make(map[string]*Entry),
		lockedKeys:    make(map[string]*KeyLock),
		dictLock:      sync.Mutex{},
		saveLock:      sync.RWMutex{},
		types:         make(map[uint32]*IPlugin),
		currentSaveId: 0,
		commandSinks:  make(map[string]*CommandSink),
		sinkChan:      make(chan *Command, 10),
		expiredKeys:   make(map[string]time.Time),
		LastSaveTime:  time.Now(),
	}
	DB.Running.Set(true)
	//start the goroutines for dispatching to sinks and for dump loading
	log.Printf("Starting side goroutines for global database")
	go DB.multiplexCommandsToSinks()
	go DB.LoadDump()

	//start the side goroutine for auto-saving the databse
	if config.BGSAVE_SECONDS > 0 {
		go DB.autoPersist()
	}
	return DB
}

func (db *DataBase) ShutDown() {
	log.Printf("Shutting down Database...")
	db.Running.Set(false)
}

// return the number of entries in the database
func (db *DataBase) Size() int {
	return len(db.dictionary)
}

// put the database in full lockdown mode for blocking save.
// This waits for all currently executing queries to finish, then locks
func (db *DataBase) Lockdown() {
	db.saveLock.RUnlock()
	db.saveLock.Lock()
}

// release the full lockdown lock of the databse
func (db *DataBase) UNLockdown() {
	db.saveLock.Unlock()
	db.saveLock.RLock()
}

// this goroutine checks for persisting the database
func (db *DataBase) autoPersist() {
	log.Printf("Starting Auto Persist loop...")
	for db.Running.IsSet() {
		now := time.Now()
		log.Printf("Checking persistence... %d changes since %s", DB.changesSinceLastSave, now.Sub(DB.LastSaveTime))
		//if we need to save the DB - let's do it!
		if DB.LastSaveTime.Add(time.Second * time.Duration(config.BGSAVE_SECONDS)).Before(now) {
			if DB.changesSinceLastSave > 0 {
				log.Printf("AutoSaving Database, %d changes in %s", DB.changesSinceLastSave, now.Sub(DB.LastSaveTime))

				go DB.Dump()
			} else {
				log.Print("No need to save the db. no changes...")
			}
		}

		log.Printf("Sleeping for %s", time.Second*time.Duration(config.BGSAVE_SECONDS))
		// sleep until next bgsave check
		time.Sleep(time.Second * time.Duration(config.BGSAVE_SECONDS))

	}
}

func (db *DataBase) SetExpire(key string, entry *Entry, when time.Time) bool {

	entry.expired = true
	db.expiredKeys[key] = when
	log.Printf("Expiring %s at %s", key, when)

	return true
}

//get the mode of a command (writer /reader / system)
//return the mode identifier or 0 if none is found
func (db *DataBase) CommandType(cmd string) int {

	commandDesc := db.commands[cmd]
	if commandDesc != nil {
		return commandDesc.CommandType
	}
	return 0
}

//create and add a sink to the database, returning it to the caller
func (db *DataBase) AddSink(flags int, id string) *CommandSink {

	sink := &CommandSink{
		make(chan *Command, config.SINK_CHANNEL_SIZE),
		flags,
		sync.Mutex{},
	}

	db.dictLock.Lock()
	db.commandSinks[id] = sink
	db.dictLock.Unlock()

	return sink
}

// remove a sink from the database
// does nothing if the sink id is not in the db's dictionary.
// this gets called when every session ends. TODO: optimize this - a session should be marked as a sink
func (db *DataBase) RemoveSink(id string) {

	db.dictLock.Lock()

	defer db.dictLock.Unlock()

	sink := db.commandSinks[id]
	if sink != nil {
		sink.Close()
		delete(db.commandSinks, id)
		log.Printf("Removed sink %s", id)
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

	if mode != CMD_WRITER {
		keyLock.RLock()
	} else {
		keyLock.Lock()
	}
}

//release a key lock. This has to be called explicitly from HandleCommand
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

	if mode != CMD_WRITER {
		keyLock.RUnlock()
	} else {
		keyLock.Unlock()
	}

}

//register the plugins in the database
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
	db.Stats.RegisteredCommands = totalCommands
	db.Stats.RegisteredPlugins = len(plugins)

}

func (db *DataBase) HandleCommand(cmd *Command, session *Session) (*Result, error) {

	db.Stats.CommandsProcessed++
	//lock the save mutex for reading so we won't access it while saving
	db.saveLock.RLock()
	defer db.saveLock.RUnlock()

	if db.DataLoadInProgress.IsSet() {
		return NewResult(&Error{E_LOAD_IN_PROGRESS}), fmt.Errorf("Could not run command - data load in progress")
	}

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

	if commandDesc.CommandType == CMD_WRITER {
		db.changesSinceLastSave++
	}

	var ret *Result = nil
	if cmd.Key != "" {
		db.dictLock.Lock()

		entry := db.dictionary[cmd.Key]

		//check if the entry is expired
		if entry != nil && entry.expired {

			now := time.Now()

			timeout, ok := db.expiredKeys[cmd.Key]

			//this key is expired - delete it so it will be created if needed
			if ok && timeout.Before(now) {

				log.Printf("Expiring key %s", cmd.Key)

				delete(db.dictionary, cmd.Key)
				delete(db.expiredKeys, cmd.Key)
				entry = nil
			}

		}

		//if the entry does not exist - create it
		if entry == nil {

			//we create new entries just on writer functions
			if commandDesc.CommandType == CMD_WRITER {

				entry = commandDesc.Owner.CreateObject()

				//if the entry is nil - we do nothing for the tree
				if entry != nil {
					entry.saveId = db.currentSaveId
					db.dictionary[cmd.Key] = entry
				}
			}

		}
		db.dictLock.Unlock()

		if entry != nil {
			//lock the entry for reading or writing
			db.LockKey(cmd.Key, commandDesc.CommandType)
			defer func() {

				db.UnlockKey(cmd.Key, commandDesc.CommandType)
			}()

			//we need to persist this entry to the temp persist dictionary! it is about to be persisted
			if commandDesc.CommandType == CMD_WRITER &&
				db.BGsaveInProgress.IsSet() && entry.saveId != db.currentSaveId {
				serialized, err := db.serializeEntry(entry, cmd.Key)
				if err == nil {
					db.bgsaveTempKeys[cmd.Key] = serialized
					log.Printf("Temp persisted %s", cmd.Key)
				}

			}

		}
		ret = commandDesc.Handler(cmd, entry, session)

	} else {
		ret = commandDesc.Handler(cmd, nil, session)
	}

	//duplicate the command to all the db's sinks
	if len(db.commandSinks) > 0 {

		db.sinkChan <- cmd
	}

	return ret, nil
}

// If the databse has sinks associated with it, this goroutine replicates all incoming commands to it
func (db *DataBase) multiplexCommandsToSinks() {

	defer func() {
		e := recover()
		if e != nil {
			log.Printf("Error multiplexing to channels: %s", e)
			debug.PrintStack()
		}
	}()
	for {

		cmd := <-db.sinkChan
		for i := range db.commandSinks {

			sink, ok := db.commandSinks[i]
			if ok && (sink.CommandType&db.CommandType(cmd.Command)) != 0 {
				sink.PushCommand(cmd)

			}
		}
	}
}

//serialize a single entry
func (db *DataBase) serializeEntry(entry *Entry, k string) (*SerializedEntry, error) {

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := entry.Value.Serialize(enc)
	if err != nil {
		log.Printf("Could not serialize entry: %s", err)
		return nil, err
	}
	serialized := SerializedEntry{buf.Bytes(), uint64(buf.Len()), entry.Type, k}
	return &serialized, nil

}

//dump the database to disk, in a non blocking mode
func (db *DataBase) Dump() (int64, error) {

	if db.DataLoadInProgress.IsSet() {
		log.Printf("Data Load In Progress!")
		return 0, fmt.Errorf("LOAD in progress")
	}

	currentlySaving := db.BGsaveInProgress.GetSet(true)
	if currentlySaving {
		log.Printf("BGSave in progress")
		return 0, fmt.Errorf("BGSave in progress")
	}

	db.dictLock.Lock()

	db.bgsaveTempKeys = make(map[string]*SerializedEntry)
	saveId := db.currentSaveId
	db.currentSaveId++
	db.dictLock.Unlock()

	startTime := time.Now()

	//make sure we release the save flag
	defer func() { db.BGsaveInProgress.Set(false) }()

	log.Printf("Starting BGSAVE...")
	//open the dump file for writing
	fp, err := os.Create(fmt.Sprintf("%s/%s", config.WORKING_DIRECTORY, "dump.bdb"))
	if err != nil {
		log.Printf("Could not save to file: %s", err)
		return 0, err
	}

	globalEnc := gob.NewEncoder(fp)

	for k := range db.dictionary {

		db.LockKey(k, CMD_WRITER)
		//try to save from temp dict
		tmpSE := db.bgsaveTempKeys[k]
		if tmpSE != nil {
			fmt.Printf("getting temp serialized...")
			globalEnc.Encode(tmpSE)
			delete(db.bgsaveTempKeys, k)
			db.UnlockKey(k, CMD_WRITER)
			continue
		}

		entry := db.dictionary[k]

		//if the save ids do not match - no need to save
		if entry.saveId != saveId {
			log.Printf("Skipping new entry %s", k)
			db.UnlockKey(k, CMD_WRITER)
			continue
		}

		entry.saveId = db.currentSaveId

		serialized, err := db.serializeEntry(entry, k)

		db.UnlockKey(k, CMD_WRITER)
		if err != nil {
			log.Printf("Could not serialize entry %s: %s", entry, err)
			continue
		}
		globalEnc.Encode(serialized)

	}
	//TODO: iterate the temp dictionary for deleted keys

	fp.Close()
	db.changesSinceLastSave = 0
	db.LastSaveTime = time.Now()
	log.Printf("Finished BGSAVE in %s", time.Now().Sub(startTime))
	return 0, nil
}

// Load the database from a dump file, as specified in the config file
// This disallows commands during load
func (db *DataBase) LoadDump() error {

	//make sure atomically no one is currently loading
	currentlyLoading := db.DataLoadInProgress.GetSet(true)
	if currentlyLoading {
		log.Printf("Cannot load dump - already loading dump")
		return fmt.Errorf("Cannot load dump - already loading dump")
	}

	defer func() { db.DataLoadInProgress.Set(false) }()

	fp, err := os.Open(fmt.Sprintf("%s/%s", config.WORKING_DIRECTORY, "dump.bdb"))
	if err != nil {
		log.Printf("Could not load file: %s", err)
		return err
	}

	db.saveLock.RLock()
	defer db.saveLock.RUnlock()

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
