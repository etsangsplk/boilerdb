// The db is the heart of Boiler - and the only part that is not a plugin.
//
// It contains the main database functionality and the API that plugins need to implement, and the API they can talk to
//
package db

import (
	"bytes"
	"config"
	//	"db/replication"
	gob "encoding/gob"
	"fmt"
	"io"
	"logging"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"util"
)

type Result struct {
	Value interface{}
}

func NewResult(i interface{}) *Result {
	return &Result{i}
}

func ResultOK() *Result {
	return NewResult(NewStatus("OK"))
}
//
//const (
//	T_NONE    uint32 = 0
//	T_STRING  uint32 = 1
//	T_INTEGER uint32 = 2
//	T_LIST    uint32 = 4
//)

type DataStruct interface {
	Serialize(*gob.Encoder) error
}

//Dictionary Entry struct
type Entry struct {
	Value   DataStruct
	TypeId  uint8
	saveId  uint8
	expired bool
}

type SerializedEntry struct {
	Bytes []byte
	Len   uint64
	Type  string
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
	MinArgs int
	MaxArgs int
	Handler HandlerFunc
	Owner   Plugin

	//whether the command is a reader, writer or builtin command
	CommandType int
	Help        string
}

type PluginManifest struct {
	Name        string
	Description string
	Commands    []CommandDescriptor
	Types       []string
}

//The API for an abstract plugin, that creates data structs and registers handlers
type Plugin interface {

	//Create an object of the type the plugin is responsible for.
	//This returns an Entry object, and a string with the textual name of the registered type (e.g. "STRING")
	CreateObject(commandName string) (*Entry, string)

	//Expose a PluginManifest object of the commands and data types this plugin handles
	GetManifest() PluginManifest

	//Init the plugin engine
	Init() error

	//Shutdown the plugin engine
	Shutdown()

	//Deserialize an object, with data as a raw buffer of how we saved that object (usually in GOB format)
	//and dtype as a string with the name of the type
	LoadObject(data []byte, dtype string) *Entry

	//used to identify the plugin as a string, for the %s formatting...
	String() string
}

////////////////////////////////////////////////////////////
// Command Sink
// Sinks are subscribers to commands sent to the database.
// e.g. a replication or active monitor
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
			logging.Error("Error writing to sink: %s", e)

		}
	}()

	timeout := time.After(1000 * time.Nanosecond)
	select {
	case s.Channel <- cmd:
		break
	case <-timeout:
		logging.Debug("Timeout writing to channel %p", s.Channel)
	}

}

//safely close the sink's channel
func (s *CommandSink) Close() {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.Channel != nil {
		close(s.Channel)
		s.Channel = nil
	}

}

////////////////////////////////////////////////////////////
//
// The core database itself
//
type DataBase struct {


	WorkingDirectory string
	//the command registrry
	commands map[string]*CommandDescriptor

	//the main dictionary
	dictionary map[string]*Entry

	//currently locked keys
	lockedKeys map[string]*KeyLock

	//global dictionary lock
	dictLock sync.Mutex

	//global save lock used to stop all running processes when saving a snapshot in blocking mode
	saveLock sync.RWMutex

	// type registry.
	// each type is externally represented as a string, but internally mapped to an id between 0 and 255
	// the order of loading plugins changes the ids but it doesn't matter

	//this is the incremental type id counter that increases with each type registering
	maxTypeId uint8
	// for each type id, we know the "owner" plugin if we need to deserialize or create a new object of that typ
	pluginsByTypeId map[uint8]*Plugin
	// mapping of type names to ids
	typeNamesToIds map[string]uint8
	// mapping of type ids back to names
	typeIdsToNames map[uint8]string

	//save status
	currentSaveId        uint8
	BGsaveInProgress     util.AtomicFlag
	LastSaveTime         time.Time
	changesSinceLastSave int64
	//tmp keys for bgsaving
	bgsaveTempKeys 		 map[string]*SerializedEntry

	DataLoadInProgress util.AtomicFlag
	Running            util.AtomicFlag

	//expiring keys
	expiredKeys map[string]time.Time

	//slaves []replication.Slave

	sinkChan     chan *Command
	commandSinks map[string]*CommandSink

	//the unique state of the database, used for replication mainly
	State struct {
		//master instance id for replication
		//this is used for the data and created when there is no data available
		DataInstanceId	string

		ServerId 		string
		//rolling transaction id. we always start on 0
		LastTransactionId uint64
	}


	Stats struct {
		//number of current sessions
		ActiveSessions     int
		CommandsProcessed  int64
		TotalSessions      int64
		RegisteredPlugins  int
		RegisteredCommands int
	}
}

func(d *DataBase ) initState() {

	d.State.DataInstanceId = util.UniqId()
	d.State.LastTransactionId = 0
	d.State.ServerId = util.UniqId()

	logging.Info("Initialized database state: %s", d.State)

}

func (d *DataBase) IncrementTransactionId() uint64 {


	return atomic.AddUint64(&(d.State.LastTransactionId), 1)
}

var DB *DataBase = nil

//this represents a lock on a key with refernce counting
type KeyLock struct {
	sync.RWMutex
	refCount int
}

// create the global database and initialize it
func InitGlobalDataBase(workingDir string, loadDump bool) *DataBase {
	logging.Info("Starting database in working directory %s", workingDir)
	DB = &DataBase{

		WorkingDirectory: workingDir,
		commands:        make(map[string]*CommandDescriptor),
		dictionary:      make(map[string]*Entry),
		lockedKeys:      make(map[string]*KeyLock),
		dictLock:        sync.Mutex{},
		saveLock:        sync.RWMutex{},
		pluginsByTypeId: make(map[uint8]*Plugin),
		typeIdsToNames:  make(map[uint8]string),
		typeNamesToIds:  make(map[string]uint8),
		maxTypeId:       0,
		currentSaveId:   0,
		commandSinks:    make(map[string]*CommandSink),
		sinkChan:        make(chan *Command, 10),
		expiredKeys:     make(map[string]time.Time),
		LastSaveTime:    time.Now(),
		bgsaveTempKeys:  make(map[string]*SerializedEntry),
	}

	DB.initState()
	DB.Running.Set(true)
	//start the goroutines for dispatching to sinks and for dump loading
	logging.Info("Starting side goroutines for global database")
	go DB.multiplexCommandsToSinks()
	if loadDump {
		go DB.LoadDump()
	}

	//start the side goroutine for auto-saving the databse
	if config.BGSAVE_SECONDS > 0 {
		go DB.autoPersist()
	}
	return DB
}

func (db *DataBase) ShutDown() {
	logging.Info("Shutting down Database...")
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
	logging.Info("Starting Auto Persist loop...")
	for db.Running.IsSet() {
		now := time.Now()
		logging.Info("Checking persistence... %d changes since %s", DB.changesSinceLastSave, now.Sub(DB.LastSaveTime))
		//if we need to save the DB - let's do it!
		if DB.LastSaveTime.Add(time.Second * time.Duration(config.BGSAVE_SECONDS)).Before(now) {
			if DB.changesSinceLastSave > 0 {
				logging.Info("AutoSaving Database, %d changes in %s", DB.changesSinceLastSave, now.Sub(DB.LastSaveTime))

				go DB.Dump()
			} else {
				logging.Info("No need to save the db. no changes...")
			}
		}

		logging.Info("Sleeping for %s", time.Second*time.Duration(config.BGSAVE_SECONDS))
		// sleep until next bgsave check
		time.Sleep(time.Second * time.Duration(config.BGSAVE_SECONDS))

	}
}

func (db *DataBase) SetExpire(key string, entry *Entry, when time.Time) bool {

	entry.expired = true
	db.expiredKeys[key] = when
	logging.Debug("Expiring %s at %s", key, when)

	return true
}

//Delete a key from the database. Return true if it existed, else false
func (db *DataBase) Delete(key string) bool {

	db.LockKey(key, CMD_WRITER)
	defer db.UnlockKey(key, CMD_WRITER)

	db.dictLock.Lock()
	defer db.dictLock.Unlock()

	_, ok := db.dictionary[key]

	delete(db.dictionary, key)
	delete(db.bgsaveTempKeys, key)
	delete(db.expiredKeys, key)

	return ok
}

//get the mode of a command (writer /reader / builtin)
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

	logging.Info("Adding sink for session %s", id)
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
		logging.Info("Removed sink %s", id)
	}

}

func (db *DataBase) registerCommand(cd CommandDescriptor) {

	//make sure we don't double register a command
	if db.commands[cd.CommandName] != nil {
		logging.Panic("Cannot register command %s, Already taken by %s", cd.CommandName, db.commands[cd.CommandName].Owner)
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
		logging.Error("Error: Empty lock! key %s", key)
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

func (db *DataBase) registerType(owner *Plugin, name string) (uint8, error) {

	// lock the dictionary just to make sure no one else is registering a type at the same time.
	// It shouldn't happen as the database is not started yet, but just in case... :)
	db.dictLock.Lock()
	defer db.dictLock.Unlock()

	//check if the type is already registered
	_, ok := db.typeNamesToIds[name]
	if ok {
		logging.Critical("Could not register type %s: It already exists in the database!", name)
		return 0, fmt.Errorf("Could not register type %s: It already exists in the database!")
	}

	//increase the max type id
	db.maxTypeId++
	if db.maxTypeId == 255 {

		logging.Panic("255 types registered! Dude!!!")

	}

	//register the type id to string mapping
	typeId := db.maxTypeId
	db.typeNamesToIds[name] = typeId
	db.typeIdsToNames[typeId] = name

	//register the plugin as the owner of this type
	db.pluginsByTypeId[typeId] = owner

	logging.Info("Registerd plugin %s as the owner of type %s, mapped to id %d", *owner, name, typeId)

	return typeId, nil
}

func (db *DataBase) GetTypeId(typeName string) uint8 {
	return db.typeNamesToIds[typeName]
}

//register the plugins in the database
func (db *DataBase) RegisterPlugins(plugins ...Plugin) error {

	totalCommands := 0
	for i := range plugins {
		plugin := plugins[i]
		logging.Info("Registering plugin %s\n", plugin)

		manifest := plugin.GetManifest()

		err := plugin.Init()
		if err != nil {
			logging.Error("Could not init plugin %s: %s", plugin, err)
			continue
		}
		//register the manifest plugins
		for t := range manifest.Types {

			logging.Info("Registering type %s to plugin %s", manifest.Types[t], plugin)

			_, err := db.registerType(&plugin, manifest.Types[t])
			if err != nil {
				logging.Error("Could not finish registering plugins for types, something is WRONG! go fix your DB")
				return err
			}

		}

		for j := range manifest.Commands {

			manifest.Commands[j].Owner = plugin
			totalCommands++

			db.registerCommand(manifest.Commands[j])

		}



	}
	logging.Info("Registered %d plugins and %d commands\n", len(plugins), totalCommands)
	db.Stats.RegisteredCommands = totalCommands
	db.Stats.RegisteredPlugins = len(plugins)

	return nil

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

	if commandDesc.MaxArgs < len(cmd.Args) {
		return NewResult(&Error{E_TOO_MANY_PARAMS}),
			fmt.Errorf("Expected at most %d params for command %s, got %d",
				commandDesc.MaxArgs,
				cmd.Command,
				len(cmd.Args))

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

				logging.Info("Expiring key %s", cmd.Key)

				delete(db.dictionary, cmd.Key)
				delete(db.expiredKeys, cmd.Key)
				entry = nil
			}

		}

		//if the entry does not exist - create it
		if entry == nil {

			//we create new entries just on writer functions
			if commandDesc.CommandType == CMD_WRITER {

				var typeName string
				entry, typeName = commandDesc.Owner.CreateObject(commandDesc.CommandName)

				//logging.Info("Created a new entry %s with type %s", entry, typeName)
				//if the entry is nil - we do nothing for the tree
				if entry != nil {
					entry.saveId = db.currentSaveId
					entry.TypeId = db.GetTypeId(typeName)
					db.dictionary[cmd.Key] = entry
				}
			}

		}
		db.dictLock.Unlock()

		if entry != nil {
			//lock the entry for reading or writing
			db.LockKey(cmd.Key, commandDesc.CommandType)
			defer db.UnlockKey(cmd.Key, commandDesc.CommandType)

			//we need to persist this entry to the temp persist dictionary! it is about to be persisted
			if commandDesc.CommandType == CMD_WRITER &&
				db.BGsaveInProgress.IsSet() && entry.saveId != db.currentSaveId {
					serialized, err := db.serializeEntry(entry, cmd.Key)
					if err == nil {
						//logging.Info("Temp persisted %s", cmd.Key)
						db.dictLock.Lock()
						db.bgsaveTempKeys[cmd.Key] = serialized
						db.dictLock.Unlock()
					}

			}

		}
		ret = commandDesc.Handler(cmd, entry, session)

	} else {
		ret = commandDesc.Handler(cmd, nil, session)
	}
	if commandDesc.CommandType == CMD_WRITER {
		db.IncrementTransactionId()
		db.changesSinceLastSave++
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
			logging.Error("Error multiplexing to channels: %s", e)
			debug.PrintStack()
		}
	}()
	for {

		cmd := <-db.sinkChan
		for i := range db.commandSinks {

			sink, ok := db.commandSinks[i]
			if ok && (sink.CommandType & db.CommandType(cmd.Command)) != 0 {
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
		logging.Warning("Could not serialize entry: %s", err)
		return nil, err
	}
	//get the string type name
	typeName, ok := db.typeIdsToNames[entry.TypeId]
	if !ok {
		return nil, fmt.Errorf("Could not serialize entry %s - unregistered type id %s", k, entry.TypeId)
	}
	serialized := SerializedEntry{buf.Bytes(), uint64(buf.Len()), typeName, k}
	return &serialized, nil

}

//dump the database to disk, in a non blocking mode
func (db *DataBase) Dump() (error) {

	if db.DataLoadInProgress.IsSet() {
		logging.Debug("Data Load In Progress!")
		return fmt.Errorf("LOAD in progress")
	}

	currentlySaving := db.BGsaveInProgress.GetSet(true)
	if currentlySaving {
		logging.Debug("BGSave in progress")
		return fmt.Errorf("BGSave in progress")
	}

	//make sure we release the save flag
	defer func() { db.BGsaveInProgress.Set(false) }()


	startTime := time.Now()
	logging.Info("Starting BGSAVE...")
	//open the dump file for writing
	fp, err := os.Create(fmt.Sprintf("%s/%s",db.WorkingDirectory, "dump.bdb"))
	if err != nil {
		logging.Error("Could not save to file: %s", err)
		return err
	}
	defer fp.Close()

	ch := make(chan *SerializedEntry)
	globalEnc := gob.NewEncoder(fp)

	err = globalEnc.Encode(db.State)
	if err != nil {
		logging.Panic("Could not save db state: %s", err)
	}
	//this function reads back from the channel each serialized entry, and sends it to the file writer

	var dumpWait sync.WaitGroup

	go func(ch chan *SerializedEntry, wg *sync.WaitGroup) {
		wg.Add(1)
		defer func() {
		   e := recover()
			if e != nil {
				logging.Warning("Error while dumping: %s", e)
				wg.Done()
			}

		}()

		saved := 0
		for se := range ch {

			if se != nil {
				logging.Debug("Read entry %s", se.Key)
				//encode it
				e:= globalEnc.Encode(se)
				if e != nil {
					logging.Warning("Error serializing enty: %s", e)
				}
				saved++
			}   else {
				break
			}
		}

		logging.Info("Finished writing %d objects to encoder", saved)
		wg.Done()
	}(ch, &dumpWait)



	err = db.DumpEntries(ch, false)

	close(ch)
	if err != nil {
	 	logging.Error("Could not dump database: %s", err)
		return err
	}

	dumpWait.Wait()
	db.changesSinceLastSave = 0
	db.LastSaveTime = time.Now()
	logging.Info("Finished BGSAVE in %s", time.Now().Sub(startTime))
	return nil
}


//dump the database to a writer, in a non blocking mode
// NOTE: this function assumes you've done the proper locking elsewhere
func (db *DataBase) DumpEntries(dumpChan chan *SerializedEntry, doLock bool) (error) {

	if doLock {
		if db.DataLoadInProgress.IsSet() {
			logging.Debug("Data Load In Progress!")
			return fmt.Errorf("LOAD in progress")
		}

		currentlySaving := db.BGsaveInProgress.GetSet(true)
		if currentlySaving {
			logging.Debug("BGSave in progress")
			return fmt.Errorf("BGSave in progress")
		}

		//make sure we release the save flag
		defer func() { db.BGsaveInProgress.Set(false) }()
	}
	db.dictLock.Lock()


	saveId := db.currentSaveId
	db.currentSaveId++
	db.dictLock.Unlock()

	startTime := time.Now()


	for k := range db.dictionary {

		db.LockKey(k, CMD_WRITER)
		//try to save from temp dict
		tmpSE := db.bgsaveTempKeys[k]
		if tmpSE != nil {

			dumpChan <- tmpSE
			delete(db.bgsaveTempKeys, k)
			db.UnlockKey(k, CMD_WRITER)
			continue
		}

		entry := db.dictionary[k]

		//if the save ids do not match - no need to save
		if entry.saveId != saveId {
			logging.Debug("Skipping new entry %s", k)
			db.UnlockKey(k, CMD_WRITER)
			continue
		}

		entry.saveId = db.currentSaveId

		serialized, err := db.serializeEntry(entry, k)

		db.UnlockKey(k, CMD_WRITER)
		if err != nil {
			logging.Info("Could not serialize entry %s: %s", entry, err)
			continue
		}
		dumpChan  <- serialized


	}
	//TODO: iterate the temp dictionary for deleted keys

	logging.Info("Finished Dump in %s", time.Now().Sub(startTime))
	return  nil
}

func (db *DataBase) LoadSerializedEntry(se *SerializedEntry) error {


	typeId, ok := db.typeNamesToIds[se.Type]
	if !ok {
		logging.Critical("Could not find a plugin for type %s", typeId)
		return nil
	}
	creator := db.pluginsByTypeId[typeId]
	if creator == nil {
		logging.Panic("Got invalid serializer type %d", se.Type)
	}

	entry := (*creator).LoadObject(se.Bytes, se.Type)
	entry.TypeId = db.GetTypeId(se.Type)

	logging.Debug("Created entry %s, registering", *entry)
	if entry != nil {
		db.dictionary[se.Key] = entry

	}

	return nil
}
// Load the database from a dump file, as specified in the config file
// This disallows commands during load
func (db *DataBase) LoadDump() error {

	//make sure atomically no one is currently loading
	currentlyLoading := db.DataLoadInProgress.GetSet(true)
	if currentlyLoading {
		logging.Debug("Cannot load dump - already loading dump")
		return fmt.Errorf("Cannot load dump - already loading dump")
	}

	defer func() { db.DataLoadInProgress.Set(false) }()

	fp, err := os.Open(fmt.Sprintf("%s/%s", db.WorkingDirectory, "dump.bdb"))
	if err != nil {
		logging.Error("Could not load file: %s", err)
		return err
	}

	db.saveLock.RLock()
	defer db.saveLock.RUnlock()



	dec := gob.NewDecoder(fp)
	var se SerializedEntry

	err = dec.Decode(&(db.State))
	nLoaded := 0

	for err != io.EOF {
		err = dec.Decode(&se)

		if err == nil {
			err = db.LoadSerializedEntry(&se)

			if err == nil {
				nLoaded++
				if nLoaded%10000 == 0 {
					logging.Info("Loaded %d objects from dump", nLoaded)
				}
			}
		}

	}

	logging.Info("Finished dump load. Loaded %d objects from dump", nLoaded)
	return nil
}


