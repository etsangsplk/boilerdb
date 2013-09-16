# Boiler DB

## A plugin-based, redis-inspired, in-memory Key-Value Database


Boiler DB is an in memory database that has a few interesting twists:

 1. It is compliant with the redis network protocol (but NOT with the full set of redis commands, although most of redis can be implemented on top of it). Meaning you can work with it from redis-cli.
 
 2. It is inspired by redis and tays in-line with some of its philosophy beyond the syntax principles:
    * It is 100% in memory.
    * It has a central dictionary mapping string keys to objects.
    * You have snapshot-based BGSAVE.
    * Keys can be expired.
    * It has (unfinished) master/slave replication that uses similar methods.

 3. BUT it is almost entirely plugin based. Commands and data structures held by the DB are registered via an abstract plugin API.
    Plugins are responsible for creating new data structure instances and operating on them.
   
 4. Running on Go, it is not single threaded like redis, but uses goroutines for query execution. Each client connecting is executed in a separate goroutine, and each key has its own read/write lock, allowing fine grained control.
 
### Motivations behind the project:
* Experimenting and learning Go.

* _"What if redis could do X"_ - While redis is an amazing piece of software and one of the best open source projects out there, a lot of times I find myself wishing redis could easily be extended to support custom commands and plugins. Doing it in C is not ideal, and I figured Go would be easier to create an environment for developing and sharing plugins, and keeping them isolated without the ability to do much damage to the database as a whole.  

##  Project Status
* I started working on Boiler about a year ago, and as of May I stopped working on it. I'm opening it in hope of finding collaborators or enough interest in the project to motivate me to get back to developing it.

* The core functionality works, and redis protocol compliancy works.
* There are a few example plugins, including:
    * Simple string based GET/SET/DELETE
    * Simple HASH key implementation with HGET/HSET
    * A prefix tree plugin allowing storage of many strings in a key, and prefix searching them.
    * A JSON object plugin, that lets you store JSON objects as strings, and manipulate them without serialization and deserialization. 
    * MONITOR like debugging stream.
    * A partially working replication plugin.
    
* Tests are really crude and basic.

* Plugins are still buggy and incomplete.

* Replication needs a lot of work.

* There exists documentation on writing plugins but it needs to be extended.

## A really short guide to writing a plugin

 Each plugin must implement the IPlugin interface, and return entries the implement the DataStruct interface. It also declares handlers that follow the db.HandlerFunc signature.

A plugin registers itself with GetManifest(), that returns a PluginManifest struct.

Here is a skeleton example of a plugin that does nothing, really:

```go

import (
	"db"
	gob "encoding/gob"
)

// The data struct this plugin is responsible for
type DummyStruct struct {

}

// Serializer for the data struct
func (s *DummyStruct)Serialize(g *gob.Encoder) error {

	return nil
}

//The name of the type we declare (each plugin can declare more than one type)
const T_DUMMY = "DUMMY"

//The plugin itself. If it has no state, it's just an empty struct
type DummyPlugin struct {

}

//Object creator. Whenever we receive a writing command for a type, we ask the responsible plugin to create an object of that type.
//We can make decisions on what to do here based on the command name given to use.
//We must return the type of the object and the actual entry
func (p *DummyPlugin)CreateObject(commandName string) (*db.Entry, string) {

	ret := &db.Entry{ Value: &DummyStruct{} }

	return ret, T_DUMMY
}

//This is a command handler. It recieves a descriptor of the command, with the command name, key and args, if any.
//
//It also receives the entry we are working on, and the session running the command. the session can be used for long lasting commadns
//like PUBSUB or MONITOR.
//We return a DB result, which can be the return value for the command, OK, or an error
func HandleFOO(cmd *db.Command, entry *db.Entry, session *db.Session) *db.Result {

	// we must cast the entry value to the struct we're suppposed to
	_ = entry.Value.(*DummyStruct)

	return db.NewResult(db.NewStatus("OK"))

}

//Deserialize and create a db entry.
//This is used for loading objects from a dump, and must be implemented at least as a stub.
//We use a gob decoder to decode the buffer
func (p *DummyPlugin)LoadObject(buf []byte, typeName string) *db.Entry {

	return nil

}

//
//Get the plugin manifest for the plugin
//A manifest declares the plugin, commmands and types it is responsible for
func (p *DummyPlugin)GetManifest() db.PluginManifest {

	return db.PluginManifest {

		Name: "DUMMY",
		Types: []string{ T_DUMMY, },
		Commands:  []db.CommandDescriptor {
			db.CommandDescriptor{
				CommandName: "FOO",
				MinArgs: 0,	MaxArgs: 0,
				Handler: HandleFOO,
				CommandType: db.CMD_WRITER,
			},
		},
	}

}

//String representation of the plugin to support %s formatting
func (p* DummyPlugin) String() string {
	return T_DUMMY
}

//init function
func (p* DummyPlugin) Init() error {

	return nil
}

//shutdown function
func (p* DummyPlugin) Shutdown() { }


```