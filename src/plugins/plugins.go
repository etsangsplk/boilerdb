//This package contains all the plugins in the system, each with its own package.
//Each plugin must implement the Plugin interface, and return entries the implement the DataStruct interface.
//It also declares handlers that follow the db.HandlerFunc signature.
//
//A plugin registers itself with GetManifest(), that returns a PluginManifest struct.
//
//Here is a skeleton example of a plugin that does nothing, really:
/*
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
					Help: "FOO [key]: Do nothing with a key",
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
*/
package plugins

