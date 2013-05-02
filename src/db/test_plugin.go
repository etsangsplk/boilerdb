
//Test plugin doing simple set/get/expire

package db


import (
	"encoding/gob"
	"logging"
	"bytes"
)


type testStruct struct {
	Val string
}

func (ht *testStruct)Serialize(g *gob.Encoder) error {

	err := g.Encode(ht)

	return err
}


const T_STRING = "STRING"

type testPlugin struct {


}

func HandleSET(cmd *Command, entry *Entry, session *Session) *Result {

	obj := entry.Value.(*testStruct)
	obj.Val = string(cmd.Args[0])

	logging.Info("Setting key %s to %s", cmd.Key, obj.Val)
	return NewResult(NewStatus("OK"))

}
func HandleGET(cmd *Command, entry *Entry, session *Session) *Result {

	if entry != nil {
		obj := entry.Value.(*testStruct)
		r := NewResult(obj.Val)
		return r
	}

	return nil

}

func HandleEXISTS(cmd *Command, entry *Entry, session *Session) *Result {


	return NewResult(entry != nil)
}

func HandlePING(cmd *Command, entry *Entry, session *Session) *Result {
	return NewResult("PONG")
}

func (p *testPlugin)CreateObject(commandName string) (*Entry, string) {

	ret := &Entry{ Value: &testStruct{} }

return ret, T_STRING
}

//deserialize and create a db entry
func (p *testPlugin)LoadObject(buf []byte, typeName string) *Entry {

	if typeName == T_STRING {
		var s testStruct
		buffer := bytes.NewBuffer(buf)
		dec := gob.NewDecoder(buffer)
		err := dec.Decode(&s)
		if err != nil {
			logging.Info("Could not deserialize oject: %s", err)
			return nil
		}

		return &Entry{ Value: &s }


} else {
logging.Warning("Could not load value, invalid type %d", typeName)
}
return nil

}


// Get the plugin manifest for the simple plugin
func (p *testPlugin)GetManifest() PluginManifest {

	return PluginManifest {

	Name: "TESTUNG",
	Types: []string{ T_STRING, },
		Commands:  []CommandDescriptor {
			CommandDescriptor{
				CommandName: "SET",
				MinArgs: 1,	MaxArgs: 1,
				Handler: HandleSET,
				CommandType: CMD_WRITER,
			},
			CommandDescriptor{
				CommandName: "GET",
				MinArgs: 0,	MaxArgs: 0,
				Handler: HandleGET,
				CommandType: CMD_READER,
			},
			CommandDescriptor{
				CommandName: "PING",
				MinArgs: 0,	MaxArgs: 0,
				Handler: HandlePING,
				CommandType: CMD_READER,
			},
			CommandDescriptor{
				CommandName: "EXISTS",
				MinArgs: 0,	MaxArgs: 0,
				Handler: HandleEXISTS,
				CommandType: CMD_READER,
			},
		},


}

}
// String representation of the plugin to support %s formatting
func (p* testPlugin) String() string {
	return "TESTUNG"
}

//init function
func (p* testPlugin) Init() error {

	return nil
}

//shutdown function
func (p* testPlugin) Shutdown() { }


