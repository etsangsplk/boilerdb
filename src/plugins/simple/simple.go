/*
Simple GET/SET/DELETE implementation of strings
*/
package simple

import (
	//	"fmt"
	"db"
	//	"io"
	"bytes"
	gob "encoding/gob"

	log "github.com/llimllib/loglevel"
)

type SimpleStruct struct {
	Val string
}

func (ht *SimpleStruct) Serialize(g *gob.Encoder) error {

	err := g.Encode(ht)

	return err
}

const T_STRING = "STRING"

type SimplePlugin struct {
}

func HandleSET(cmd *db.Command, entry *db.Entry, session *db.Session) *db.Result {

	obj := entry.Value.(*SimpleStruct)
	obj.Val = string(cmd.Args[0])

	return db.NewResult(db.NewStatus("OK"))

}
func HandleGET(cmd *db.Command, entry *db.Entry, session *db.Session) *db.Result {

	if entry != nil {
		obj := entry.Value.(*SimpleStruct)
		r := db.NewResult(obj.Val)
		return r
	}

	return nil

}

func HandleEXISTS(cmd *db.Command, entry *db.Entry, session *db.Session) *db.Result {

	return db.NewResult(entry != nil)
}

func HandlePING(cmd *db.Command, entry *db.Entry, session *db.Session) *db.Result {
	return db.NewResult("PONG")
}

func (p *SimplePlugin) CreateObject(commandName string) (*db.Entry, string) {

	ret := &db.Entry{Value: &SimpleStruct{}}

	return ret, T_STRING
}

//deserialize and create a db entry
func (p *SimplePlugin) LoadObject(buf []byte, typeName string) *db.Entry {

	if typeName == T_STRING {
		var s SimpleStruct
		buffer := bytes.NewBuffer(buf)
		dec := gob.NewDecoder(buffer)
		err := dec.Decode(&s)
		if err != nil {
			log.Infof("Could not deserialize oject: %s", err)
			return nil
		}

		return &db.Entry{Value: &s}

	} else {
		log.Warnf("Could not load value, invalid type %d", typeName)
	}
	return nil

}

// Get the plugin manifest for the simple plugin
func (p *SimplePlugin) GetManifest() db.PluginManifest {

	return db.PluginManifest{

		Name:  "SIMPLE",
		Types: []string{T_STRING},
		Commands: []db.CommandDescriptor{
			db.CommandDescriptor{
				CommandName: "SET",
				MinArgs:     1, MaxArgs: 1,
				Handler:     HandleSET,
				CommandType: db.CMD_WRITER,
			},
			db.CommandDescriptor{
				CommandName: "GET",
				MinArgs:     0, MaxArgs: 0,
				Handler:     HandleGET,
				CommandType: db.CMD_READER,
			},
			db.CommandDescriptor{
				CommandName: "PING",
				MinArgs:     0, MaxArgs: 0,
				Handler:     HandlePING,
				CommandType: db.CMD_READER,
			},
			db.CommandDescriptor{
				CommandName: "EXISTS",
				MinArgs:     0, MaxArgs: 0,
				Handler:     HandleEXISTS,
				CommandType: db.CMD_READER,
			},
		},
	}

}

// String representation of the plugin to support %s formatting
func (p *SimplePlugin) String() string {
	return "SIMPLE"
}

//init function
func (p *SimplePlugin) Init() error {

	return nil
}

//shutdown function
func (p *SimplePlugin) Shutdown() {}
