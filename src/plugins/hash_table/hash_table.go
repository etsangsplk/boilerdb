/*
This example plugin implements some of the H* commands of redis:
 	HSET HGET HGETALL
*/
package hash_table

import (
	//	"fmt"
	"bytes"
	"db"
	gob "encoding/gob"

	log "github.com/llimllib/loglevel"
)

type HashTableStruct struct {
	Table map[string][]byte
}

func (ht *HashTableStruct) Serialize(g *gob.Encoder) error {

	err := g.Encode(ht)
	return err
}

type HashTablePlugin struct {
}

func HandleHSET(cmd *db.Command, entry *db.Entry, session *db.Session) *db.Result {

	obj := entry.Value.(*HashTableStruct)
	//fmt.Printf("%p %p %s\n", &obj, &(obj.table), obj.table)
	obj.Table[string(cmd.Args[0])] = cmd.Args[1]

	return db.NewResult(db.NewStatus("OK"))

}
func HandleHGET(cmd *db.Command, entry *db.Entry, session *db.Session) *db.Result {
	tbl := entry.Value.(*HashTableStruct)

	r := db.NewResult(string(tbl.Table[string(cmd.Args[0])]))
	return r
}

func HandleHGETALL(cmd *db.Command, entry *db.Entry, session *db.Session) *db.Result {
	tbl := entry.Value.(*HashTableStruct)
	r := db.NewResult(tbl.Table)
	return r
}

const T_HASHTABLE = "HASHTABLE"

func (p *HashTablePlugin) CreateObject(commandName string) (*db.Entry, string) {

	return &db.Entry{Value: &HashTableStruct{make(map[string][]byte)}}, T_HASHTABLE
}

func (p *HashTablePlugin) LoadObject(buf []byte, t string) *db.Entry {

	if t == T_HASHTABLE {

		var ht HashTableStruct
		buffer := bytes.NewBuffer(buf)
		dec := gob.NewDecoder(buffer)
		err := dec.Decode(&ht)
		if err != nil {
			log.Infof("Could not deserialize oject: %s", err)
			return nil
		}

		return &db.Entry{Value: &ht}

	}
	log.Errorf("Invalid type %u. Could not deserialize", t)
	return nil
}

func (p *HashTablePlugin) GetManifest() db.PluginManifest {

	return db.PluginManifest{

		Name: T_HASHTABLE,

		Types: []string{T_HASHTABLE},

		Commands: []db.CommandDescriptor{
			db.CommandDescriptor{
				CommandName: "HSET",
				MinArgs:     2, MaxArgs: 2,
				Handler:     HandleHSET,
				CommandType: db.CMD_WRITER,
			},
			db.CommandDescriptor{
				CommandName: "HGET",
				MinArgs:     1, MaxArgs: 1,
				Handler:     HandleHGET,
				CommandType: db.CMD_READER,
			},
			db.CommandDescriptor{
				CommandName: "HGETALL",
				MinArgs:     0, MaxArgs: 0,
				Handler:     HandleHGETALL,
				CommandType: db.CMD_READER,
			},
		},
	}
}

func (p *HashTablePlugin) String() string {
	return "HASHTABLE"
}

//init function
func (p *HashTablePlugin) Init() error {

	return nil
}

//shutdown function
func (p *HashTablePlugin) Shutdown() {}
