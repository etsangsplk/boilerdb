/**
 * Created with IntelliJ IDEA.
 * User: dvirsky
 * Date: 11/15/12
 * Time: 12:58 AM
 * To change this template use File | Settings | File Templates.
 */
package hash_table

import (
//	"fmt"
	"db"
	"log"
	"bytes"
	gob "encoding/gob"
)


type HashTableStruct struct {

	Table map[string][]byte
}

func (ht *HashTableStruct)Serialize(g *gob.Encoder) error {

	err := g.Encode(ht)
	return err
}


type HashTablePlugin struct {


}

func HandleHSET(cmd *db.Command, entry *db.Entry) *db.Result {

	obj := entry.Value.(*HashTableStruct)
	//fmt.Printf("%p %p %s\n", &obj, &(obj.table), obj.table)
	obj.Table[string(cmd.Args[0])] = cmd.Args[1]

	return db.NewResult(db.NewStatus("OK"))

}
func HandleHGET(cmd *db.Command, entry *db.Entry) *db.Result {
	tbl := entry.Value.(*HashTableStruct)

	r := db.NewResult(string(tbl.table[string(cmd.Args[0])]))
	return r
}

func HandleHGETALL(cmd *db.Command, entry *db.Entry) *db.Result {
	tbl := entry.Value.(*HashTableStruct)
	r := db.NewResult(tbl.table)
	return r
}

const T_HASHTABLE uint32 = 8
func (p *HashTablePlugin)CreateObject() *db.Entry {

	ret := &db.Entry{ Value: &HashTableStruct{make(map[string][]byte)},
					 Type: T_HASHTABLE,
					}
	//fmt.Println("Created new hash table ", ret)
	return ret
}

func (p *HashTablePlugin)LoadObject(buf []byte, t uint32) *db.Entry {

	if t == T_HASHTABLE {

		var ht HashTableStruct
		buffer := bytes.NewBuffer(buf)
		dec := gob.NewDecoder(buffer)
		err := dec.Decode(&ht)
		if err != nil {
			log.Printf("Could not deserialize oject: %s", err)
			return nil
		}

		return &db.Entry{
			Value: &ht,
			Type: T_HASHTABLE,
		}

	}
	log.Printf("Invalid type %u. Could not deserialize", t)
	return nil
}




func (p *HashTablePlugin)GetCommands() []db.CommandDescriptor {


	return []db.CommandDescriptor {
		db.CommandDescriptor{"HSET", "subkey:string value:string", HandleHSET, p, 0, db.CMD_WRITER},
		db.CommandDescriptor{"HGET", "subkey:string", HandleHGET, p, 0, db.CMD_READER},
		db.CommandDescriptor{"HGETALL", "subkey:string", HandleHGETALL, p, 0, db.CMD_READER},
	}
}

func (p* HashTablePlugin) GetTypes() []uint32 {
	return []uint32{T_HASHTABLE,}
}
