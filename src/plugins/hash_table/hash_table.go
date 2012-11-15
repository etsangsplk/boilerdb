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
)


type HashTableStruct struct {

	table map[string]string
}

type HashTablePlugin struct {


}

func HandleHSET(cmd *db.Command, entry *db.Entry) *db.Result {

	obj := entry.Value.(*HashTableStruct)
	//fmt.Printf("%p %p %s\n", &obj, &(obj.table), obj.table)
	obj.table[cmd.Args[0]] = cmd.Args[1]

	return db.NewResult("OK")

}
func HandleHGET(cmd *db.Command, entry *db.Entry) *db.Result {

	tbl := entry.Value.(*HashTableStruct)
	return db.NewResult(tbl.table[cmd.Args[0]])

}

const T_HASHTABLE uint32 = 8
func (p *HashTablePlugin)CreateObject() *db.Entry {

	ret := &db.Entry{&HashTableStruct{make(map[string]string)}, T_HASHTABLE}
	//fmt.Println("Created new hash table ", ret)
	return ret
}

func (p *HashTablePlugin)GetCommands() []db.CommandDescriptor {


	return []db.CommandDescriptor {
		db.CommandDescriptor{"HSET", "subkey:string value:string", HandleHSET, p},
		db.CommandDescriptor{"HGET", "subkey:string", HandleHSET, p},
	}
}
