/**
 * Created with IntelliJ IDEA.
 * User: dvirsky
 * Date: 12/3/12
 * Time: 1:04 AM
 * To change this template use File | Settings | File Templates.
 */
package json
import (
	gob "encoding/gob"
	"encoding/json"
	"db"
	"strings"
	"log"
	"bytes"
	"fmt"

)

const T_JSON uint32 = 32


func (jq *JsonQuery)Serialize(g *gob.Encoder) error {

	err := g.Encode(jq)

	return err
}



type JSONPlugin struct {

}

func parseJSONPath(path string)[]string {
	arg := strings.Replace(path, "[", ".", -1)
	arg = strings.Replace(arg, "]", "", -1)
	arg = strings.TrimRight(arg, ".")
	return strings.Split(arg, ".")
}
func HandleJSET(cmd *db.Command, entry *db.Entry, session *db.Session) *db.Result {

	jo := entry.Value.(*JsonQuery)
	var data interface{}
	dec := json.NewDecoder(strings.NewReader(string(cmd.Args[1])))
	err := dec.Decode(&data)
	if err != nil {
		log.Printf("Error decoding data: %s (%s)", err,string(cmd.Args[1]) )
		return db.NewResult(db.NewError(db.E_INVALID_PARAMS))
	}


	//set the root object
	if string(cmd.Args[0]) == "." {
		jo.Blob = data
		err = nil
	} else {
		err = jo.Set(data, parseJSONPath(string(cmd.Args[0]))...)
	}

	if err == nil {
		return db.NewResult("OK")
	}
	log.Printf("Unable to set: %s", err)
	return db.NewResult(db.NewError(db.E_INVALID_PARAMS))

}

func HandleJGET(cmd *db.Command, entry *db.Entry, session *db.Session) *db.Result {

	if entry == nil {
		return nil
	}


	jq := entry.Value.(*JsonQuery)
	ret, err := json.Marshal(jq.Blob)

	if err != nil {
		return db.NewResult(db.NewError(db.E_UNKNOWN_ERROR))

	}

	return db.NewResult(string(ret))



}

func HandleJQUERY(cmd *db.Command, entry *db.Entry, session *db.Session) *db.Result {

	if entry == nil {
		return nil
	}
	jq := entry.Value.(*JsonQuery)

	arg := strings.Replace(string(cmd.Args[0]), "[", ".", -1)
	arg = strings.Replace(arg, "]", "", -1)
	arg = strings.TrimRight(arg, ".")



	ret, _ := jq.String(strings.Split(arg, ".")...)
	return db.NewResult(ret)
}



func (p *JSONPlugin)CreateObject() *db.Entry {

	ret := &db.Entry{
		Value: NewQuery(make(map[string]interface{})) ,
		Type: T_JSON,
	}

	//fmt.Println("Created new hash table ", ret)
	return ret
}

//deserialize and create a db entry
func (p *JSONPlugin)LoadObject(buf []byte, t uint32) *db.Entry {

	fmt.Println(t)
	if t == T_JSON {
		var s JsonQuery
		buffer := bytes.NewBuffer(buf)
		dec := gob.NewDecoder(buffer)
		err := dec.Decode(&s)
		if err != nil {
			log.Printf("Could not deserialize oject: %s", err)
			return nil
		}

		return &db.Entry{
			Value: &s,
			Type: T_JSON,
		}
	}

	return nil
}


func (p *JSONPlugin)GetCommands() []db.CommandDescriptor {


	return []db.CommandDescriptor {
		db.CommandDescriptor{"JSET",2, HandleJSET, p, 1, db.CMD_WRITER},
		db.CommandDescriptor{"JQUERY", 1, HandleJQUERY, p, 1, db.CMD_READER},
		db.CommandDescriptor{"JGET", 0, HandleJGET, p, 1, db.CMD_READER},
	}
}


func (p* JSONPlugin) GetTypes() []uint32 {
	return []uint32{T_JSON,}
}


