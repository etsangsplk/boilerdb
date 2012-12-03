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
	"fmt"

)

const T_JSON uint32 = 32


func (jq *JsonQuery)Serialize(g *gob.Encoder) error {

	err := g.Encode(jq)

	return err
}



type JSONPlugin struct {

}

func HandleJSET(cmd *db.Command, entry *db.Entry, session *db.Session) *db.Result {

	jo := entry.Value.(*JsonQuery)
	data := map[string]interface{}{}
	dec := json.NewDecoder(strings.NewReader(string(cmd.Args[0])))
	err := dec.Decode(&data)
	if err != nil {
		log.Printf("Error decoding data: %s", err)
		return db.NewResult(db.NewError(db.E_INVALID_PARAMS))
	}
	jo.Blob = data
	return db.NewResult("OK")
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
	fmt.Println(ret)

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
	fmt.Println(ret)
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

	return nil
}


func (p *JSONPlugin)GetCommands() []db.CommandDescriptor {


	return []db.CommandDescriptor {
		db.CommandDescriptor{"JSET",1, HandleJSET, p, 1, db.CMD_WRITER},
		db.CommandDescriptor{"JQUERY", 1, HandleJQUERY, p, 1, db.CMD_READER},
		db.CommandDescriptor{"JGET", 0, HandleJGET, p, 1, db.CMD_READER},
	}
}


func (p* JSONPlugin) GetTypes() []uint32 {
	return []uint32{T_JSON,}
}


