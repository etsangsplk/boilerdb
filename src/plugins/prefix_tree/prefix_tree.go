/**
 * Created with IntelliJ IDEA.
 * User: dvirsky
 * Date: 11/17/12
 * Time: 1:40 PM
 * To change this template use File | Settings | File Templates.
 */
package prefix_tree

import (
	"db"
	gob "encoding/gob"
	"strconv"
//	"fmt"
)

type PrefixTree struct {

	root *Node
}

func (ht *PrefixTree)Serialize(g *gob.Encoder) error {

	err := g.Encode(ht)
	return err
}



type PrefixTreePlugin struct {


}

func HandlePSET(cmd *db.Command, entry *db.Entry) *db.Result {

	pt := entry.Value.(*PrefixTree)
	score, err := strconv.ParseFloat(string(cmd.Args[1]), 32)
	if err != nil {
		return db.NewResult(db.NewError(db.E_INVALID_PARAMS))
	}
	pt.root.set(string(cmd.Args[0]), float32(score) , "")

	return db.NewResult(db.NewStatus("OK"))

}
func HandlePGET(cmd *db.Command, entry *db.Entry) *db.Result {
	pt := entry.Value.(*PrefixTree)

	res, _ := pt.root.prefixSearch(string(cmd.Args[0]), cmd.HasArg("WITHSCORES"))
	var r *db.Result
	if res != nil {
		r = db.NewResult(*res)
	}	else {
	    r = db.NewResult("")
	}


	return r
}

const T_PREFIX_TREE uint32 = 16
func (p *PrefixTreePlugin)CreateObject() *db.Entry {

	root := newNode(0, 0)

	//	//root.pos = 0
	ret := &db.Entry{ Value: &PrefixTree{root},
		Type: T_PREFIX_TREE,
	}
	//fmt.Println("Created new hash table ", ret)
	return ret
}

func (p *PrefixTreePlugin)LoadObject(buf []byte, t uint32) *db.Entry {
	return nil
}


func (p *PrefixTreePlugin)GetCommands() []db.CommandDescriptor {


	return []db.CommandDescriptor {
		db.CommandDescriptor{"PSET", 2, HandlePSET, p, 0, db.CMD_WRITER},
		db.CommandDescriptor{"PGET", 1, HandlePGET, p, 0, db.CMD_READER},

	}
}


func (p* PrefixTreePlugin) GetTypes() []uint32 {
	return []uint32{T_PREFIX_TREE,}
}
