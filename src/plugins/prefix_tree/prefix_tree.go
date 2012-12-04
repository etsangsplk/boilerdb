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
	"fmt"
	"log"
	"bytes"
)

type PrefixTree struct {

	Root *Node
}

func (ht *PrefixTree)Serialize(g *gob.Encoder) error {

	err := g.Encode(ht)
	return err
}



type PrefixTreePlugin struct {


}

// set handler. sets (or replaces) a key in the tree with a given score
func HandlePSET(cmd *db.Command, entry *db.Entry, session *db.Session) *db.Result {

	pt := entry.Value.(*PrefixTree)
	
	score, err := strconv.ParseFloat(string(cmd.Args[1]), 32)
	if err != nil {
		return db.NewResult(db.NewError(db.E_INVALID_PARAMS))
	}
	pt.Root.set(string(cmd.Args[0]), float32(score) , "")

	return db.NewResult(db.NewStatus("OK"))

}

// increment handler. we can automatically increment the score of a given key
func HandlePINCRBY(cmd *db.Command, entry *db.Entry, session *db.Session) *db.Result {

	pt := entry.Value.(*PrefixTree)
	score, err := strconv.ParseFloat(string(cmd.Args[1]), 32)
	if err != nil {
		return db.NewResult(db.NewError(db.E_INVALID_PARAMS))
	}
	newScore := pt.Root.increment(string(cmd.Args[0]), float32(score))
	

	return db.NewResult(fmt.Sprintf("%f", newScore))

}

// Prefix search handler. the format is "PSEARCH <key> <prefix> [WITHSCORES]"
func HandlePSEARCH(cmd *db.Command, entry *db.Entry, session *db.Session) *db.Result {

	if entry == nil {
		return nil
	}

	pt := entry.Value.(*PrefixTree)

	res, _ := pt.Root.prefixSearch(string(cmd.Args[0]))
	var r *db.Result
	if res != nil {

		step := 1
		num := len(res)

		//see if we need to return scores as well
		withScores := cmd.HasArg("WITHSCORES")

		//if we do, we allocate a double list, where one member is the value and the next is its score
		if  withScores {
			step = 2
			num *= step
		}
		ret := make([]interface{}, num)

		//put the relevant values in place
		n := 0
		for i := 0; i < num; i+=step {
			if res[n] != nil {
				ret[i] = res[n].Key
			}
			//redis doesn't support floats over protocol....
			if withScores {
				ret[i+1] = res[n].Score
			}
			n++
		}

		r = db.NewResult(ret)
	}	else {
	    r = nil
	}


	return r
}

// Test existence of a key, and return it and its score if it exists
func HandlePGET(cmd *db.Command, entry *db.Entry, session *db.Session) *db.Result {

	if entry == nil {
		return nil
	}

	pt := entry.Value.(*PrefixTree)

	record := pt.Root.get(string(cmd.Args[0]))
	var r *db.Result
	if record != nil {
		r = db.NewResult([2]string{record.Key, fmt.Sprintf("%f", record.Score)})
	}	else {
	    r = db.NewResult("")
	}


	return r
}

//magic number for prefix trees
const T_PREFIX_TREE uint32 = 16


//callback for the database to allocate a new prefix tree
func (p *PrefixTreePlugin)CreateObject() *db.Entry {

	Root := newNode(0, 0)

	//	//root.pos = 0
	ret := &db.Entry{ Value: &PrefixTree{Root},
		Type: T_PREFIX_TREE,
	}
	//fmt.Println("Created new hash table ", ret)
	return ret
}

// de-serialize callback (to be implemented...)
func (p *PrefixTreePlugin)LoadObject(buf []byte, t uint32) *db.Entry {
	if t == T_PREFIX_TREE {

		var pt PrefixTree
		buffer := bytes.NewBuffer(buf)
		dec := gob.NewDecoder(buffer)
		err := dec.Decode(&pt)
		if err != nil {
			log.Printf("Could not deserialize oject: %s", err)
			return nil
		}

		return &db.Entry{
			Value: &pt,
			Type: T_PREFIX_TREE,
		}

	}
	log.Printf("Invalid type %u. Could not deserialize", t)
	return nil
}

//the commands exposed by this plugin and their handlers
func (p *PrefixTreePlugin)GetCommands() []db.CommandDescriptor {


	return []db.CommandDescriptor {
		db.CommandDescriptor{"PSET", 2, HandlePSET, p, 0, db.CMD_WRITER},
		db.CommandDescriptor{"PINCRBY", 2, HandlePINCRBY, p, 0, db.CMD_WRITER},
		db.CommandDescriptor{"PSEARCH", 1, HandlePSEARCH, p, 0, db.CMD_READER},
		db.CommandDescriptor{"PGET", 1, HandlePGET, p, 0, db.CMD_READER},

	}
}


func (p* PrefixTreePlugin) GetTypes() []uint32 {
	return []uint32{T_PREFIX_TREE,}
}
