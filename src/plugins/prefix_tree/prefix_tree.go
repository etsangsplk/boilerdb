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
	"logging"
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

//identifier for prefix tree
const T_PREFIX_TREE = "PREFIX_TREE"


//callback for the database to allocate a new prefix tree
func (p *PrefixTreePlugin)CreateObject(command string) (*db.Entry, string) {

	Root := newNode(0, 0)

	ret := &db.Entry{ Value: &PrefixTree{Root} }

	return ret, T_PREFIX_TREE
}

// de-serialize callback (to be implemented...)
func (p *PrefixTreePlugin)LoadObject(buf []byte, t string) *db.Entry {
	if t == T_PREFIX_TREE {

		var pt PrefixTree
		buffer := bytes.NewBuffer(buf)
		dec := gob.NewDecoder(buffer)
		err := dec.Decode(&pt)
		if err != nil {
			logging.Info("Could not deserialize oject: %s", err)
			return nil
		}

		return &db.Entry{  Value: &pt }

	}
	logging.Info("Invalid type %s. Could not deserialize", t)
	return nil
}

//the commands exposed by this plugin and their handlers
func (p *PrefixTreePlugin)GetManifest() db.PluginManifest {

	return db.PluginManifest {

		Name: "PREFIX_TREE",

		Types: []string{T_PREFIX_TREE,},

		Commands:  []db.CommandDescriptor {
			db.CommandDescriptor{
				CommandName: "PSET",
				MinArgs: 2,	MaxArgs: 2,
				Handler: HandlePSET,
				CommandType: db.CMD_WRITER,
			},
			db.CommandDescriptor{
				CommandName: "PINCRBY",
				MinArgs: 2,	MaxArgs: 2,
				Handler: HandlePINCRBY,
				CommandType: db.CMD_WRITER,
			},
			db.CommandDescriptor{
				CommandName: "PSEARCH",
				MinArgs: 1,	MaxArgs: 2,
				Handler: HandlePSEARCH,
				CommandType: db.CMD_READER,
			},
			db.CommandDescriptor{
				CommandName: "PGET",
				MinArgs: 1,	MaxArgs: 1,
				Handler: HandlePGET,
				CommandType: db.CMD_READER,
			},
		},
	}

}


func (p* PrefixTreePlugin) String() string {
	return "PREFIX_TREE"
}

//init function
func (p* PrefixTreePlugin) Init() error {

	return nil
}

//shutdown function
func (p* PrefixTreePlugin) Shutdown() { }
