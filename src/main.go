/**
 * Created with IntelliJ IDEA.
 * User: dvirsky
 * Date: 11/8/12
 * Time: 6:50 PM
 * To change this template use File | Settings | File Templates.
 */
package main

import (
	redis_adapter "adapters/redis"
	"db"
	"logging"
	"flag"
	"fmt"

	"net"
	builtin "plugins/builtin"
	hash_table "plugins/hash_table"
	json "plugins/json"
	ptree "plugins/prefix_tree"
	simple "plugins/simple"
	repl "plugins/replication"
	"runtime"
	"config"
)

///////////////////////////////////////////////////

func main() {

	logging.SetLevel(logging.ERROR | logging.WARN | logging.CRITICAL | logging.INFO)

	runtime.GOMAXPROCS(runtime.NumCPU())



	///Register all the plugins
	ht := new(hash_table.HashTablePlugin)

	smp := new(simple.SimplePlugin)

	ptree := new(ptree.PrefixTreePlugin)
	builtin := new(builtin.BuiltinPlugin)
	js := new(json.JSONPlugin)
	rep := new(repl.ReplicationPlugin)

	workingDir := flag.String("dir", config.WORKING_DIRECTORY, "Database working directory")
	port := flag.Int("port", config.LISTEN_PORT, "Listening port" )

	flag.Parse()
	database := db.InitGlobalDataBase(*workingDir)

	database.RegisterPlugins(ht, smp, ptree, builtin, js, rep)

	//

	if true {
		adap := redis_adapter.RedisAdapter{}

		adap.Init(database)
		adap.Name()
		addr, _ := net.ResolveTCPAddr("tcp", fmt.Sprintf("0.0.0.0:%d", *port))
		err := adap.Listen(addr)

		if err != nil {

			logging.Panic("Could not start adapter: %s", err)
			return
		}

		logging.Info("Starting adapter...")
		adap.Start()

	}

}
