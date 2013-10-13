package main

import (
	redis_adapter "adapters/redis"
	"db"
	"flag"
	"fmt"
	"logging"

	"config"
	"net"
	builtin "plugins/builtin"
	hash_table "plugins/hash_table"
	json "plugins/json"
	ptree "plugins/prefix_tree"
	repl "plugins/replication"
	simple "plugins/simple"
	"runtime"
)

///////////////////////////////////////////////////

func main() {

	//logging.SetLevel(logging.ERROR | logging.WARN | logging.CRITICAL | logging.INFO)
	logging.SetLevel(logging.ALL &^ logging.DEBUG)
	logging.Critical("Running on Go %s", runtime.GOROOT())
	runtime.GOMAXPROCS(runtime.NumCPU())

	///Register all the plugins
	ht := new(hash_table.HashTablePlugin)

	smp := new(simple.SimplePlugin)

	ptree := new(ptree.PrefixTreePlugin)
	builtin := new(builtin.BuiltinPlugin)
	js := new(json.JSONPlugin)
	rep := new(repl.ReplicationPlugin)

	workingDir := flag.String("dir", config.WORKING_DIRECTORY, "Database working directory")
	port := flag.Int("port", config.LISTEN_PORT, "Listening port")

	flag.Parse()
	database := db.InitGlobalDataBase(*workingDir, true)

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
