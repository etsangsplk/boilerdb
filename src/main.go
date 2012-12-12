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

	"net"
	hash_table "plugins/hash_table"
	ptree "plugins/prefix_tree"
	simple "plugins/simple"
	builtin "plugins/builtin"
	json "plugins/json"
	"plugins/slave"
	"runtime"

)

///////////////////////////////////////////////////

func main() {

	logging.SetLevel(logging.ERROR | logging.WARN | logging.CRITICAL | logging.INFO)

	runtime.GOMAXPROCS(runtime.NumCPU())

	database := db.InitGlobalDataBase()

	///Register all the plugins
	ht := new(hash_table.HashTablePlugin)
	smp := new(simple.SimplePlugin)
	ptree := new(ptree.PrefixTreePlugin)
	builtin := new(builtin.BuiltinPlugin)
	js := new(json.JSONPlugin)
	sl := new(slave.SlavePlugin      )


	database.RegisterPlugins(ht, smp, ptree, builtin, js, sl)



	//


	if true {
		adap := redis_adapter.RedisAdapter{}

		adap.Init(database)
		adap.Name()
		addr, _ := net.ResolveTCPAddr("tcp", "0.0.0.0:2001")
		err := adap.Listen(addr)

		if err != nil {

			logging.Panic("Could not start adapter: %s", err)
			return
		}

		logging.Info("Starting adapter...")
		adap.Start()

	}

}
