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
	"fmt"
	"log"
	"net"
	hash_table "plugins/hash_table"
	ptree "plugins/prefix_tree"
	simple "plugins/simple"
	system "plugins/system"
	"runtime"
//	"time"
//	"os"
//	"runtime/pprof"
//	"bufio"
)

///////////////////////////////////////////////////

func main() {



	runtime.GOMAXPROCS(runtime.NumCPU())

	database := db.InitGlobalDataBase()

	///Register all the plugins
	ht := new(hash_table.HashTablePlugin)
	smp := new(simple.SimplePlugin)
	ptree := new(ptree.PrefixTreePlugin)
	sys := new(system.SystemPlugin)


	database.RegisterPlugins(ht, smp, ptree, sys)


	//


	if true {
		adap := redis_adapter.RedisAdapter{}

		adap.Init(database)
		adap.Name()
		addr, _ := net.ResolveTCPAddr("tcp", "0.0.0.0:2000")
		err := adap.Listen(addr)

		if err != nil {
			fmt.Printf("Err: %s", err.Error())
			log.Fatal(err)
			return
		}

		fmt.Printf("Go..\n")

		go db.DB.LoadDump()

		adap.Start()

	}
	//fmt.Println(ret)
//	for i := 0; i < 10; i++ {
//		cmd := db.Command{"HSET", fmt.Sprintf("foo%d", i), [][]byte{[]byte("bar"), []byte("baz")}}
//		_, _ = database.HandleCommand(&cmd, nil)
//
//	}
	//_, _ = database.Dump()

}
