/**
 * Created with IntelliJ IDEA.
 * User: dvirsky
 * Date: 11/8/12
 * Time: 6:50 PM
 * To change this template use File | Settings | File Templates.
 */
package main

import (
	"runtime"
	"db"
	"net"
	"log"
	"fmt"
	simple "plugins/simple"
	hash_table "plugins/hash_table"
	redis_adapter "adapters/redis"

)


///////////////////////////////////////////////////




func main() {

	runtime.GOMAXPROCS(8)
	database := db.NewDataBase()
	ht := new(hash_table.HashTablePlugin)
	smp := new(simple.SimplePlugin)
	database.RegisterPlugins(ht, smp)


	if false {
		adap := redis_adapter.RedisAdapter{}

		adap.Init(database)
		addr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:2000")
		err := adap.Listen(addr)

		if err != nil {
			fmt.Printf("Err: %s", err.Error())
			log.Fatal(err)
			return
		}

		fmt.Printf("Go..\n")
		adap.Start()
	}
	//fmt.Println(ret)
//	for i := 0; i < 10; i++ {
//		cmd := db.Command{"HSET", fmt.Sprintf("foo%d", i), [][]byte{[]byte("bar"), []byte("baz")}}
//		_, _ = database.HandleCommand(&cmd)
//
//	}
//	_, _ = database.Dump()

	_ = database.LoadDump()



}
