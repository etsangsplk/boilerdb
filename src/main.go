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
	hash_table "plugins/hash_table"
	redis_adapter "adapters/redis"

)


///////////////////////////////////////////////////




func main() {

	runtime.GOMAXPROCS(8)
	database := db.NewDataBase()
	ht := new(hash_table.HashTablePlugin)
	database.RegisterPlugins(ht)

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

	//fmt.Println(ret)
	//ret = db.HandleCommand("myhash", &Command{"HGET", []string{"foo"}})
	//fmt.Println(ret)

}
