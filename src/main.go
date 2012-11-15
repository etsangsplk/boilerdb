/**
 * Created with IntelliJ IDEA.
 * User: dvirsky
 * Date: 11/8/12
 * Time: 6:50 PM
 * To change this template use File | Settings | File Templates.
 */
package main

import (
	"fmt"
	"time"
	"runtime"
	"db"
	hash_table "plugins/hash_table"

)


///////////////////////////////////////////////////



func main() {
	runtime.GOMAXPROCS(8)
	database := db.NewDataBase()
	ht := new(hash_table.HashTablePlugin)
	database.RegisterPlugins(ht)

	st := time.Now()

	n := 1000000



	cmd := db.Command{"HSET", "foo", []string{ "k", "v"}}
	for i := 0; i < n; i++ {
		cmd.Args[0] = string(i)
		//cmd.Args[0] = fmt.Sprintf("k%d", i)

		_, _ = database.HandleCommand(cmd.Key, &cmd)
		//fmt.Println(r, e)

	}

	duration := time.Since(st)

	fmt.Printf("%d ops took %s, %f ops/sec", n, duration, float64(n)/duration.Seconds())
	//fmt.Println(ret)
	//ret = db.HandleCommand("myhash", &Command{"HGET", []string{"foo"}})
	//fmt.Println(ret)

}
