/**
 * Created with IntelliJ IDEA.
 * User: dvirsky
 * Date: 3/31/13
 * Time: 12:21 AM
 * To change this template use File | Settings | File Templates.
 */
package db

import (
	"net"
	"testing"
	"time"

	//"fmt"
)

func Test_Database(t *testing.T) {

	plugin := new(testPlugin)

	//test registering a plugin - we'll use it later!
	d := InitGlobalDataBase(".", false)
	err := d.RegisterPlugins(plugin)
	if err != nil {
		t.Errorf("Could not register plugin")
	}

	//create test command
	cmd := NewCommand("SET", "foo", []byte("bar"))
	if cmd == nil || cmd.Command != "SET" ||
		cmd.Key != "foo" ||
		len(cmd.Args) != 1 {
		t.Errorf("Failed creating command")
	}

	//test creating a session with an address
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:8080")
	if err != nil {
		t.Fail()
	}

	//test creating the session
	s := d.NewSession(addr)

	if s == nil {
		t.Error("Got nil session")
	}
	if s.Addr != addr {
		t.Errorf("Got different addresses - %s vs %s", s.Addr, addr)
	}

	//test sending and recieving responses

	st := "This is a test"
	s.Send(NewResult(st))

	r := s.Receive()

	if r == nil {
		t.Error("Got nil response")
	}

	v := r.Value.(string)

	if v != st {
		t.Errorf("Got invalid response when receiving from session. expected %s, got %s", st, v)
	}

	//Test db.HandleCommand

	res, err := d.HandleCommand(cmd, s)
	if err != nil || res == nil {
		t.Errorf("Failed processing command")
	}

	val, ok := res.Value.(*Status)
	if !ok {
		t.Fail()
	}

	if val == nil || val.Str != "OK" || val.Code != E_OK {
		t.Errorf("Wrong return code on SET: %s", val)
	}

	//test getting the value we just set
	res, err = d.HandleCommand(NewCommand("GET", "foo"), s)
	if err != nil {
		t.Fail()
	}
	val2, ok := res.Value.(string)
	if !ok {
		t.Fail()
	}
	if val2 != "bar" {
		t.Errorf("Got invalid result for get: %s", val)
	}

	//make sure it's in the database dictionary
	_, ok = d.dictionary[cmd.Key]
	if !ok {
		t.Fail()
	}

	//test deleting the value
	deleted := d.Delete(cmd.Key)
	if !deleted {
		t.Errorf("Wrong return valuf on deletion")
	}

	//make sure it was really deleted
	_, ok = d.dictionary[cmd.Key]
	if ok {
		t.Fail()
	}

	//try again - we should get false
	deleted = d.Delete(cmd.Key)
	if deleted {
		t.Errorf("Wrong return valuf on deletion")
	}

	//test expiration

	//set the value again
	res, err = d.HandleCommand(cmd, s)
	if err != nil || res == nil {
		t.Errorf("Failed processing command SET")
	}

	entry := d.dictionary[cmd.Key]
	when := time.Now().Add(1 * time.Second)
	//expire the key
	ok = d.SetExpire(cmd.Key, entry, when)
	if !ok {
		t.Errorf("Failed setting expiration")
	}
	if !entry.expired {
		t.Errorf("Entry was not set to expire")
	}
	//sleep to see it expires
	time.Sleep(1100 * time.Millisecond)
	//test getting the value expired - should be nil
	res, err = d.HandleCommand(NewCommand("GET", "foo"), s)
	if err != nil {
		t.Fail()
	}
	if err != nil {
		t.Errorf("Got invalid type for result: %s", res.Value)
	}

}
