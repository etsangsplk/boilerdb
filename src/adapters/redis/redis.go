package redis

import (
	"net"
	"log"
	"runtime"
	"strconv"
	"errors"
	//"time"
	"reflect"
	"fmt"
	"bufio"
	"io"
	"db"
)

var globalDict map[string][]byte = make(map[string][]byte)

func ReadRequest(reader *bufio.Reader) (cmd *db.Command, err error) {
	buf := readToCRLF(reader)

	switch buf[0] {
		case '*': {
			len, err  := strconv.Atoi(string(buf[1:]))
			if err == nil {
				res := readMultiBulkData(reader, len)
				return &db.Command{cmd: res[0], args: res, }, nil
			}
		}
		default: {
			return &db.Command{cmd: buf, args: nil}, nil
		}
	}
	return nil, fmt.Errorf("Could not read line. buf is '%s'", buf)
}

// panics on error (with redis.Error)
func assertCtlByte(buf []byte, b byte, info string) {
	if buf[0] != b {
		panic(fmt.Errorf("control byte for %s is not '%s' as expected - got '%s'", info, string(b), string(buf[0])))
	}
}

// panics on error (with redis.Error)
func assertNotError(e error, info string) {
	if e != nil {
		panic(e)
	}
}



// ----------------------------------------------------------------------
// Go-Redis System Errors or Bugs
// ----------------------------------------------------------------------


// ----------------------------------------------------------------------------
// protocol i/o
// ----------------------------------------------------------------------------

// reads all bytes upto CR-LF.  (Will eat those last two bytes)
// return the line []byte up to CR-LF
// error returned is NOT ("-ERR ...").  If there is a Redis error
// that is in the line buffer returned
//
// panics on errors (with redis.Error)

const (
	cr_byte    byte = byte('\r')
	lf_byte         = byte('\n')
	space_byte      = byte(' ')
	err_byte        = byte('-')
	ok_byte         = byte('+')
	count_byte      = byte('*')
	size_byte       = byte('$')
	num_byte        = byte(':')
	true_byte       = byte('1')
)


func readToCRLF(r *bufio.Reader) []byte {
	//	var buf []byte
	buf, e := r.ReadBytes(cr_byte)
	if e != nil {
		panic(fmt.Errorf("readToCRLF - ReadBytes", e))
	}

	var b byte
	b, e = r.ReadByte()
	if e != nil {
		panic(fmt.Errorf("readToCRLF - ReadByte", e))
	}
	if b != lf_byte {
		e = errors.New("<BUG> Expecting a Linefeed byte here!")
	}
	return buf[0 : len(buf)-1]
}

// Reads a multibulk response of given expected elements.
//
// panics on errors (with redis.Error)
func readBulkData(r *bufio.Reader, n int) (data []byte) {
	if n >= 0 {
		buffsize := n + 2
		data = make([]byte, buffsize)
		if _, e := io.ReadFull(r, data); e != nil {
			panic(fmt.Errorf("readBulkData - ReadFull", e))
		} else {
			if data[n] != cr_byte || data[n+1] != lf_byte {
				panic(fmt.Errorf("terminal was not crlf_bytes as expected - data[n:n+1]:%s", data[n:n+1]))
			}
			data = data[:n]
		}
	}
	return
}

// Reads a multibulk response of given expected elements.
// The initial *num\r\n is assumed to have been consumed.
//
// panics on errors (with redis.Error)
func readMultiBulkData(conn *bufio.Reader, num int) [][]byte {
	data := make([][]byte, num)
	for i := 0; i < num; i++ {
		buf := readToCRLF(conn)
		if buf[0] != size_byte {
			panic(fmt.Errorf("readMultiBulkData - expected: size_byte got: %d", buf[0]))
		}

		size, e := strconv.Atoi(string(buf[1:]))
		if e != nil {
			panic(fmt.Errorf("readMultiBulkData - Atoi parse error", e))
		}
		data[i] = readBulkData(conn, size)
	}
	return data
}

func serve(c *net.TCPConn) {
	cont := true
	reader := bufio.NewReader(c)
	defer func(cont *bool) {
		if e := recover(); e != nil {
			log.Println(e)
		}
		*cont = false

	}(&cont)


	//resp := []byte("+PONG\r\n")

	for cont {

		cmd, err := ReadRequest(reader)
		if err != nil {
			cont = false
			log.Println("Quitting!", err)
		}

		switch string(cmd.cmd) {
		case "SET": {
			//globalDict[string(cmd.args[1])] = cmd.args[2]
			go c.Write([]byte("+OK\r\n"))
		}
		case "GET":{

			go c.Write([]byte(fmt.Sprintf("+%s\r\n", globalDict[string(cmd.args[1])])))
		}
		default: {
			go c.Write([]byte("+OK\r\n"))
		}

		}
		//log.Printf("Read %d bytes %s: '%s'\n", n, err, buf)


	}
	// Shut down the connection.
	c.Close()
}
func main() {
	runtime.GOMAXPROCS(8)

	// Listen on TCP port 2000 on all interfaces.
	l, err := net.Listen("tcp", ":2000")

	if err != nil {
		log.Fatal(err)
	}
	for {
		// Wait for a connection.

		conn, err := l.(* net.TCPListener).AcceptTCP()

		if err != nil {
			log.Fatal(err)
		}
		// Handle the connection in a new goroutine.
		// The loop then returns to accepting, so that
		// multiple connections may be served concurrently.
		go serve(conn)

	}
}
