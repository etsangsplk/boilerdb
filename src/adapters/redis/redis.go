package redis

import (
	"bufio"
	"db"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"reflect"
	"runtime/debug"
	"strconv"

//	"strings"
)

type RedisAdapter struct {
	db         *db.DataBase
	listener   net.Listener
	numClients uint
	isRunning  bool
}

var globalDict map[string][]byte = make(map[string][]byte)

func (r *RedisAdapter) Init(d *db.DataBase) {
	r.db = d
}

func (r *RedisAdapter) Listen(addr net.Addr) error {
	listener, err := net.Listen(addr.Network(), addr.String())

	if err != nil {
		return err
	}

	r.listener = listener
	return nil
}

func (r *RedisAdapter) SerializeResponse(res interface{}, writer io.Writer) error {



	switch res.(type) {

	case nil:
		writer.Write([]byte("$-1\r\n"))

	case bool:
		boolVal := res.(bool)
		intVal := 0
		if boolVal {
			intVal = 1
		}
		writer.Write([]byte(fmt.Sprintf(":%d\r\n", intVal)))

	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		writer.Write([]byte(fmt.Sprintf(":%d\r\n", res)))
	case  float32 , float64:
		s := fmt.Sprintf("%f", res)
		writer.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)))
	case string:
		s := res.(string)
		writer.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)))

	case []interface{}:
		arr := res.([]interface {})
		l := len(arr)
		writer.Write([]byte(fmt.Sprintf("*%d\r\n", l)))
		//recursively serialize all entries in the list
		for i := 0; i < l; i++ {
			err := r.SerializeResponse(arr[i], writer )
			if err != nil {
				log.Panic(err)
			}
		}

	case *db.Status:
		status := res.(*db.Status)
		writer.Write([]byte(fmt.Sprintf("+%s\r\n", status.Str)))

	case *db.Command:

		writer.Write([]byte(fmt.Sprintf("+%s\r\n", res.(*db.Command).ToString())))

	case *db.Error:
		e := res.(*db.Error)
		writer.Write([]byte(fmt.Sprintf("-ERR %d: %s\r\n", e.Code, e.ToString())))

	case map[string]interface{}:
		m := res.(map[string]interface{})
		writer.Write([]byte(fmt.Sprintf("*%d\r\n", len(m)*2)))
		for k, _ := range m {
			writer.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(k), k)))
			err := r.SerializeResponse(m[k], writer )
			if err != nil {
				log.Panic(err)
			}
		}

	default:
		writer.Write([]byte(fmt.Sprintf("-ERR Unknown type '%s'\r\n", )))
		return fmt.Errorf("Unknown type '%s'. Could not serialize", reflect.TypeOf(res))
	}

	return nil
}

func (r *RedisAdapter) HandleConnection(c *net.TCPConn) error {
	var err error = nil

	fp, _ := c.File()
	_ = fp
	reader := bufio.NewReaderSize(fp, 8192)
	writer := bufio.NewWriter(fp)

	session := r.db.NewSession(c.RemoteAddr())

	//error handler - write an error message to the socket and close it
	defer func(err *error, writer *bufio.Writer) {
		if e := recover(); e != nil {

			defer func() {
				ee := recover()
				if ee != nil {
					log.Printf("Error handling error :) %s", ee)
				}
			}()

			*err = e.(error)
			session.Stop()
			c.Close()

			if *err != io.EOF && *err != io.ErrClosedPipe {
				log.Printf("Error processing command: %s\n", e)
				debug.PrintStack()

			}

		}
	}(&err, writer)

	go session.Run()

	//this goroutine actually handles processing and writing to the
	go func() {

		for session.IsRunning {

			msg := <-session.OutChan
			if msg != nil {
				r.SerializeResponse(msg.Value, writer)
			} else {
				r.SerializeResponse(nil, writer)
			}
			err = writer.Flush()
			if err != nil {

				session.Stop()

				break

			}

		}
		fmt.Printf("Stopping Serializer....\n")
	}()

	//the request reading loop
	for err == nil && r.isRunning && session.IsRunning {

		//read an parse the request
		cmd, err := ReadRequest(reader)

		if err != nil {
			log.Println("Quitting!", err)
			break

		} else {
			session.InChan <- cmd
		}
	}

	//stop the session
	session.Stop()

	c.Close()
	return err
}

func (r *RedisAdapter) Start() error {
	r.isRunning = true

	for r.isRunning {
		conn, err := r.listener.(*net.TCPListener).AcceptTCP()

		if err != nil {
			log.Fatal(err)
			return err
		}
		// Handle the connection in a new goroutine.
		// The loop then returns to accepting, so that
		// multiple connections may be served concurrently.
		go r.HandleConnection(conn)
	}

	return nil
}

func (r *RedisAdapter) Stop() error {
	r.isRunning = false

	return nil
}

func (r *RedisAdapter) Name() string {
	return "Redis"
}

func ReadRequest(reader *bufio.Reader) (cmd *db.Command, err error) {
	buf := readToCRLF(reader)

	switch buf[0] {
	case '*':
		{
			ll, err := strconv.Atoi(string(buf[1:]))
			if err == nil {
				res := readMultiBulkData(reader, ll)
				if len(res) > 1 {
					return &db.Command{Command: string(res[0]), Key: string(res[1]), Args: res[2:]}, nil

				} else {
					return &db.Command{Command: string(res[0]), Key: "", Args: nil}, nil
				}
			}
		}
	default:
		{
			return &db.Command{Command: string(buf), Args: nil}, nil
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
		panic(e)
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
