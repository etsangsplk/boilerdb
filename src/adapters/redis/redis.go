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

func (r *RedisAdapter) SerializeResponse(res *db.Result, writer io.Writer) string {

	//for nil values we write nil yo...
	if res == nil {

		writer.Write([]byte("$-1\r\n"))
		return ""
	}
	kind := res.Kind()

	switch kind {

	case reflect.Bool:
		boolVal := res.Bool()
		intVal := 0
		if boolVal {
			intVal = 1
		}
		writer.Write([]byte(fmt.Sprintf(":%d\r\n", intVal)))

	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		writer.Write([]byte(fmt.Sprintf(":%d\r\n", res.Int())))

	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		writer.Write([]byte(fmt.Sprintf(":%d\r\n", res.Uint())))

	case reflect.Slice, reflect.Array:
		l := res.Len()
		writer.Write([]byte(fmt.Sprintf("*%d\r\n", l)))

		for i := 0; i < l; i++ {

			v := res.Index(i).String()
			writer.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(v), v)))
		}

	case reflect.Map:
		l := res.Len() * 2
		writer.Write([]byte(fmt.Sprintf("*%d\r\n", l)))

		for _, k := range res.MapKeys() {
			v := string(res.MapIndex(k).Bytes())
			writer.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n$%d\r\n%s\r\n", len(k.String()), k.String(), len(v), v)))
		}

	case reflect.Ptr:
		__value := *res

		s, ok := __value.Interface().(*db.Status)
		if ok {
			writer.Write([]byte(fmt.Sprintf("+%s\r\n", s.Str)))
			break
		}

		e, ok := __value.Interface().(*db.Error)
		if ok {
			writer.Write([]byte(fmt.Sprintf("-ERR %d: %s\r\n", e.Code, e.ToString())))
			break
		}

		writer.Write([]byte(fmt.Sprintf("-ERR Unknown type\r\n")))

	default:
		s := res.String()
		writer.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)))
	}

	return ""
}

func (r *RedisAdapter) HandleConnection(c *net.TCPConn) error {
	var err error = nil

	reader := bufio.NewReaderSize(c, 8192)
	writer := bufio.NewWriter(c)

	//mark start and end of session to track number of sessions on the server
	db.DB.SessionStart()
	defer db.DB.SessionEnd()

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

			r.SerializeResponse(db.NewResult(db.NewError(db.E_UNKNOWN_ERROR)), writer)
			writer.Flush()
			c.Close()

			log.Printf("Error processing command: %s\n", e)
			debug.PrintStack()

		}
	}(&err, writer)

	//c.SetNoDelay(true)
	//c.SetReadBuffer(4)

	searilzerChan := make(chan *db.Result, 10)
	processorChan := make(chan *db.Command, 5)

	running := true

	//this goroutine actually handles processing and writing to the
	go func() {

		for running {

			msg := <-searilzerChan
			r.SerializeResponse(msg, writer)
			err = writer.Flush()
			if err != nil {

				running = false
				break

			}

		}
		fmt.Printf("Stopping Serializer....\n")
	}()


	//a go routine that processes the requests from the parsed channel and pushes the responses to the serializer channel
	go func() {
		for running {

			cmd := <-processorChan
			ret, _ := r.db.HandleCommand(cmd)
			searilzerChan <- ret

		}
		fmt.Printf("Stopping Processor....\n")
	}()


	//the request reading loop
	for err == nil && r.isRunning && running {

		//read an parse the request
		cmd, err := ReadRequest(reader)

		if err != nil {
			log.Println("Quitting!", err)
			break

		} else {

			processorChan <- cmd
//			ret, _ := r.db.HandleCommand(cmd)
//			searilzerChan <- ret
		}
	}


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
