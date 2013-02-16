//The redis protocol adapter, currently the only adapter in town :)
//
//
//It wraps the database and povides network access to it.
//
//We've made a great deal of effort decoupling the network layer from the database itself,
//using channels to abstract streams for PubSub type scenarios.
//
//Writing other network protocols should be pretty trivial ;)
//
package redis



import (
	"bufio"
	"db"
	"errors"
	"fmt"
	"io"
	"logging"
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

func (r *RedisAdapter) Init(d *db.DataBase) {
	r.db = d
}

func (r *RedisAdapter) Listen(addr net.Addr) error {
	listener, err := net.Listen(addr.Network(), addr.String())

	if err != nil {
		return err
	}

	logging.Info("Redis adapter listening on %s", addr)
	r.listener = listener
	return nil
}

// Take a response as an abstract interface, and serialize it to the redis protocol.
//
// Allowed response types are:
//
//
// 1. Integers of all types
//
// 2. Strings
//
// 3. booleans (converted to strings)
//
// 4. db Status
//
// 5. db Error
//
// 6. db Command (serialized as string, for monitoring)
//
// 7. nil
//
// 8. []interface{} (serialized recursively)
//
// 9. map[string]interface{} (serialized recuresively)
//
// 10. float32/64 (serialized as string,)
func SerializeResponse(res interface{}, writer io.Writer) error {



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
			err := SerializeResponse(arr[i], writer )
			if err != nil {
				logging.Panic("Could not serialize response: %s", err)
			}
		}

	case *db.Status:
		status := res.(*db.Status)
		writer.Write([]byte(fmt.Sprintf("+%s\r\n", status.Str)))

	case *db.Command:

		cmd := res.(*db.Command)

		if cmd.Key != "" {
			l := len(cmd.Args) + 2

			writer.Write([]byte(fmt.Sprintf("*%d\r\n", l)))
		}
		writer.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(cmd.Command), cmd.Command)))

		if cmd.Key == "" {
			break
		}

		writer.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(cmd.Key), cmd.Key)))

		//recursively serialize all entries in the list
		for i := 0; i < len(cmd.Args); i++ {
			err := SerializeResponse(string(cmd.Args[i]), writer )
			if err != nil {
				logging.Panic("Could not serialize response: %s", err)
			}
		}

	case *db.Error:
		e := res.(*db.Error)
		writer.Write([]byte(fmt.Sprintf("-ERR %d: %s\r\n", e.Code, e.ToString())))

	case *db.PluginError:
		e := res.(*db.PluginError)

		writer.Write([]byte(fmt.Sprintf("-Error in plugin '%s': %s\r\n", e.PluginName, e.Message)))

	case map[string]interface{}:
		m := res.(map[string]interface{})
		writer.Write([]byte(fmt.Sprintf("*%d\r\n", len(m)*2)))
		for k, _ := range m {
			writer.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(k), k)))
			err := SerializeResponse(m[k], writer )
			if err != nil {
				log.Panic(err)
			}
		}

	default:
		writer.Write([]byte(fmt.Sprintf("-ERR Unknown type '%s'\r\n", )))
		logging.Error("Unknown type '%s'. Could not serialize", reflect.TypeOf(res))
		return fmt.Errorf("Unknown type '%s'. Could not serialize", reflect.TypeOf(res))

	}

	return nil
}

// the connection handling loop.
//
// This gets called for each connection initialized
func (r *RedisAdapter) HandleConnection(c *net.TCPConn) error {
	var err error = nil

	fp, _ := c.File()
	_ = fp

	reader := bufio.NewReaderSize(c, 32768)
	writer := bufio.NewWriterSize(c, 32768)

	session := r.db.NewSession(c.RemoteAddr())

	//error handler - write an error message to the socket and close it
	defer func(err *error, writer *bufio.Writer) {
		if e := recover(); e != nil {

			defer func() {
				ee := recover()
				if ee != nil {
					logging.Error("Error handling error :) %s", ee)
				}
			}()

			*err = e.(error)
			session.Stop()
			c.Close()

			if *err != io.EOF && *err != ReadError {
				logging.Error("Error processing command: %s\n", e)
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
				SerializeResponse(msg.Value, writer)
			} else {
				SerializeResponse(nil, writer)
			}
			err = writer.Flush()
			if err != nil {

				session.Stop()

				break

			}

		}
		logging.Debug("Stopping Serializer....\n")
	}()

	//the request reading loop
	for err == nil && r.isRunning && session.IsRunning {

		//read an parse the request
		cmd, err := ReadRequest(reader)

		if err != nil {
			logging.Info("Quitting!", err)
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


// Start the redis adapter and listen to its port.
//
// Returns an error if something went wrong (usually the port is already taken...)
//
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

// Stop the adapter. Currently it doesn't wait for everything to exit. TBD...
func (r *RedisAdapter) Stop() error {
	r.isRunning = false

	return nil
}

//
// 	Return the name of the adapter
func (r *RedisAdapter) Name() string {
	return "Redis"
}

//
// Read one request from the redis protocol
//
// This is based on the client code of Go-redis: https://github.com/alphazero/Go-Redis
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

// hard coded error to be detected upstream
var ReadError = errors.New("Error Reading from Client")

// Everything below is taken from https://github.com/alphazero/Go-Redis



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
	//var buf []byte


	buf, _, e := r.ReadLine()
	if e != nil {
		logging.Info("Error: %s", e)
		panic(ReadError)
	}

	return buf

	var b byte
	b, e = r.ReadByte()
	if e != nil {
		logging.Info("readToCRLF - ReadByte", e)
		panic(ReadError)
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
