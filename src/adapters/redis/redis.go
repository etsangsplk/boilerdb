package redis

import (
	"net"
	"log"
	"strconv"
	"errors"
	"fmt"
	"bufio"
	"io"
	"db"
	"reflect"
)

type RedisAdapter struct {
	db *db.DataBase
	listener net.Listener
	numClients uint
	isRunning bool
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
	switch (res.Kind()) {
		case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				writer.Write([]byte(fmt.Sprintf(":%d\r\n", res.Int())))
				break
		case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				writer.Write([]byte(fmt.Sprintf(":%d\r\n", res.Uint())))
				break
		case reflect.Slice, reflect.Array:
			l := res.Len()
			writer.Write([]byte(fmt.Sprintf("*%d\r\n", l)))

			for i := 0; i < l; i++ {
				v := res.Index(i).String()
				writer.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(v), v)))
			}
			break
		case reflect.Map:
			l := res.Len()*2
			writer.Write([]byte(fmt.Sprintf("*%d\r\n", l)))

			for _, k := range(res.MapKeys()) {
				v := string(res.MapIndex(k).Bytes())
				writer.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n$%d\r\n%s\r\n", len(k.String()), k.String(), len(v), v)))
			}
			break
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
			break

	default:
			s := res.String()
			writer.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)))
	}

	return ""
}

func (r *RedisAdapter) HandleConnection(c *net.TCPConn) error {
	var err error = nil

	reader := bufio.NewReader(c)
	writer := bufio.NewWriter(c)

	defer func(err *error, writer *bufio.Writer) {
		if e := recover(); e != nil {
			*err = e.(error)
			r.SerializeResponse(db.NewResult(db.NewError(db.E_UNKNOWN_ERROR)), writer)
			writer.Flush()
			c.Close()
			log.Printf("Error processing command: %s\n", e)

		}
	}(&err, writer)


	for err == nil && r.isRunning {
		cmd, err := ReadRequest(reader)

		if err != nil {
			log.Println("Quitting!", err)

		} else {
			ret, _ := r.db.HandleCommand(cmd)

			if ret != nil {
				r.SerializeResponse(ret, writer)
				err = writer.Flush()
			}
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
		case '*': {
			ll, err  := strconv.Atoi(string(buf[1:]))
			if err == nil {
				res := readMultiBulkData(reader, ll)
				if len(res) > 1 {
					return &db.Command{Command: string(res[0]), Key: string(res[1]), Args: res[2:], }, nil

				} else {
					return &db.Command{Command: string(res[0]), Key: "", Args: nil, }, nil
				}
			}
		}
		default: {
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

