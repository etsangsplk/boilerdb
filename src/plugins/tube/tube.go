/**
 * Created with IntelliJ IDEA.
 * User: dvirsky
 * Date: 1/16/13
 * Time: 12:21 AM
 * To change this template use File | Settings | File Templates.
 */
package tube

import (
	"os"
)

type Tube struct {
	Name       string
	Signature  string
	writers    []Writer
	readers    []interface{}
	maxReaders int
	maxWriters int
	parallel   bool
	continuous bool
	fp         *os.File
}

func NewTube(name, signature string, maxReaders, maxWriters int, parallel bool, continuous bool) (*Tube, error) {

	return nil, nil
}

func (t *Tube) GetReader(name string, offset int) *Reader {

	return nil
}

func (t *Tube) GetWriter(name string, offset int) *Reader {
	return nil
}

type Writer struct {
}

type Reader struct {
}
