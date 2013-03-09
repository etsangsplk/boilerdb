/**
 * Created with IntelliJ IDEA.
 * User: dvirsky
 * Date: 11/16/12
 * Time: 4:13 AM
 * To change this template use File | Settings | File Templates.
 */

package config
import (
//	goconf "code.google.com/p/goconf/conf"
//	"camlistore.org/pkg/serverconfig"

//"reflect"
	)

var (
	//where we'll save the dump files
	WORKING_DIRECTORY     = "/tmp"

	//the buffer on the input and output channels of sessions
	IN_CHAN_BUFSIZE   int = 10
	OUT_CHAN_BUFSIZE  int = 5

	//the buffer on a command sink's channel
	SINK_CHANNEL_SIZE = 100


	// save every N seconds if the database has changed
	// set to 0 for no saving
	// setting the value too low will cause us to try the next time, no biggie
	BGSAVE_SECONDS = 120

	// The listening port of the database
	LISTEN_PORT = 2000

	MAX_SYNC_RETRIES = 5
)

func init() {


}
