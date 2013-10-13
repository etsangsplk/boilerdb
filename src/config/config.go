package config

import ()

var (
	//WorkingDirectory is where we'll save the dump files
	WorkingDirectory = "/tmp"

	//InChanBufsize is the buffer on the input channel of sessions
	InChanBufsize = 200

	//OutChanBufsize is the buffer on the output channel of sessions
	OutChanBufsize = 100

	//SinkChannelSize is the buffer on a command sink's channel
	SinkChannelSize = 100

	// BgsaveSeconds tells the server to save every N seconds if the database
	// has changed
	// set to 0 for no saving
	BgsaveSeconds = 120

	// ListenPort defines the listening port of the database
	ListenPort = 2000

	// MaxSyncRetries defines how many times to try resyncing the replication
	MaxSyncRetries = 5
)

func init() {

}
