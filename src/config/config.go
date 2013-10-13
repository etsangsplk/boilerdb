package config

import (
)

var (
	//WORKING_DIRECTORY is where we'll save the dump files
	WORKING_DIRECTORY = "/tmp"

	//IN_CHAN_BUFSIZE is the buffer on the input channel of sessions
	IN_CHAN_BUFSIZE  int = 200

	//OUT_CHAN_BUFSIZE is the buffer on the output channel of sessions
	OUT_CHAN_BUFSIZE int = 100

	//SINK_CHANNEL_SIZE is the buffer on a command sink's channel
	SINK_CHANNEL_SIZE = 100

	// BGSAVE_SECONDS tells the server to save every N seconds if the database
	// has changed
	// set to 0 for no saving
	BGSAVE_SECONDS = 120

	// LISTEN_PORT defines the listening port of the database
	LISTEN_PORT = 2000

	// MAX_SYNC_RETRIES defines how many times to try resyncing the replication
	MAX_SYNC_RETRIES = 5
)

func init() {

}
