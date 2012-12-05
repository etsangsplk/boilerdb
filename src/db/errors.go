
package db

const (
	E_UNKNOWN_ERROR int = 0
	E_INVALID_COMMAND int = 2
	E_TYPE_MISMATCH int = 3
	E_INVALID_PARAMS int = 4
	E_NOT_ENOUGH_PARAMS int = 5
	E_BGSAVE_IN_PROGRESS int = 6
	E_LOAD_IN_PROGRESS int = 7
	E_PLUGIN_ERROR int = 8
)


var errorCodes map[int]string = map[int]string {
	E_UNKNOWN_ERROR: "Unknwon Error",
	E_INVALID_COMMAND: "Invalid Command",
	E_TYPE_MISMATCH: "Type Mismatch",
	E_INVALID_PARAMS: "Invalid parameters for call",
	E_NOT_ENOUGH_PARAMS: "Not enough params for command",
	E_BGSAVE_IN_PROGRESS: "BGSAVE in progress",
	E_LOAD_IN_PROGRESS: "LOAD in progress...",
	//plugin error is no in the map...

}

//Internal error codes with preset messages
type Error struct {
	Code int

}

// Custom plugin errors with plugin name and custom message
type PluginError struct {
	Error
	PluginName string
	Message string

}

// Statuses with OK error code
type Status struct {
	Error
	Str string
}

func NewPluginError(pluginName string, msg string) *PluginError {
	return &PluginError {
		Error: Error{E_PLUGIN_ERROR},
		PluginName: pluginName,
		Message: msg,
	}
}



func (e *Error) ToString() string {

	msg, ok := errorCodes[e.Code]
	if ok {
		return msg
	}
	return ""

}


func NewError(code int) *Error {
	return &Error{code}
}

func NewStatus(str string) *Status {
	return &Status{Error {0}, str}
}
