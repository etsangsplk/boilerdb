/**
 * Created with IntelliJ IDEA.
 * User: dvirsky
 * Date: 11/15/12
 * Time: 10:46 PM
 * To change this template use File | Settings | File Templates.
 */
package db

const (
	E_UNKNOWN_ERROR int = 0
	E_INVALID_COMMAND int = 2
	E_TYPE_MISMATCH int = 3
	E_INVALID_PARAMS int = 4
	E_NOT_ENOUGH_PARAMS int = 5
	E_BGSAVE_IN_PROGRESS int = 6
	E_LOAD_IN_PROGRESS int = 7
)


var errorCodes map[int]string = map[int]string {
	E_UNKNOWN_ERROR: "Unknwon Error",
	E_INVALID_COMMAND: "Invalid Command",
	E_TYPE_MISMATCH: "Type Mismatch",
	E_INVALID_PARAMS: "Invalid parameters for call",
	E_NOT_ENOUGH_PARAMS: "Not enough params for command",
	E_BGSAVE_IN_PROGRESS: "BGSAVE in progress",
	E_LOAD_IN_PROGRESS: "LOAD in progress...",

}

type Error struct {
	Code int
}

func (e *Error) ToString() string {
	return errorCodes[e.Code]
}

type Status struct {
	Error
	Str string
}

func NewError(code int) *Error {
	return &Error{code}
}

func NewStatus(str string) *Status {
	return &Status{Error {0}, str}
}
