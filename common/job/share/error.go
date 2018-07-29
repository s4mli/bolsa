package share

import "fmt"

type strategyType int

const (
	TypeLabor strategyType = iota
	TypeRetry
)

func (ht *strategyType) String() string {
	switch *ht {
	case TypeLabor:
		return "labor"
	case TypeRetry:
		return "retry"
	}
	return "?"
}

////////////////
// Job Error //
type Error struct {
	T strategyType
	error
}

func (je Error) Error() string {
	return fmt.Sprintf("✗ %s failed %s", je.T.String(), je.error.Error())
}
func NewError(st strategyType, err error) *Error { return &Error{st, err} }
