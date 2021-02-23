package clustercache


import (
	"github.com/pkg/errors"
)

var (
	errCreatedCluster = errors.New("cluster failed to be created")
)

type fatalError struct {
	reason string
}

func (fe *fatalError) Error() string {
	return fe.reason
}

func newFatalError(reason string) *fatalError {
	return &fatalError{reason}
}

func isFatalError(err error) bool {
	switch errors.Cause(err).(type) {
	case *fatalError:
		return true
	default:
		return false
	}
}

