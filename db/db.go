package db

import (
	"fmt"
	"time"

	"github.com/onokonem/sillyQueueServer/logiface"
	"github.com/onokonem/sillyQueueServer/tasks"
)

var (
	errAlreadyExists = fmt.Errorf("Key already exists")
	errConnectClosed = fmt.Errorf("Connect closed")
	// errBadRecord     = fmt.Errorf("Record damaged")
)

// ForeachFunc is a type of func to be passed to Foreach method
type ForeachFunc func(task *tasks.Task) error

// Connect interface represents a database connection with al the necessary methods
type Connect interface {
	SaveTasks(...*tasks.Task) error
	Close() error
	MustClose()
	String() string
	GetTask(string) (*tasks.Task, error)
	Saver() chan<- *tasks.Task
	SaverLoop(time.Duration, int, logiface.Logger)
	Foreach(ForeachFunc) (int, error)
}

// Open creates a new DB connection
func Open(driverName, dataSourceName string) (Connect, error) {
	switch driverName {
	case "bolt":
		return newBoltDB(dataSourceName)
	default:
		panic(fmt.Errorf("Unsupported database type %q", driverName))
	}
}
