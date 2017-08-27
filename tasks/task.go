package tasks

import (
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/onokonem/sillyQueueServer/queueproto"
)

var sequence = int64(0)

// Task as it will be serialized to DB
type Task struct {
	id          string
	Queue       string
	Subject     string
	Originator  string
	Timeout     time.Duration
	Persistence persistenceType
	Delivery    deliveryType
	Ack         Acks
	Payload     []byte
	Status      StatusType
	CTime       time.Time

	Sequence int64

	expire *time.Timer

	lock sync.RWMutex
}

// StatusType is a type for the task statuses
type StatusType int8

// Status values (must be the same as in queueproto)
const (
	StatusQueued         StatusType = 0
	StatusAccepted       StatusType = 1
	StatusDone           StatusType = 2
	StatusTimedout       StatusType = 3
	StatusAborted        StatusType = 4
	StatusDismissed      StatusType = 5
	StatusDispatched     StatusType = 6
	StatusInProgress     StatusType = 8
	StatusToRemove       StatusType = -1
	StatusAcceptTimeout  StatusType = -2
	StatusProcessTimeout StatusType = -3
)

type persistenceType int8

// Persistence types (must be the same as in queueproto)
const (
	PersistenceWritetrough persistenceType = 0
	PersistenceWriteback   persistenceType = 1
	PersistenceNone        persistenceType = 2
)

type deliveryType int8

// Delivery types (must be the same as in queueproto)
const (
	DeliveryAnyone               deliveryType = 0
	DeliveryEveryone             deliveryType = 1
	DeliveryEveryoneNowAndFuture deliveryType = 2
)

// ChangeExpire changes the task expire timer
func (t *Task) ChangeExpire(expire *time.Timer) {
	t.lock.Lock()
	defer t.lock.Unlock()

	if t.expire != nil {
		t.expire.Stop()
	}

	t.expire = expire
}

// ChangeStatus changes the task status (surprise)
func (t *Task) ChangeStatus(status StatusType) {
	t.lock.Lock()
	defer t.lock.Unlock()

	t.Status = status
}

// ChangeStatusAndExpire changes the task status and expire (surprise)
func (t *Task) ChangeStatusAndExpire(status StatusType, expire *time.Timer) {
	t.lock.Lock()
	defer t.lock.Unlock()

	t.Status = status

	if t.expire != nil {
		t.expire.Stop()
	}

	t.expire = expire
}

// ID returns task id (surprise)
func (t *Task) ID() string {
	return t.id
}

// SetID sets task id (surprise)
func (t *Task) SetID(id string) {
	t.id = id
}

var emptyByteSlice = make([]byte, 0)

// Marshal marshals Task to the JSON bytes
func (t *Task) Marshal() []byte {
	if t == nil {
		return emptyByteSlice
	}
	t.lock.RLock()
	defer t.lock.RUnlock()

	buf, err := json.Marshal(t)
	if err != nil {
		panic(err)
	}
	return buf
}

// Acks is a struct to hold a list of acknowledgements requested for the task
type Acks struct {
	Accepted  bool
	Done      bool
	Timeout   bool
	Aborted   bool
	Dismissed bool
}

// NewTask builds a task from queueproto.QueueTask. Just in case we will have to change proto and DB scheme independently
func NewTask(protoTask *queueproto.QueueTask) *Task {
	task := Task{
		id:          protoTask.Id,
		Queue:       protoTask.Queue,
		Subject:     protoTask.Subject,
		Originator:  protoTask.Originator,
		Timeout:     time.Duration(protoTask.Timeout) * time.Millisecond,
		Persistence: persistenceType(protoTask.Persistence),
		Delivery:    deliveryType(protoTask.Delivery),
		Payload:     protoTask.Payload,
		Sequence:    atomic.AddInt64(&sequence, 1),
	}

	if protoTask.Ack != nil {
		task.Ack.Accepted = protoTask.Ack.Accepted
		task.Ack.Done = protoTask.Ack.Done
		task.Ack.Timeout = protoTask.Ack.Timeout
		task.Ack.Aborted = protoTask.Ack.Aborted
		task.Ack.Dismissed = protoTask.Ack.Dismissed
	}

	return &task
}
