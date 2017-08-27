package queue

import (
	"fmt"
	"io"
	"time"

	"github.com/onokonem/sillyQueueServer/db"
	"github.com/onokonem/sillyQueueServer/logiface"
	"github.com/onokonem/sillyQueueServer/queueproto"
	"github.com/onokonem/sillyQueueServer/tasks"
	"github.com/onokonem/sillyQueueServer/timeuuid"
)

// Server is a queueproto.QueueServer implementation
type Server struct {
	tasks         *tasks.Storage
	connects      *connectsStorage
	logger        logiface.Logger
	db            db.Connect
	senderBufSize int
	connTTL       time.Duration
	queueTTL      time.Duration
	acceptTTL     time.Duration
	processTTL    time.Duration
	dispatcher    chan *tasks.Task
}

// NewServer just creates a ne server instance
func NewServer(
	db db.Connect,
	senderBufSize int,
	connTTL time.Duration,
	queueTTL time.Duration,
	acceptTTL time.Duration,
	processTTL time.Duration,
	logger logiface.Logger,
) *Server {
	dispatcher := make(chan *tasks.Task)
	return &Server{
		tasks:         tasks.NewStorage(),
		connects:      newConnectsStorage(),
		logger:        logger,
		db:            db,
		senderBufSize: senderBufSize,
		connTTL:       connTTL,
		queueTTL:      queueTTL,
		acceptTTL:     acceptTTL,
		processTTL:    processTTL,
		dispatcher:    dispatcher,
	}
}

// queue method queues a task
func (s *Server) queue(
	task *queueproto.QueueTask,
) error {
	newTask := tasks.NewTask(task)

	if s.tasks.Get(newTask.ID()) != nil {
		return s.logger.Err("Duplicate task", "id", newTask.ID())
	}

	switch newTask.Persistence {
	case tasks.PersistenceWritetrough:
		s.db.SaveTasks(newTask)
	case tasks.PersistenceWriteback:
		s.db.Saver() <- newTask
	}

	dispatched := s.connects.foreach(
		func(connID connIDType, conn *connInfo) (bool, bool) {
			if conn.matchSubscribe(task.Queue, task.Subject) {
				// ToDo: possible global slowdown because of RLock on connects
				ok := s.dispatchTask(connID, conn, newTask)
				return ok, ok && newTask.Delivery == tasks.DeliveryAnyone
			}
			return false, false
		},
	)

	if newTask.Delivery == tasks.DeliveryEveryone {
		s.tasks.Add(newTask)
		return nil
	}

	switch {
	case newTask.Delivery == tasks.DeliveryEveryoneNowAndFuture:
		newTask.ChangeStatusAndExpire(
			tasks.StatusQueued,
			nil,
		)
	case dispatched > 0:
		newTask.ChangeStatusAndExpire(
			tasks.StatusDispatched,
			taskExpire(newTask, s.acceptTTL, tasks.StatusAcceptTimeout, s.dispatcher),
		)
	default:
		newTask.ChangeStatusAndExpire(
			tasks.StatusQueued,
			taskExpire(newTask, s.acceptTTL, tasks.StatusToRemove, s.dispatcher),
		)
	}

	switch newTask.Persistence {
	case tasks.PersistenceWritetrough:
		s.db.SaveTasks(newTask)
	case tasks.PersistenceWriteback:
		s.db.Saver() <- newTask
	}

	// ToDo: possible diplication
	s.tasks.Add(newTask)

	s.logger.Debug("Task queued", "id", newTask.ID(), "task", newTask)

	return nil
}

// Ack method process the acknowledgements coming from the clients
func (s *Server) Ack(ackServer queueproto.Queue_AckServer) error {
	connID := connIDType(timeuuid.TimeUUID().String())
	clientID := clientIDType("")
	conn := s.connects.add(connID, ackServer, s.senderBufSize, s.connTTL)
	defer s.connects.del(connID)

	go conn.senderLoop(connID, s.logger)

	s.logger.Debug("Connection opened", "conn", connID)

	for conn.isAlive() {
		in, err := ackServer.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return s.logger.Err("Recv error", "err", err, "conn", connID, "client", clientID)
		}

		conn.touch(s.connTTL)

		switch msg := in.GetMsg().(type) {
		case *queueproto.AckToHub_Attach:
			if clientID != "" {
				return s.logger.Err("Repeated attach", "conn", connID, "client", clientID, "new client", msg.Attach.Originator)
			}

			clientID = clientIDType(msg.Attach.Originator)
			s.connects.attach(connID, clientID)

			ackServer.Send(makeAckAttached(clientID))

			s.logger.Info("Attached", "conn", connID, "client", clientID)

		case *queueproto.AckToHub_Subscribe:
			conn.subscribe(msg.Subscribe.Queue, msg.Subscribe.Subject)
			ackServer.Send(makeAckSubscribed(clientID))

			dispatched := s.tasks.Foreach(
				func(id string, task *tasks.Task) (bool, bool) {
					if task.Delivery == tasks.DeliveryEveryoneNowAndFuture &&
						task.Sequence > msg.Subscribe.Sequence {
						// ToDo: possible global slowdown because of RLock on tasks
						ok := s.dispatchTask(connID, conn, task)
						return ok, false
					}
					return false, false
				},
			)

			s.logger.Info(
				"Subscribed",
				"conn", connID,
				"client", clientID,
				"queue", msg.Subscribe.Queue,
				"subj", msg.Subscribe.Subject,
				"dispatched", dispatched,
			)

		case *queueproto.AckToHub_Queue:
			err := s.queue(msg.Queue)
			if err != nil {
				ackServer.Send(makeAckRejected(msg.Queue.Id, err.Error()))
			}
			ackServer.Send(makeAckQueued(msg.Queue.Id))

		case *queueproto.AckToHub_Ping:
			ackServer.Send(makeAckPong())

		case *queueproto.AckToHub_Accepted:
			updateTaskStatus(s, msg.Accepted.Id, tasks.StatusAccepted)

		case *queueproto.AckToHub_InProgress:
			updateTaskStatus(s, msg.InProgress.Id, tasks.StatusInProgress)

		case *queueproto.AckToHub_Done:
			updateTaskStatus(s, msg.Done.Id, tasks.StatusDone)

		case *queueproto.AckToHub_Dismissed:
			updateTaskStatus(s, msg.Dismissed.Id, tasks.StatusDismissed)

		default:
			s.logger.Warn("Unknown", "msg", fmt.Sprintf("%#+v", in))
		}
	}

	s.logger.Debug("Disconnected", "conn", connID, "client", clientID)
	return nil
}

// DispatcherLoop does re-dispatch the tasks expired timer faired for
func (s *Server) DispatcherLoop() {
	for task := range s.dispatcher {
		switch task.Status {
		case tasks.StatusToRemove:
			s.tasks.Del(task.ID())
			s.saveTask(task)

		case tasks.StatusProcessTimeout:
			if task.Ack.Aborted {
				err := s.sendAck(clientIDType(task.Originator), makeAckAborted(task.ID()))
				if err != nil {
					s.logger.PrintErr("Sending Ack error", "err", err)
				}
			}
			s.redispatchTask(task)

		case tasks.StatusQueued, tasks.StatusAcceptTimeout:
			s.redispatchTask(task)

		default:
			s.logger.Panicf("Unexpected status: %v", task.Status)
		}
	}
}

func updateTaskStatus(s *Server, taskID string, status tasks.StatusType) {
	task := s.tasks.Get(taskID)
	if task == nil {
		s.logger.PrintErr("Task does not exists", "id", taskID)
		return
	}

	var err error
	switch status {
	case tasks.StatusAccepted:
		if task.Ack.Accepted {
			err = s.sendAck(clientIDType(task.Originator), makeAckAccepted(taskID))
		}
		if task.Delivery == tasks.DeliveryAnyone {
			task.ChangeStatusAndExpire(
				status,
				taskExpire(task, s.processTTL, tasks.StatusProcessTimeout, s.dispatcher),
			)
		}

	case tasks.StatusInProgress:
		if task.Delivery == tasks.DeliveryAnyone {
			task.ChangeStatusAndExpire(
				status,
				taskExpire(task, s.processTTL, tasks.StatusProcessTimeout, s.dispatcher),
			)
		}

	case tasks.StatusDone:
		if task.Ack.Done {
			err = s.sendAck(clientIDType(task.Originator), makeAckDone(taskID))
		}
		task.ChangeStatusAndExpire(
			status,
			taskExpire(task, s.queueTTL, tasks.StatusToRemove, s.dispatcher),
		)

	case tasks.StatusDismissed:
		if task.Ack.Dismissed {
			err = s.sendAck(clientIDType(task.Originator), makeAckDismissed(taskID))
		}
		task.ChangeStatusAndExpire(
			status,
			taskExpire(task, s.queueTTL, tasks.StatusToRemove, s.dispatcher),
		)

	default:
		s.logger.Panicf("Unexpected status: %q", status)
	}

	if err != nil {
		s.logger.PrintErr("Sending Ack error", "err", err)
	}

	s.saveTask(task)
}

// sendAck passes Ack to the client
func (s *Server) sendAck(client clientIDType, ack *queueproto.AckFromHub) error {
	_, conn := s.connects.getConn(clientIDType(client))

	if conn == nil {
		return s.logger.Err("No connection", "client", client)
	}

	return conn.send(ack)
}

func (s *Server) saveTask(task *tasks.Task) {
	if task.Delivery == tasks.DeliveryEveryone {
		return
	}

	switch task.Persistence {
	case tasks.PersistenceWritetrough:
		s.db.SaveTasks(task)
	case tasks.PersistenceWriteback:
		s.db.Saver() <- task
	}
}

func (s *Server) dispatchTask(
	connID connIDType,
	conn *connInfo,
	task *tasks.Task,
) bool {
	err := conn.send(makeAccept(task.Queue, task.ID(), task.Subject, task.Payload))
	if err != nil {
		s.logger.PrintErr("Sending error", "err", err)
		return false
	}

	if task.Delivery == tasks.DeliveryAnyone {
		task.ChangeStatusAndExpire(
			tasks.StatusDispatched,
			taskExpire(task, s.acceptTTL, tasks.StatusAcceptTimeout, s.dispatcher),
		)
	}

	s.logger.Debug("Task dispatched", "client", conn.client, "conn", connID, "id", task.ID(), "task", task)
	return true
}

func (s *Server) redispatchTask(task *tasks.Task) {
	if task.CTime.Before(time.Now().Add(-s.queueTTL)) {
		task.ChangeStatus(tasks.StatusToRemove)
		s.tasks.Del(task.ID())

		if task.Ack.Timeout {
			err := s.sendAck(clientIDType(task.Originator), makeAckTimeouted(task.ID()))
			if err != nil {
				s.logger.PrintErr("Sending Ack error", "err", err)
			}
		}
		s.saveTask(task)
		return
	}

	dispatched := s.connects.foreach(
		func(connID connIDType, conn *connInfo) (bool, bool) {
			if conn.matchSubscribe(task.Queue, task.Subject) {
				// ToDo: possible global slowdown because of RLock on connects
				ok := s.dispatchTask(connID, conn, task)
				return ok, ok
			}
			return false, false
		},
	)

	if dispatched > 0 {
		task.ChangeStatusAndExpire(
			tasks.StatusDispatched,
			taskExpire(task, s.acceptTTL, tasks.StatusAcceptTimeout, s.dispatcher),
		)
	} else {
		task.ChangeStatusAndExpire(
			tasks.StatusQueued,
			taskExpire(task, s.acceptTTL, tasks.StatusQueued, s.dispatcher),
		)
	}

	s.saveTask(task)
}

func taskExpire(
	task *tasks.Task,
	ttl time.Duration,
	status tasks.StatusType,
	dispatcher chan<- *tasks.Task,
) *time.Timer {
	return time.AfterFunc(
		ttl,
		func() {
			task.ChangeStatus(status)
			dispatcher <- task
		},
	)
}

// RestoreTask is used to restore the task from DB on start
func (s *Server) RestoreTask(task *tasks.Task) error {
	task.ChangeStatusAndExpire(
		task.Status,
		taskExpire(task, s.acceptTTL, task.Status, s.dispatcher),
	)

	s.tasks.Restore(task)

	return nil
}
