package queue_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/onokonem/sillyQueueServer/db"
	"github.com/onokonem/sillyQueueServer/logiface"
	"github.com/onokonem/sillyQueueServer/queue"
	"github.com/onokonem/sillyQueueServer/queueproto"
	"github.com/onokonem/sillyQueueServer/tasks"
	"github.com/onokonem/sillyQueueServer/timeuuid"
	"google.golang.org/grpc"
)

var server net.Listener
var clients map[string]*client
var lock sync.Mutex

func TestServer(t *testing.T) {
	logger := &testLogger{t}

	testAttach(logger)
	testSubscribeAllBut3(logger)
	testQueue1(logger)
	testQueue2(logger)
	testQueue3(logger)
}

func prepareServer(logger logiface.Logger) net.Listener {
	lock.Lock()
	defer lock.Unlock()

	if server != nil {
		return server
	}

	fileName := timeuuid.TimeUUID().String()

	dbConn, err := db.Open("bolt", fileName)
	if err != nil {
		logger.Panicf("%v", err)
	}
	defer dbConn.MustClose()
	os.Remove(fileName)

	tcpServer, err := net.Listen("tcp", "")
	if err != nil {
		logger.Fatalf("failed to listen on %q: %v", "", err)
	}

	logger.Info("Listening", "bind", tcpServer.Addr())

	grpcServer := grpc.NewServer()
	queueServer := queue.NewServer(
		dbConn,
		1024,
		time.Minute,
		time.Minute,
		time.Second,
		time.Second*5,
		logger,
	)

	go dbConn.SaverLoop(time.Second, 1024, logger)
	go queueServer.DispatcherLoop()

	queueproto.RegisterQueueServer(grpcServer, queueServer)

	go func() {
		err = grpcServer.Serve(tcpServer)
		if err != nil {
			logger.Fatalf("Unexpected error: %v", err)
		}
	}()

	server = tcpServer
	return server
}

func prepareClients(tcpServer net.Listener, logger logiface.Logger) map[string]*client {
	lock.Lock()
	defer lock.Unlock()

	if clients != nil {
		return clients
	}

	clients = map[string]*client{
		"sender1":   nil,
		"receiver1": nil,
		"receiver2": nil,
		"receiver3": nil,
	}

	var err error
	for clientName := range clients {
		clients[clientName], err = newClient(tcpServer.Addr().String(), clientName, logger)
		if err != nil {
			logger.Fatalf("Unexpected error creating client %q: %v", clientName, err)
		}
	}

	for _, client := range clients {
		go client.sendLoop()
		go client.recvLoop()
	}

	return clients
}

type client struct {
	ctx         context.Context
	clientID    string
	out         chan *queueproto.AckToHub
	in          chan *queueproto.AckFromHub
	logger      logiface.Logger
	conn        *grpc.ClientConn
	queueClient queueproto.QueueClient
	ackClient   queueproto.Queue_AckClient
}

func newClient(
	addr string,
	clientID string,
	logger logiface.Logger,
) (cln *client, err error) {
	cln = &client{
		ctx:      context.Background(),
		clientID: clientID,
		out:      make(chan *queueproto.AckToHub, 1),
		in:       make(chan *queueproto.AckFromHub, 1),
		logger:   logger,
	}

	cln.conn, err = grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	cln.queueClient = queueproto.NewQueueClient(cln.conn)

	cln.ackClient, err = cln.queueClient.Ack(cln.ctx)
	if err != nil {
		return nil, err
	}

	return cln, nil
}

func (c *client) sendLoop() {
	c.logger.Debug("Send loop started", "client", c.clientID)
	for ack := range c.out {
		err := c.ackClient.Send(ack)
		if err != nil {
			c.logger.Fatalf("Error sending to server: client %q: %v", c.clientID, err)
			return
		}
	}
	c.logger.Debug("Send loop finished", "client", c.clientID)
}

func (c *client) recvLoop() {
	c.logger.Debug("Recv loop started", "client", c.clientID)
	defer close(c.in)
	for {
		ack, err := c.ackClient.Recv()
		if err != nil {
			c.logger.Fatalf("Error reading from server: client %q: %v", c.clientID, err)
			return
		}
		c.in <- ack
	}
	c.logger.Debug("Recv loop finished", "client", c.clientID)
}

//////////////////////////////////////////////////////////////////////

type testLogger struct {
	t *testing.T
}

func (l *testLogger) Err(msg interface{}, keyvals ...interface{}) error {
	l.t.Log(append(append(make([]interface{}, 0, len(keyvals)+1), getCaller(1), msg), keyvals...))

	if err, ok := msg.(error); ok {
		return err
	}
	for _, keyval := range keyvals {
		if err, ok := keyval.(error); ok {
			return err
		}
	}
	return fmt.Errorf("%s", msg)
}

func (l *testLogger) PrintErr(msg interface{}, keyvals ...interface{}) {
	l.t.Log(append(append(make([]interface{}, 0, len(keyvals)+1), getCaller(1), msg), keyvals...))
}

func (l *testLogger) Warn(msg interface{}, keyvals ...interface{}) {
	l.t.Log(append(append(make([]interface{}, 0, len(keyvals)+1), getCaller(1), msg), keyvals...))
}

func (l *testLogger) Info(msg interface{}, keyvals ...interface{}) {
	l.t.Log(append(append(make([]interface{}, 0, len(keyvals)+1), getCaller(1), msg), keyvals...))
}

func (l *testLogger) Debug(msg interface{}, keyvals ...interface{}) {
	l.t.Log(append(append(make([]interface{}, 0, len(keyvals)+1), getCaller(1), msg), keyvals...))
}

func (l *testLogger) Fatalf(format string, v ...interface{}) {
	l.t.Fatalf(format, v...)
}

func (l *testLogger) Panicf(format string, v ...interface{}) {
	//
	l.t.Fatalf(format, v...)
}

func getCaller(stackBack int) string {
	_, file, line, ok := runtime.Caller(stackBack + 1)
	if !ok {
		return "UNKNOWN"
	}

	if li := strings.LastIndex(file, "/"); li > 0 {
		file = file[li+1:]
	}

	return fmt.Sprintf("%s:%d", file, line)
}

///////////////////////////////////////////////////

func testAttach(logger logiface.Logger) {
	clients := prepareClients(prepareServer(logger), logger)

	for clientName, client := range clients {
		client.out <- &queueproto.AckToHub{
			Msg: &queueproto.AckToHub_Attach{
				Attach: &queueproto.Attach{
					Originator: clientName,
				},
			},
		}
	}

	maxCount := len(clients)
	count := 0
	var wg sync.WaitGroup
	stopper := make(chan bool)

	names := make([]string, 1, len(clients)+1)
	cases := make([]reflect.SelectCase, 1, len(clients)+1)
	names[0] = "stopper"
	cases[0] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(stopper)}

	for clientName, client := range clients {
		names = append(names, clientName)
		cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(client.in)})
	}

	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			chosen, value, _ := reflect.Select(cases)

			if chosen == 0 {
				break
			}

			ack, ok := value.Interface().(*queueproto.AckFromHub)
			if !ok {
				logger.PrintErr("Unexpected message", "client", names[chosen], "message", value)
			}

			attached := ack.GetAttached()
			if attached == nil || attached.Originator != names[chosen] {
				logger.PrintErr("Unexpected message", "client", names[chosen], "message", ack)
			}

			count++
		}
	}()

	time.Sleep(time.Second)

	close(stopper)
	wg.Wait()

	if count != maxCount {
		logger.Fatalf("Wrong delivery count: got %v but %v expected", count, maxCount)
	}
}

func testSubscribeAllBut3(logger logiface.Logger) {
	clients := prepareClients(prepareServer(logger), logger)

	for clientName, client := range clients {
		if clientName == "receiver3" {
			continue
		}

		client.out <- &queueproto.AckToHub{
			Msg: &queueproto.AckToHub_Subscribe{
				Subscribe: &queueproto.Subscribe{
					Queue:   []string{clientName, "common"},
					Subject: []string{""},
				},
			},
		}
	}

	maxCount := len(clients) - 1
	count := 0
	var wg sync.WaitGroup
	stopper := make(chan bool)

	names := make([]string, 1, len(clients)+1)
	cases := make([]reflect.SelectCase, 1, len(clients)+1)
	names[0] = "stopper"
	cases[0] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(stopper)}

	for clientName, client := range clients {
		names = append(names, clientName)
		cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(client.in)})
	}

	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			chosen, value, _ := reflect.Select(cases)

			if chosen == 0 {
				break
			}

			ack, ok := value.Interface().(*queueproto.AckFromHub)
			if !ok {
				logger.PrintErr("Unexpected message", "client", names[chosen], "message", value)
			}

			subscribed := ack.GetSubscribed()
			if subscribed == nil || subscribed.Originator != names[chosen] {
				logger.PrintErr("Unexpected message", "client", names[chosen], "message", ack)
			}

			count++
		}
	}()

	time.Sleep(time.Second)

	close(stopper)
	wg.Wait()

	if count != maxCount {
		logger.Fatalf("Wrong delivery count: got %v but %v expected", count, maxCount)
	}
}

func testQueue1(logger logiface.Logger) {
	clients := prepareClients(prepareServer(logger), logger)

	clients["sender1"].out <- &queueproto.AckToHub{
		Msg: &queueproto.AckToHub_Queue{
			Queue: &queueproto.QueueTask{
				Originator: "sender1",
				Id:         timeuuid.TimeUUID().String(),
				Queue:      "receiver1",
				Subject:    "test1",
				Delivery:   queueproto.QueueTask_Delivery(tasks.DeliveryAnyone),
				Ack: &queueproto.QueueTask_Acknowledgement{
					Accepted: true,
				},
			},
		},
	}

	maxCount := 3
	count := 0
	var wg sync.WaitGroup
	stopper := make(chan bool)

	names := make([]string, 1, len(clients)+1)
	cases := make([]reflect.SelectCase, 1, len(clients)+1)
	names[0] = "stopper"
	cases[0] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(stopper)}

	for clientName, client := range clients {
		names = append(names, clientName)
		cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(client.in)})
	}

	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			chosen, value, _ := reflect.Select(cases)

			if chosen == 0 {
				break
			}

			ack, ok := value.Interface().(*queueproto.AckFromHub)
			if !ok {
				logger.PrintErr("Unexpected message", "client", names[chosen], "message", value)
			}

			switch names[chosen] {
			case "sender1":
				switch typedAck := ack.Msg.(type) {
				case *queueproto.AckFromHub_Queued:
					logger.Debug("Queued", "client", names[chosen], "message", typedAck)
				case *queueproto.AckFromHub_Accepted:
					logger.Debug("Accepted", "client", names[chosen], "message", typedAck)
				default:
					logger.PrintErr("Unexpected message", "client", names[chosen], "message", ack)
				}
			default:
				switch typedAck := ack.Msg.(type) {
				case *queueproto.AckFromHub_Accept:
					logger.Debug("Arrived", "client", names[chosen], "message", typedAck)
					clients[names[chosen]].out <- &queueproto.AckToHub{
						Msg: &queueproto.AckToHub_Accepted{
							Accepted: &queueproto.Accepted{
								Id: typedAck.Accept.Id,
							},
						},
					}
				default:
					logger.PrintErr("Unexpected message", "client", names[chosen], "message", ack)
				}
			}
			count++
		}
	}()

	time.Sleep(time.Second)

	close(stopper)
	wg.Wait()

	if count != maxCount {
		logger.Fatalf("Wrong delivery count: got %v but %v expected", count, maxCount)
	}
}

func testQueue2(logger logiface.Logger) {
	clients := prepareClients(prepareServer(logger), logger)

	clients["sender1"].out <- &queueproto.AckToHub{
		Msg: &queueproto.AckToHub_Queue{
			Queue: &queueproto.QueueTask{
				Originator: "sender1",
				Id:         timeuuid.TimeUUID().String(),
				Queue:      "common",
				Subject:    "test2",
				Delivery:   queueproto.QueueTask_Delivery(tasks.DeliveryEveryoneNowAndFuture),
				Ack: &queueproto.QueueTask_Acknowledgement{
					Accepted: true,
				},
			},
		},
	}

	maxCount := 7
	count := 0
	var wg sync.WaitGroup
	stopper := make(chan bool)

	names := make([]string, 1, len(clients)+1)
	cases := make([]reflect.SelectCase, 1, len(clients)+1)
	names[0] = "stopper"
	cases[0] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(stopper)}

	for clientName, client := range clients {
		names = append(names, clientName)
		cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(client.in)})
	}

	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			chosen, value, _ := reflect.Select(cases)

			if chosen == 0 {
				break
			}

			ack, ok := value.Interface().(*queueproto.AckFromHub)
			if !ok {
				logger.PrintErr("Unexpected message", "client", names[chosen], "message", value)
			}

			switch names[chosen] {
			case "sender1":
				switch typedAck := ack.Msg.(type) {
				case *queueproto.AckFromHub_Queued:
					logger.Debug("Queued", "client", names[chosen], "message", typedAck)
				case *queueproto.AckFromHub_Accepted:
					logger.Debug("Accepted", "client", names[chosen], "message", typedAck)
				case *queueproto.AckFromHub_Accept:
					logger.Debug("Arrived", "client", names[chosen], "message", typedAck)
					clients[names[chosen]].out <- &queueproto.AckToHub{
						Msg: &queueproto.AckToHub_Accepted{
							Accepted: &queueproto.Accepted{
								Id: typedAck.Accept.Id,
							},
						},
					}
				default:
					logger.PrintErr("Unexpected message", "client", names[chosen], "message", ack)
				}
			default:
				switch typedAck := ack.Msg.(type) {
				case *queueproto.AckFromHub_Accept:
					logger.Debug("Arrived", "client", names[chosen], "message", typedAck)
					clients[names[chosen]].out <- &queueproto.AckToHub{
						Msg: &queueproto.AckToHub_Accepted{
							Accepted: &queueproto.Accepted{
								Id: typedAck.Accept.Id,
							},
						},
					}
				default:
					logger.PrintErr("Unexpected message", "client", names[chosen], "message", ack)
				}
			}
			count++
		}
	}()

	time.Sleep(time.Second)

	close(stopper)
	wg.Wait()

	if count != maxCount {
		logger.Fatalf("Wrong delivery count: got %v but %v expected", count, maxCount)
	}
}

func testQueue3(logger logiface.Logger) {
	clients := prepareClients(prepareServer(logger), logger)

	clientName := "receiver3"
	clients[clientName].out <- &queueproto.AckToHub{
		Msg: &queueproto.AckToHub_Subscribe{
			Subscribe: &queueproto.Subscribe{
				Queue:    []string{clientName, "common"},
				Subject:  []string{""},
				Sequence: -1,
			},
		},
	}

	maxCount := 3
	count := 0
	var wg sync.WaitGroup
	stopper := make(chan bool)

	names := make([]string, 1, len(clients)+1)
	cases := make([]reflect.SelectCase, 1, len(clients)+1)
	names[0] = "stopper"
	cases[0] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(stopper)}

	for clientName, client := range clients {
		names = append(names, clientName)
		cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(client.in)})
	}

	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			chosen, value, _ := reflect.Select(cases)

			if chosen == 0 {
				break
			}

			ack, ok := value.Interface().(*queueproto.AckFromHub)
			if !ok {
				logger.PrintErr("Unexpected message", "client", names[chosen], "message", value)
			}

			switch names[chosen] {
			case "sender1":
				switch typedAck := ack.Msg.(type) {
				case *queueproto.AckFromHub_Queued:
					logger.Debug("Queued", "client", names[chosen], "message", typedAck)
				case *queueproto.AckFromHub_Accepted:
					logger.Debug("Accepted", "client", names[chosen], "message", typedAck)
				case *queueproto.AckFromHub_Accept:
					logger.Debug("Arrived", "client", names[chosen], "message", typedAck)
					clients[names[chosen]].out <- &queueproto.AckToHub{
						Msg: &queueproto.AckToHub_Accepted{
							Accepted: &queueproto.Accepted{
								Id: typedAck.Accept.Id,
							},
						},
					}
				default:
					logger.PrintErr("Unexpected message", "client", names[chosen], "message", ack)
				}
			default:
				switch typedAck := ack.Msg.(type) {
				case *queueproto.AckFromHub_Subscribed:
				case *queueproto.AckFromHub_Accept:
					logger.Debug("Arrived", "client", names[chosen], "message", typedAck)
					clients[names[chosen]].out <- &queueproto.AckToHub{
						Msg: &queueproto.AckToHub_Accepted{
							Accepted: &queueproto.Accepted{
								Id: typedAck.Accept.Id,
							},
						},
					}
				default:
					logger.PrintErr("Unexpected message", "client", names[chosen], "message", ack)
				}
			}
			count++
		}
	}()

	time.Sleep(time.Second)

	close(stopper)
	wg.Wait()

	if count != maxCount {
		logger.Fatalf("Wrong delivery count: got %v but %v expected", count, maxCount)
	}
}
