package queue

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/onokonem/sillyQueueServer/logiface"
	"github.com/onokonem/sillyQueueServer/queueproto"
)

type connInfo struct {
	server     queueproto.Queue_AckServer
	client     clientIDType
	closed     bool
	queue      []string
	subj       []string
	subscribed bool

	sender chan *queueproto.AckFromHub
	expire *time.Timer

	sync.RWMutex
}

func newConnInfo(server queueproto.Queue_AckServer, senderBufSize int, ttl time.Duration) *connInfo {
	c := &connInfo{
		server: server,
		sender: make(chan *queueproto.AckFromHub, senderBufSize),
	}
	c.expire = time.AfterFunc(ttl, func() { c.close() })

	return c
}

func (c *connInfo) close() {
	if c == nil {
		return
	}

	c.Lock()
	defer c.Unlock()

	if !c.closed {
		close(c.sender)
		c.closed = true
	}
}

func (c *connInfo) isAlive() bool {
	if c == nil {
		return false
	}

	c.RLock()
	defer c.RUnlock()

	return !c.closed

}

func (c *connInfo) attach(clientID clientIDType) {
	c.Lock()
	defer c.Unlock()

	c.client = clientID
}

func (c *connInfo) touch(ttl time.Duration) {
	c.Lock()
	defer c.Unlock()

	if c.expire.Stop() {
		c.expire = time.AfterFunc(ttl, func() { c.close() })
	}
}

func (c *connInfo) subscribe(queue []string, subj []string) {
	c.Lock()
	defer c.Unlock()

	c.queue = queue
	c.subj = subj
	c.subscribed = true
}

func (c *connInfo) matchSubscribe(queue string, subj string) bool {
	c.RLock()
	defer c.RUnlock()

	return c.subscribed && matchList(queue, c.queue) && matchList(subj, c.subj)
}

func matchList(s string, l []string) bool {
	if len(l) == 0 {
		return true
	}

	for _, m := range l {
		if strings.HasPrefix(s, m) {
			return true
		}
	}
	return false
}

func (c *connInfo) send(ack *queueproto.AckFromHub) error {
	if !c.isAlive() {
		return fmt.Errorf("Connection closed")
	}
	c.sender <- ack
	return nil
}

func (c *connInfo) senderLoop(connID connIDType, logger logiface.Logger) {
	logger.Debug("Send loop started", "conn", connID, "client", c.client)

	for ack := range c.sender {
		err := c.server.Send(ack)
		if err != nil {
			logger.PrintErr("Error sending", "err", err, "conn", connID, "client", c.client)
			c.close()
			break
		}
	}

	logger.Debug("Send loop finished", "conn", connID, "client", c.client)
}
