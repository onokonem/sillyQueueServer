package queue

import (
	"fmt"
	"sync"
	"time"

	"github.com/onokonem/sillyQueueServer/queueproto"
)

type connIDType string
type clientIDType string

type connectsStorage struct {
	connects map[connIDType]*connInfo
	clients  map[clientIDType]connIDType
	sync.RWMutex
}

func newConnectsStorage() *connectsStorage {
	return &connectsStorage{
		connects: make(map[connIDType]*connInfo),
		clients:  make(map[clientIDType]connIDType),
	}
}

func (cc *connectsStorage) getConn(client clientIDType) (connIDType, *connInfo) {
	cc.RLock()
	defer cc.RUnlock()

	connID := cc.clients[client]
	if connID == "" {
		return "", nil
	}

	conn := cc.connects[connID]
	if conn == nil {
		panic(fmt.Errorf("Connection not found for client %q, connID %q", client, connID))
	}

	return connID, conn
}

func (cc *connectsStorage) get(connID connIDType) *connInfo {
	cc.RLock()
	defer cc.RUnlock()

	return cc.connects[connID]
}

func (cc *connectsStorage) add(
	connID connIDType,
	server queueproto.Queue_AckServer,
	senderBufSize int,
	ttl time.Duration,
) *connInfo {
	cc.Lock()
	defer cc.Unlock()

	conn := newConnInfo(server, senderBufSize, ttl)
	cc.connects[connID] = conn

	return conn
}

func (cc *connectsStorage) attach(connID connIDType, clientID clientIDType) {
	cc.get(connID).attach(clientID)

	cc.Lock()
	defer cc.Unlock()

	cc.clients[clientID] = connID
}

func (cc *connectsStorage) del(connID connIDType) {
	cc.Lock()
	defer cc.Unlock()

	conn := cc.connects[connID]

	if conn == nil {
		return
	}

	delete(cc.connects, connID)

	if conn.client != "" {
		delete(cc.clients, conn.client)
	}
}

type foreachFunc func(connID connIDType, conn *connInfo) (found bool, stop bool)

func (cc *connectsStorage) foreach(f foreachFunc) int {
	cc.RLock()
	defer cc.RUnlock()

	totalFound := 0
	for connID, conn := range cc.connects {
		found, stop := f(connID, conn)
		if found {
			totalFound++
		}
		if stop {
			return totalFound
		}
	}

	return totalFound
}
