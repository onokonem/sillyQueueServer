package main

import (
	"flag"
	"net"
	"time"

	"github.com/onokonem/sillyQueueServer/db"
	"github.com/onokonem/sillyQueueServer/queue"
	"github.com/onokonem/sillyQueueServer/queueproto"
	"github.com/powerman/structlog"

	"google.golang.org/grpc"
)

func main() {
	logger := structlog.New()

	flag.Parse()

	dbConn, err := db.Open(*Flags.DB, *Flags.DBURI)
	if err != nil {
		logger.Panic(err)
	}
	defer dbConn.MustClose()

	tcpServer, err := net.Listen("tcp", *Flags.Listen)
	if err != nil {
		logger.Fatalf("failed to listen on %q: %v", *Flags.Listen, err)
	}

	if !*Flags.Debug {
		logger.SetLogLevel(structlog.INF)
	}

	grpcServer := grpc.NewServer()
	queueServer := queue.NewServer(
		dbConn,
		*Flags.Sendbuf,
		*Flags.ConnTTL,
		*Flags.QueueTTL,
		*Flags.AcceptTTL,
		*Flags.ProcessTTL,
		logger.New(),
	)

	count, err := dbConn.Foreach(queueServer.RestoreTask)
	if err != nil {
		logger.Panic(err)
	}
	logger.Info("Queue recovered", "count", count)

	go dbConn.SaverLoop(*Flags.Interval, *Flags.Bunch, logger.New())
	go queueServer.DispatcherLoop()

	queueproto.RegisterQueueServer(grpcServer, queueServer)

	err = grpcServer.Serve(tcpServer)
	if err != nil {
		logger.Fatal("Unexpected error", "error", err)
	}
}

// Flags is a struct for command line flags ready to be utilized by flag.Parse()
var Flags = struct {
	Debug      *bool
	Listen     *string
	DB         *string
	DBURI      *string
	Interval   *time.Duration
	Bunch      *int
	ConnTTL    *time.Duration
	Sendbuf    *int
	AcceptTTL  *time.Duration
	ProcessTTL *time.Duration
	QueueTTL   *time.Duration
}{
	flag.Bool("debug", false, "Verbose logging"),
	flag.String("listen", "127.0.0.1:8080", "Address to listen"),
	flag.String("db", "bolt", "DB driver to be used"),
	flag.String("dburi", "./sillyQueueServer.boltdb", "DB URI"),
	flag.Duration("interval", time.Second, "How frequently the DB write cache will be flushed"),
	flag.Int("bunch", 1024, "How big the DB cache can grow to be flushed"),
	flag.Duration("connttl", time.Minute, "Connection silence timeout"),
	flag.Int("sendbuf", 10, "Sender buffer size"),
	flag.Duration("acceptttl", time.Second, "Time gap for the client to return task accepted acknowledgement"),
	flag.Duration("processttl", time.Second*10, "Time gap for the client to return task completed acknowledgement"),
	flag.Duration("queuettl", time.Minute, "Maximum time to keed the task in queue"),
}
