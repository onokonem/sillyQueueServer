package db_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/onokonem/sillyQueueServer/db"
	"github.com/onokonem/sillyQueueServer/queueproto"
	"github.com/onokonem/sillyQueueServer/tasks"
	"github.com/onokonem/sillyQueueServer/timeuuid"
	"github.com/powerman/structlog"
)

func TestOpenClose(t *testing.T) {
	fileName := timeuuid.TimeUUID().String()

	dbConn, err := db.Open("bolt", fileName)
	if err != nil {
		t.Fatalf("Close() returned error: %v", err)
	}
	defer mustRemove(fileName)

	if fmt.Sprintf(`DB<"%s">`, fileName) != dbConn.String() {
		t.Fatalf("unexpected DB String(): %q", dbConn.String())
	}

	err = dbConn.Close()
	if err != nil {
		t.Fatalf("Close() returned error: %v", err)
	}

	dbConn, err = db.Open("bolt", fileName)
	if err != nil {
		t.Fatalf("Close() returned error: %v", err)
	}

	err = mustClose(dbConn)
	if err != nil {
		t.Fatalf("MustClose() paniced: %v", err)
	}

}

func TestReadWrite(t *testing.T) {
	fileName := timeuuid.TimeUUID().String()

	dbConn, err := db.Open("bolt", fileName)
	if err != nil {
		t.Fatalf("Close() returned error: %v", err)
	}
	defer mustRemove(fileName)

	go dbConn.SaverLoop(time.Millisecond, 1024, structlog.New())

	taskID := timeuuid.TimeUUID().String()
	task := tasks.NewTask(&queueproto.QueueTask{Id: taskID, Payload: []byte(timeuuid.TimeUUID().String())})

	dbConn.Saver() <- task
	time.Sleep(time.Millisecond * 100)

	newTask, err := dbConn.GetTask(taskID)
	if err != nil {
		t.Fatalf("Error restoring task: %v", err)
	}

	if string(task.Marshal()) != string(newTask.Marshal()) {
		t.Fatalf("Restored task is not the same as saved\n%#+v\n%#+v", newTask, task)
	}

	err = dbConn.SaveTasks(
		tasks.NewTask(
			&queueproto.QueueTask{
				Id:      timeuuid.TimeUUID().String(),
				Payload: []byte(timeuuid.TimeUUID().String()),
			},
		),
	)
	if err != nil {
		t.Fatalf("Save task iteration error: %v", err)
	}

	count, err := dbConn.Foreach(
		func(task *tasks.Task) error {
			return nil
		},
	)
	if err != nil {
		t.Fatalf("Save task iteration error: %v", err)
	}

	if count != 2 {
		t.Fatalf("Saved tasks count mismatch: got %d but 2 expected", count)
	}
}

func mustClose(dbConn db.Connect) (err error) {
	defer func() {
		e := recover()
		if e != nil {
			err = fmt.Errorf("%v", e)
		}
	}()

	dbConn.MustClose()

	return nil
}

func mustRemove(name string) {
	err := os.Remove(name)
	if err != nil {
		panic(err)
	}
}
