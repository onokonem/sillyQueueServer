package tasks_test

import (
	"testing"
	"time"

	"github.com/onokonem/sillyQueueServer/queueproto"
	"github.com/onokonem/sillyQueueServer/tasks"
	"github.com/onokonem/sillyQueueServer/timeuuid"
)

func TestTaskOps(t *testing.T) {
	taskID := timeuuid.TimeUUID().String()

	task := tasks.NewTask(&queueproto.QueueTask{Id: taskID})

	etalonMarshal := `{"Queue":"","Subject":"","Originator":"","Timeout":0,"Persistence":0,"Delivery":0,"Ack":{"Accepted":false,"Done":false,"Timeout":false,"Aborted":false,"Dismissed":false},"Payload":null,"Status":0,"CTime":"0001-01-01T00:00:00Z","Sequence":1}`
	taskMarshal := string(task.Marshal())

	if taskMarshal != etalonMarshal {
		t.Errorf("Invalid task marshal: %q", taskMarshal)
	}

	etalonID := "testID 1"
	task.SetID(etalonID)
	newID := task.ID()
	if newID != etalonID {
		t.Errorf("Invalid task ID, got %q, expected %q", newID, etalonID)
	}

	etalonStatus := tasks.StatusAccepted
	task.ChangeStatus(etalonStatus)
	if task.Status != etalonStatus {
		t.Errorf("Invalid task Status, got %v, expected %v", task.Status, etalonStatus)
	}

	etalonTimer := time.AfterFunc(time.Hour, func() {})
	task.ChangeExpire(etalonTimer)

	etalonStatus = tasks.StatusDone
	task.ChangeStatusAndExpire(etalonStatus, nil)
	if task.Status != etalonStatus {
		t.Errorf("Invalid task Status, got %v, expected %v", task.Status, etalonStatus)
	}

	timerStop := etalonTimer.Stop()
	if timerStop {
		t.Errorf("Expire timer was running but has to be stopped")
	}
}
