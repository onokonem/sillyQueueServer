package tasks_test

import (
	"testing"

	"github.com/onokonem/sillyQueueServer/tasks"
	"github.com/onokonem/sillyQueueServer/timeuuid"
)

func TestTasksOps(t *testing.T) {
	storage := tasks.NewStorage()

	etalonStorage := map[string]*tasks.Task{
		timeuuid.TimeUUID().String(): &tasks.Task{},
		timeuuid.TimeUUID().String(): &tasks.Task{},
		timeuuid.TimeUUID().String(): &tasks.Task{},
		timeuuid.TimeUUID().String(): &tasks.Task{},
		timeuuid.TimeUUID().String(): &tasks.Task{},
		timeuuid.TimeUUID().String(): &tasks.Task{},
		timeuuid.TimeUUID().String(): &tasks.Task{},
		timeuuid.TimeUUID().String(): &tasks.Task{},
		timeuuid.TimeUUID().String(): &tasks.Task{},
		timeuuid.TimeUUID().String(): &tasks.Task{},
	}

	for id, task := range etalonStorage {
		task.SetID(id)
		storage.Add(task)
	}

	for id, task := range etalonStorage {
		if storage.Get(id) != task {
			t.Fatalf("Invalid task value")
		}
	}

	count := storage.Foreach(
		func(id string, task *tasks.Task) (bool, bool) {
			foundTask := etalonStorage[id]
			if foundTask != nil && task.Status != tasks.StatusDone {
				foundTask.ChangeStatus(tasks.StatusDone)
				return true, false
			}
			return false, false
		},
	)

	if count != len(etalonStorage) {
		t.Fatalf("Invalid count returned by Foreach(): %d instead of %d", count, len(etalonStorage))
	}

	for id := range etalonStorage {
		storage.Del(id)
	}

	count = storage.Foreach(
		func(id string, task *tasks.Task) (bool, bool) {
			return true, false
		},
	)

	if count != 0 {
		t.Fatalf("Invalid count returned by Foreach(): %d instead of %d", count, 0)
	}
}
