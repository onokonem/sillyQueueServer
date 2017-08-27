package tasks

import "sync"

// Storage is a simple RWLock protected map
type Storage struct {
	storage map[string]*Task

	sync.RWMutex
}

// Get returns task from the storage
func (s *Storage) Get(id string) *Task {
	s.RLock()
	defer s.RUnlock()

	return s.storage[id]
}

// Add adds task to the sorage
func (s *Storage) Add(task *Task) {
	s.Lock()
	defer s.Unlock()

	s.storage[task.id] = task
}

// Add adds task to the sorage
func (s *Storage) Restore(task *Task) {
	s.Lock()
	defer s.Unlock()

	s.storage[task.id] = task

	if sequence < task.Sequence {
		sequence = task.Sequence
	}
}

// Del deletes task to the sorage
func (s *Storage) Del(id string) {
	s.Lock()
	defer s.Unlock()

	delete(s.storage, id)
}

// NewStorage creates a new task storage struct with the internal map initialised
func NewStorage() *Storage {
	return &Storage{
		storage: make(map[string]*Task),
	}
}

// ForeachFunc is a type of func to be passed to Foreach method
type ForeachFunc func(id string, task *Task) (found bool, stop bool)

// Foreach executes the function provided for each task in the storage
func (s *Storage) Foreach(f ForeachFunc) int {
	s.RLock()
	defer s.RUnlock()

	totalFound := 0
	for id, task := range s.storage {
		found, stop := f(id, task)
		if found {
			totalFound++
		}
		if stop {
			return totalFound
		}
	}

	return totalFound
}
