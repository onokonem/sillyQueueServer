package logiface_test

import (
	"testing"

	"github.com/onokonem/sillyQueueServer/logiface"
	"github.com/powerman/structlog"
)

func TestLogiface(t *testing.T) {
	var logger logiface.Logger = structlog.New()
	_ = logger
}
