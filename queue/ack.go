package queue

import "github.com/onokonem/sillyQueueServer/queueproto"

func makeAckAccepted(id string) *queueproto.AckFromHub {
	return &queueproto.AckFromHub{
		Msg: &queueproto.AckFromHub_Accepted{
			Accepted: &queueproto.Accepted{
				Id: id,
			},
		},
	}
}

func makeAckDone(id string) *queueproto.AckFromHub {
	return &queueproto.AckFromHub{
		Msg: &queueproto.AckFromHub_Done{
			Done: &queueproto.Done{
				Id: id,
			},
		},
	}
}

func makeAckDismissed(id string) *queueproto.AckFromHub {
	return &queueproto.AckFromHub{
		Msg: &queueproto.AckFromHub_Dismissed{
			Dismissed: &queueproto.Dismissed{
				Id: id,
			},
		},
	}
}

func makeAckAborted(id string) *queueproto.AckFromHub {
	return &queueproto.AckFromHub{
		Msg: &queueproto.AckFromHub_Aborted{
			Aborted: &queueproto.Aborted{
				Id: id,
			},
		},
	}
}

func makeAckTimeouted(id string) *queueproto.AckFromHub {
	return &queueproto.AckFromHub{
		Msg: &queueproto.AckFromHub_Timeouted{
			Timeouted: &queueproto.Timeouted{
				Id: id,
			},
		},
	}
}

func makeAckPong() *queueproto.AckFromHub {
	return &queueproto.AckFromHub{
		Msg: &queueproto.AckFromHub_Pong{
			Pong: &queueproto.Pong{
				Alive: true,
			},
		},
	}
}

func makeAccept(queue string, id string, subject string, payload []byte) *queueproto.AckFromHub {
	return &queueproto.AckFromHub{
		Msg: &queueproto.AckFromHub_Accept{
			Accept: &queueproto.Accept{
				Queue:   queue,
				Id:      id,
				Subject: subject,
				Payload: payload,
			},
		},
	}
}

func makeAckAttached(originator clientIDType) *queueproto.AckFromHub {
	return &queueproto.AckFromHub{
		Msg: &queueproto.AckFromHub_Attached{
			Attached: &queueproto.Attached{
				Originator: string(originator),
			},
		},
	}
}

func makeAckSubscribed(originator clientIDType) *queueproto.AckFromHub {
	return &queueproto.AckFromHub{
		Msg: &queueproto.AckFromHub_Subscribed{
			Subscribed: &queueproto.Subscribed{
				Originator: string(originator),
			},
		},
	}
}

func makeAckQueued(id string) *queueproto.AckFromHub {
	return &queueproto.AckFromHub{
		Msg: &queueproto.AckFromHub_Queued{
			Queued: &queueproto.Queued{
				Id: id,
			},
		},
	}
}

func makeAckRejected(id string, reason string) *queueproto.AckFromHub {
	return &queueproto.AckFromHub{
		Msg: &queueproto.AckFromHub_Rejected{
			Rejected: &queueproto.Rejected{
				Id:     id,
				Reason: reason,
			},
		},
	}
}
