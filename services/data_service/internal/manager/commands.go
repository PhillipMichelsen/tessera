package manager

import (
	"github.com/google/uuid"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/domain"
)

// Session Commands

type createSessionCommand struct {
	resp chan createSessionResult
}

type createSessionResult struct {
	sid uuid.UUID
}

type leaseSessionReceiverCommand struct {
	sid  uuid.UUID
	resp chan leaseSessionReceiverResult
}

type leaseSessionReceiverResult struct {
	receiveFunc func() (domain.Message, error)
	err         error
}

type leaseSessionSenderCommand struct {
	sid  uuid.UUID
	resp chan leaseSessionSenderResult
}

type leaseSessionSenderResult struct {
	sendFunc func(domain.Message) error
	err      error
}

type releaseSessionReceiverCommand struct {
	sid  uuid.UUID
	resp chan releaseSessionReceiverResult
}

type releaseSessionReceiverResult struct {
	err error
}

type releaseSessionSenderCommand struct {
	sid  uuid.UUID
	resp chan releaseSessionSenderResult
}

type releaseSessionSenderResult struct {
	err error
}

type configureSessionCommand struct {
	sid  uuid.UUID
	cfg  any
	resp chan configureSessionResult
}

type configureSessionResult struct {
	err error
}

type closeSessionCommand struct {
	sid  uuid.UUID
	resp chan closeSessionResult
}

type closeSessionResult struct {
	err error
}
