package manager

import (
	"github.com/google/uuid"
	"gitlab.michelsen.id/phillmichelsen/tessera/services/data_service/internal/domain"
)

// Commands posted into the manager loop. One struct per action.
type createSessionCommand struct {
	resp chan createSessionResult
}

type createSessionResult struct {
	sid uuid.UUID
}

type leaseReceiverCommand struct {
	sid  uuid.UUID
	resp chan leaseReceiverResult
}

type leaseReceiverResult struct {
	receiveFunc func() (domain.Message, error)
	releaseFunc func()
	err         error
}

type leaseSenderCommand struct {
	sid  uuid.UUID
	resp chan leaseSenderResult
}

type leaseSenderResult struct {
	sendFunc    func(domain.Message) error
	releaseFunc func()
	err         error
}

type configureSessionCommand struct {
	sid    uuid.UUID
	config SessionConfig
	resp   chan configureSessionResult
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

type spawnWorkerCommand struct {
	workerType string
	resp chan spawnWorkerResult
}

type spawnWorkerResult struct {
	wid uuid.UUID
	err error
}

type configureWorkerCommand struct {
	wid uuid.UUID
	configuration any
	resp chan instructWorkerResponse
}

type configureWorkerResponse struct {
	err error
}

type terminateWorkerCommand struct {
	wid uuid.UUID
	resp chan terminateWorkerResult
}

type terminateWorkerResult struct {
	err error
}
