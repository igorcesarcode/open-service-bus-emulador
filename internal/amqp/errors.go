package amqp

import (
	"errors"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/encoding"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/frames"
)

// Error conditions defined by AMQP 1.0, part 2.8.
const (
	CondInternalError         encoding.Symbol = "amqp:internal-error"
	CondNotFound              encoding.Symbol = "amqp:not-found"
	CondUnauthorizedAccess    encoding.Symbol = "amqp:unauthorized-access"
	CondDecodeError           encoding.Symbol = "amqp:decode-error"
	CondResourceLimitExceeded encoding.Symbol = "amqp:resource-limit-exceeded"
	CondNotAllowed            encoding.Symbol = "amqp:not-allowed"
	CondInvalidField          encoding.Symbol = "amqp:invalid-field"
	CondNotImplemented        encoding.Symbol = "amqp:not-implemented"
	CondPreconditionFailed    encoding.Symbol = "amqp:precondition-failed"
	CondIllegalState          encoding.Symbol = "amqp:illegal-state"

	CondConnectionForced = encoding.Symbol("amqp:connection:forced")
	CondFramingError     = encoding.Symbol("amqp:connection:framing-error")

	CondWindowViolation  = encoding.Symbol("amqp:session:window-violation")
	CondErrantLink       = encoding.Symbol("amqp:session:errant-link")
	CondHandleInUse      = encoding.Symbol("amqp:session:handle-in-use")
	CondUnattachedHandle = encoding.Symbol("amqp:session:unattached-handle")

	CondDetachForced          = encoding.Symbol("amqp:link:detach-forced")
	CondTransferLimitExceeded = encoding.Symbol("amqp:link:transfer-limit-exceeded")
	CondMessageSizeExceeded   = encoding.Symbol("amqp:link:message-size-exceeded")
)

// Errors returned by link operations.
var (
	// ErrLinkDetached reports that the link was detached before the
	// operation completed.
	ErrLinkDetached = errors.New("amqp: link detached")
	// ErrConnClosed reports that the connection closed before the operation
	// completed.
	ErrConnClosed = errors.New("amqp: connection closed")
	// ErrServerClosed is returned by Server.Serve after Server.Close.
	ErrServerClosed = errors.New("amqp: server closed")
	// ErrAttachPending, returned by Handler.AttachSender, leaves the
	// attach unanswered until SenderLink.CompleteAttach is called.
	ErrAttachPending = errors.New("amqp: attach pending")
)

// NewError returns an AMQP error with the given condition and description.
func NewError(cond encoding.Symbol, description string) *frames.Error {
	return &frames.Error{Condition: cond, Description: description}
}

// asAMQPError converts err into an AMQP error, defaulting to
// amqp:internal-error.
func asAMQPError(err error) *frames.Error {
	var e *frames.Error
	if errors.As(err, &e) {
		return e
	}
	return NewError(CondInternalError, err.Error())
}
