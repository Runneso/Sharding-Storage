package protocol

const (
	StatusOK    = "OK"
	StatusError = "ERROR"

	ErrorTimeout              = "TIMEOUT"
	ErrorBadRequest           = "BAD_REQUEST"
	ErrorInternalServer       = "INTERNAL_SERVER"
	ErrorInvalidClusterConfig = "INVALID_CLUSTER_CONFIG"
)

type ApplicationError interface {
	error
	ErrorCode() string
}

type ClientError struct {
	Code    string
	Message string
}

func (clientError ClientError) Error() string {
	return clientError.Message
}

func (clientError ClientError) ErrorCode() string {
	return clientError.Code
}

func NewBadRequestError(message string) ClientError {
	return ClientError{
		Code:    ErrorBadRequest,
		Message: message,
	}
}

func NewUnknowError(message string) ClientError {
	return ClientError{
		Code:    ErrorInternalServer,
		Message: message,
	}
}

func NewInvalidClusterConfigError(message string) ClientError {
	return ClientError{
		Code:    ErrorInvalidClusterConfig,
		Message: message,
	}
}

func NewTimeoutError(message string) ClientError {
	return ClientError{
		Code:    ErrorTimeout,
		Message: message,
	}
}
