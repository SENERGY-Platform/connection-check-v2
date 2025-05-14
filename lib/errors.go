package lib

type ResponseError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (r *ResponseError) Error() string {
	return r.Message
}

func NewResponseError(c int, m string) *ResponseError {
	return &ResponseError{
		Code:    c,
		Message: m,
	}
}

type MultiStatusError struct {
	err    error
	Errors []StatesRefreshResponseErrItem
}

func (e *MultiStatusError) Error() string {
	return e.err.Error()
}

func (e *MultiStatusError) Unwrap() error {
	return e.err
}

func NewMultiStatusError(err error, errors []StatesRefreshResponseErrItem) *MultiStatusError {
	return &MultiStatusError{
		err:    err,
		Errors: errors,
	}
}
