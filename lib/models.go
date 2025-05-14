package lib

const (
	LMResultParam  = "lm_result"
	SubResultParam = "sub_result"
)

type StatesRefreshRequestItem struct {
	ID        string `json:"id"`
	LMResult  int    `json:"lm_result"`
	SubResult int    `json:"sub_result"`
}

type StatesRefreshResponseErrItem struct {
	ID    string `json:"id"`
	Error string `json:"error"`
}
