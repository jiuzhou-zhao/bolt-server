package boltsc

/*
POST	[db]open|close|update|view
POST 	[tx]create_bucket|bucket|delete_bucket|close
POST 	[bucket]put|get|delete|cursor|close
POST 	[cursor]first|next|close
*/

const (
	StatusOK        = 0
	StatusNotExists = 1
	StatusExists    = 2
	StatusFailed    = 10
)

const (
	UriDB     = "db"
	UriTX     = "tx"
	UriBucket = "bucket"
	UriCursor = "cursor"
)

type ResponseBase struct {
	Status  int    `json:"status"`
	Message string `json:"message,omitempty"`
}

func (resp *ResponseBase) SetStatusWithMessage(status int, msg string) {
	resp.Status = status
	resp.Message = msg
}

func (resp *ResponseBase) SetErrorMessage(msg string) {
	resp.Status = StatusFailed
	resp.Message = msg
}

func (resp *ResponseBase) SetError(err error) {
	resp.Status = -1
	resp.Message = err.Error()
}

type DBMethod int

const (
	DBMethodOpen          DBMethod = 1
	DBMethodClose         DBMethod = 2
	DBMethodUpdate        DBMethod = 3
	DBMethodView          DBMethod = 4
	DBMethodRebuild4Debug DBMethod = 5
)

type DBRequest struct {
	Method DBMethod `json:"method"`
	DbID   string   `json:"db_id,omitempty"`
	DBName string   `json:"db_name,omitempty"`
}

type DBResponse struct {
	ResponseBase `json:",inline"`
	DbID         string `json:"db_id,omitempty"`
	TxID         string `json:"tx_id,omitempty"`
}

type TXMethod int

const (
	TXMethodCreateBucket TXMethod = 1
	TXMethodGetBucket    TXMethod = 2
	TXMethodDeleteBucket TXMethod = 3
	TXMethodClose        TXMethod = 4
)

type TXRequest struct {
	Method          TXMethod `json:"method"`
	TxID            string   `json:"tx_id,omitempty"`
	BucketName      string   `json:"tx_name,omitempty"`
	RollbackMessage string   `json:"rollback_message"`
}

type TXResponse struct {
	ResponseBase `json:",inline"`
	BucketID     string `json:"bucket_id,omitempty"`
}

type BucketMethod int

const (
	BucketMethodPut    BucketMethod = 1
	BucketMethodGet    BucketMethod = 2
	BucketMethodDelete BucketMethod = 3
	BucketMethodCursor BucketMethod = 4
	BucketMethodClose  BucketMethod = 5
)

type BucketRequest struct {
	Method   BucketMethod `json:"method"`
	BucketID string       `json:"bucket_id,omitempty"`
	Key      []byte       `json:"key,omitempty"`
	Value    []byte       `json:"value,omitempty"`
}
type BucketResponse struct {
	ResponseBase `json:",inline"`
	CursorID     string `json:"cursor_id,omitempty"`
	Value        []byte `json:"value,omitempty"`
}

type CursorMethod int

const (
	CursorMethodFirst CursorMethod = 1
	CursorMethodNext  CursorMethod = 2
	CursorMethodClose CursorMethod = 3
)

type CursorRequest struct {
	Method   CursorMethod `json:"method"`
	CursorID string       `json:"cursor_id,omitempty"`
}

type CursorResponse struct {
	ResponseBase `json:",inline"`
	CursorID     string `json:"cursor_id,omitempty"`
	Key          []byte `json:"key"`
	Value        []byte `json:"value"`
}
