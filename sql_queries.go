package godruid

type SQLQuery struct {
	Query        string `json:"query"`
	ResultFormat string `json:"resultFormat"`
	response     []byte
}

func (q *SQLQuery) setup() {
	q.ResultFormat = "object" // We only support object for now
}

func (q *SQLQuery) onResponse(content []byte) error {
	q.response = content
	return nil
}

func (q *SQLQuery) GetRawJSON() []byte {
	return q.response
}
