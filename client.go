package godruid

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

const (
	DefaultEndPoint = "/druid/v2"
	SQLEndPoint     = "/druid/v2/sql"
)

type QueryStyle int

const (
	NativeQueryStyle QueryStyle = 0
	SQLQueryStyle    QueryStyle = 1
)

type Client struct {
	Url      string
	EndPoint string

	Debug        bool
	LastRequest  string
	LastResponse string
	HttpClient   *http.Client
}

func (c *Client) Query(query Query) (err error) {
	return c.NativeQuery(query)
}

func (c *Client) NativeQuery(query Query) (err error) {
	return c.PrepareAndExecuteQuery(query, NativeQueryStyle)
}

func (c *Client) SQLQuery(query Query) (err error) {
	return c.PrepareAndExecuteQuery(query, SQLQueryStyle)
}

func (c *Client) PrepareAndExecuteQuery(query Query, style QueryStyle) (err error) {
	query.setup()
	var reqJson []byte
	if c.Debug {
		reqJson, err = json.MarshalIndent(query, "", "  ")
	} else {
		reqJson, err = json.Marshal(query)
	}
	if err != nil {
		return
	}

	result, err := c.QueryRaw(reqJson, style)
	if err != nil {
		return
	}

	return query.onResponse(result)
}

func (c *Client) QueryRaw(req []byte, style QueryStyle) (result []byte, err error) {

	endPoint := DefaultEndPoint
	if style == SQLQueryStyle {
		endPoint = SQLEndPoint
	}

	if c.Debug {
		endPoint += "?pretty"
		c.LastRequest = string(req)
	}
	if err != nil {
		return
	}

	request, err := http.NewRequest("POST", c.Url+endPoint, bytes.NewBuffer(req))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")

	resp, err := c.HttpClient.Do(request)
	defer func() {
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
	}()

	if err != nil {
		return nil, err
	}

	result, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}
	if c.Debug {
		c.LastResponse = string(result)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: %s", resp.Status, string(result))
	}

	return
}
