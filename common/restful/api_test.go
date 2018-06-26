package restful

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	TestEndpoint = "/test"
)

func ensureServerIsRunning(r interface{}) *httptest.Server {
	return httptest.NewServer(NewAPI().RegisterResource(r, TestEndpoint).Router[TestEndpoint])
}

func doRequest(method string, s *httptest.Server, data map[string]interface{}) (*http.Response, error) {
	jsonV, _ := json.Marshal(data)
	client := &http.Client{}
	if request, err := http.NewRequest(method, s.URL+TestEndpoint, bytes.NewBuffer(jsonV)); err != nil {
		return nil, err
	} else {
		return client.Do(request)
	}
}

// GET
type mockGet struct {
	Inf string
	Err error
}

func (g *mockGet) Get(r *Request) (interface{}, error) {
	if id, exist := r.Form["id"]; exist {
		g.Inf += strings.Join(id, "|")
	}
	return g.Inf, g.Err
}

func newMockGet(info string, err string) *mockGet {
	if err == "" {
		return &mockGet{info, nil}
	} else {
		return &mockGet{"", fmt.Errorf(err)}
	}
}

func TestGetWithError(t *testing.T) {
	server := ensureServerIsRunning(newMockGet("", "nice GET error"))
	defer server.Close()

	resp, err := http.Get(server.URL + TestEndpoint)
	defer resp.Body.Close()
	assert.Equal(t, nil, err)
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	http.DefaultServeMux = new(http.ServeMux)
}

func TestGetWithoutError(t *testing.T) {
	server := ensureServerIsRunning(newMockGet("some GET info", ""))
	defer server.Close()

	resp, err := http.Get(server.URL + TestEndpoint)
	assert.Equal(t, nil, err)
	actual, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, nil, err)
	jd, err := json.MarshalIndent("some GET info", "", "	")
	assert.Equal(t, nil, err)
	assert.Equal(t, jd, actual)
	http.DefaultServeMux = new(http.ServeMux)
}

func TestGetWith1Para(t *testing.T) {
	server := ensureServerIsRunning(newMockGet("some GET info", ""))
	defer server.Close()

	resp, err := http.Get(server.URL + TestEndpoint + "?id=12345")
	assert.Equal(t, nil, err)
	actual, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, nil, err)
	jd, err := json.MarshalIndent("some GET info12345", "", "	")
	assert.Equal(t, nil, err)
	assert.Equal(t, jd, actual)
	http.DefaultServeMux = new(http.ServeMux)
}

func TestGetWith2Para(t *testing.T) {
	server := ensureServerIsRunning(newMockGet("some GET info", ""))
	defer server.Close()

	resp, err := http.Get(server.URL + TestEndpoint + "?id=12345&id=98765")
	assert.Equal(t, nil, err)
	actual, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, nil, err)
	jd, err := json.MarshalIndent("some GET info12345|98765", "", "	")
	assert.Equal(t, nil, err)
	assert.Equal(t, jd, actual)
	http.DefaultServeMux = new(http.ServeMux)
}

func TestGetWithNotAllowed(t *testing.T) {
	server := ensureServerIsRunning(newMockGet("some GET info", ""))
	defer server.Close()

	resp, err := http.Post(server.URL+TestEndpoint, "application/json", nil)
	defer resp.Body.Close()
	assert.Equal(t, nil, err)
	assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
	http.DefaultServeMux = new(http.ServeMux)

	resp, err = doRequest("PUT", server, nil)
	defer resp.Body.Close()
	assert.Equal(t, nil, err)
	assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
	http.DefaultServeMux = new(http.ServeMux)

	resp, err = doRequest("DELETE", server, nil)
	defer resp.Body.Close()
	assert.Equal(t, nil, err)
	assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
	http.DefaultServeMux = new(http.ServeMux)
}

// POST
type mockPost struct {
	Inf string
	Err error
}

func (g *mockPost) Post(r *Request) (interface{}, error) {
	if r.Body != nil {
		if body, ok := r.Body.(map[string]interface{}); !ok {
			g.Inf += "cast error"
		} else {
			g.Inf += fmt.Sprintf("%.0f", body["id"])
		}
	}
	return g.Inf, g.Err
}

func newMockPost(info string, err string) *mockPost {
	if err == "" {
		return &mockPost{info, nil}
	} else {
		return &mockPost{"", fmt.Errorf(err)}
	}
}

func TestPostWithError(t *testing.T) {
	server := ensureServerIsRunning(newMockPost("", "nice POST error"))
	defer server.Close()

	resp, err := http.Post(server.URL+TestEndpoint, "application/json", nil)
	defer resp.Body.Close()
	assert.Equal(t, nil, err)
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	http.DefaultServeMux = new(http.ServeMux)
}

func TestPostWithoutError(t *testing.T) {
	server := ensureServerIsRunning(newMockPost("some POST info", ""))
	defer server.Close()

	data := map[string]interface{}{
		"id": 1,
	}
	jsonV, _ := json.Marshal(data)
	resp, err := http.Post(server.URL+TestEndpoint, "application/json",
		bytes.NewBuffer(jsonV))
	defer resp.Body.Close()
	assert.Equal(t, nil, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	actual, err := ioutil.ReadAll(resp.Body)
	assert.Equal(t, nil, err)
	assert.Equal(t, "\"some POST info1\"", string(actual))
	http.DefaultServeMux = new(http.ServeMux)
}

func TestPostWithNotAllowed(t *testing.T) {
	server := ensureServerIsRunning(newMockPost("some POST info", ""))
	defer server.Close()

	data := map[string]interface{}{
		"id": 1,
	}
	resp, err := http.Get(server.URL + TestEndpoint)
	defer resp.Body.Close()
	assert.Equal(t, nil, err)
	assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
	http.DefaultServeMux = new(http.ServeMux)

	resp, err = doRequest("PUT", server, data)
	defer resp.Body.Close()
	assert.Equal(t, nil, err)
	assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
	http.DefaultServeMux = new(http.ServeMux)

	resp, err = doRequest("DELETE", server, data)
	defer resp.Body.Close()
	assert.Equal(t, nil, err)
	assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
	http.DefaultServeMux = new(http.ServeMux)
}

// PUT
type mockPut struct {
	Inf string
	Err error
}

func (g *mockPut) Put(r *Request) (interface{}, error) {
	if r.Body != nil {
		if body, ok := r.Body.(map[string]interface{}); !ok {
			g.Inf += "cast error"
		} else {
			g.Inf += fmt.Sprintf("%.0f", body["id"])
		}
	}
	return g.Inf, g.Err
}

func newMockPut(info string, err string) *mockPut {
	if err == "" {
		return &mockPut{info, nil}
	} else {
		return &mockPut{"", fmt.Errorf(err)}
	}
}

func TestPutWithError(t *testing.T) {
	server := ensureServerIsRunning(newMockPut("", "nice PUT error"))
	defer server.Close()

	resp, err := doRequest("PUT", server, nil)
	defer resp.Body.Close()
	assert.Equal(t, nil, err)
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	http.DefaultServeMux = new(http.ServeMux)
}

func TestPutWithoutError(t *testing.T) {
	server := ensureServerIsRunning(newMockPut("some PUT info", ""))
	defer server.Close()

	data := map[string]interface{}{
		"id": 1,
	}
	resp, err := doRequest("PUT", server, data)
	defer resp.Body.Close()
	assert.Equal(t, nil, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	actual, err := ioutil.ReadAll(resp.Body)
	assert.Equal(t, nil, err)
	assert.Equal(t, "\"some PUT info1\"", string(actual))
	http.DefaultServeMux = new(http.ServeMux)
}

func TestPutWithNotAllowed(t *testing.T) {
	server := ensureServerIsRunning(newMockPut("some PUT info", ""))
	defer server.Close()

	data := map[string]interface{}{
		"id": 1,
	}
	resp, err := http.Get(server.URL + TestEndpoint)
	defer resp.Body.Close()
	assert.Equal(t, nil, err)
	assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
	http.DefaultServeMux = new(http.ServeMux)

	resp, err = http.Post(server.URL+TestEndpoint, "application/json", nil)
	defer resp.Body.Close()
	assert.Equal(t, nil, err)
	assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
	http.DefaultServeMux = new(http.ServeMux)

	resp, err = doRequest("DELETE", server, data)
	defer resp.Body.Close()
	assert.Equal(t, nil, err)
	assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
	http.DefaultServeMux = new(http.ServeMux)
}

// DELETE
type mockDelete struct {
	Inf string
	Err error
}

func (g *mockDelete) Delete(r *Request) (interface{}, error) {
	if r.Body != nil {
		if body, ok := r.Body.(map[string]interface{}); !ok {
			g.Inf += "cast error"
		} else {
			g.Inf += fmt.Sprintf("%.0f", body["id"])
		}
	}
	return g.Inf, g.Err
}

func newMockDelete(info string, err string) *mockDelete {
	if err == "" {
		return &mockDelete{info, nil}
	} else {
		return &mockDelete{"", fmt.Errorf(err)}
	}
}

func TestDeleteWithError(t *testing.T) {
	server := ensureServerIsRunning(newMockDelete("", "nice DEL error"))
	defer server.Close()

	resp, err := doRequest("DELETE", server, nil)
	defer resp.Body.Close()
	assert.Equal(t, nil, err)
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	http.DefaultServeMux = new(http.ServeMux)
}

func TestDeleteWithoutError(t *testing.T) {
	server := ensureServerIsRunning(newMockDelete("some DEL info", ""))
	defer server.Close()

	data := map[string]interface{}{
		"id": 1,
	}
	resp, err := doRequest("DELETE", server, data)
	defer resp.Body.Close()
	assert.Equal(t, nil, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	actual, err := ioutil.ReadAll(resp.Body)
	assert.Equal(t, nil, err)
	assert.Equal(t, "\"some DEL info1\"", string(actual))
	http.DefaultServeMux = new(http.ServeMux)
}

func TestDeleteWithNotAllowed(t *testing.T) {
	server := ensureServerIsRunning(newMockDelete("some DEL info", ""))
	defer server.Close()

	data := map[string]interface{}{
		"id": 1,
	}
	resp, err := http.Get(server.URL + TestEndpoint)
	defer resp.Body.Close()
	assert.Equal(t, nil, err)
	assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
	http.DefaultServeMux = new(http.ServeMux)

	resp, err = http.Post(server.URL+TestEndpoint, "application/json", nil)
	defer resp.Body.Close()
	assert.Equal(t, nil, err)
	assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
	http.DefaultServeMux = new(http.ServeMux)

	resp, err = doRequest("PUT", server, data)
	defer resp.Body.Close()
	assert.Equal(t, nil, err)
	assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
	http.DefaultServeMux = new(http.ServeMux)
}
