package utils

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
)

// DebugMockServerCall can be set to true to print all requests received
// by the MockServer
var DebugMockServerCall = false

// MockServerCall defines what we expect from a call to the server.
//
//		UrlMatchFunc: a function that will check the URL for a match. The default
//	               function will return true.
//	 Method:       The HTTP method expected: get, post, put, patch, delete, etc.
//	               Defaults: `get`
//	 Responsecode: The HTTP response code to this function. Default: `200`
//	 ResponseBody: The response body. This could be any interface{} that will
//	               be marsheled to a json object. Default: interface{}{}
type MockServerCall struct {
	UrlMatchFunc func(string) bool
	Method       string
	ResponseCode int
	ResponseBody interface{}
}

// NewMockServer creates a httptest.Server that will consume calls from the
// passed serverCallChan channel. For each call it consumes one MockServercall
// and use it to format the response
//
// Usage:
//
//	callChannel = make(chan MockServerCall, 2)
//	// A first GET call will return an error
//	callChannel <- MockServerCall{
//		UrlMatchFunc: func(url string) { return true }, //Match any url
//		Method: "get",
//		ResponseCode: 500,
//	}
//	// A second POST call will return OK with a body
//	callChannel <- MockServerCall{
//		UrlMatchFunc: func(url string) { return true }, //Match any url
//		Method: "post",
//		ResponseCode: 200,
//		ResponseBody: map[string]string{"status": "200"}
//	}
//	mockServer := NewMockServer(callChannel)
//	// Use the server
func NewMockServer(serverCallChan chan MockServerCall) *httptest.Server {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serverCall := <-serverCallChan
		serverCall = applyDefaults(serverCall)
		code := serverCall.ResponseCode

		if DebugMockServerCall {
			fmt.Printf("MockServer received: [%s] %s", r.Method, r.URL)
		}

		if !serverCall.UrlMatchFunc(r.RequestURI) || strings.ToLower(r.Method) != serverCall.Method {
			panic(fmt.Sprintf("Unexpected server call for [%s] %s", r.Method, r.RequestURI))
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.WriteHeader(code)
		if code >= 200 && code < 300 {
			body, err := json.Marshal(serverCall.ResponseBody)
			if err != nil {
				panic(fmt.Errorf("error parsing response body: %s", err))
			}

			fmt.Fprintln(w, string(body))
		} else {
			fmt.Fprintf(w, `{"error": {"code": %d, "message": "Error occurred"}}\n`, code)
		}
	}))

	return mockServer
}

func applyDefaults(mockCall MockServerCall) MockServerCall {
	if mockCall.UrlMatchFunc == nil {
		mockCall.UrlMatchFunc = func(string) bool {
			return true
		}
	}
	if mockCall.Method == "" {
		mockCall.Method = "get"
	}
	if mockCall.ResponseCode == 0 {
		mockCall.ResponseCode = 200
	}
	if mockCall.ResponseBody == nil {
		mockCall.ResponseBody = new(struct{})
	}
	return mockCall
}
