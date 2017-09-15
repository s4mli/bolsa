package restful

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/gorilla/mux"
)

const (
	GET    = "GET"
	PUT    = "PUT"
	POST   = "POST"
	DELETE = "DELETE"
)

type RequestParameters struct {
	Header http.Header
	Form   url.Values
	Body   interface{}
}

// HTTP GET
type GetSupported interface {
	Get(params *RequestParameters) (int, interface{}, http.Header, error)
}

// HTTP POST
type PostSupported interface {
	Post(params *RequestParameters) (int, interface{}, http.Header, error)
}

// HTTP PUT
type PutSupported interface {
	Put(params *RequestParameters) (int, interface{}, http.Header, error)
}

// HTTP DELETE
type DeleteSupported interface {
	Delete(params *RequestParameters) (int, interface{}, http.Header, error)
}

type API struct {
	router      *mux.Router
	initialized bool
}

func NewAPI() *API {
	return &API{}
}

func (api *API) parametersFrom(r *http.Request) (*RequestParameters,
	error) {
	if err := r.ParseForm(); err != nil {
		return nil, err
	}
	var resource interface{}
	decoder := json.NewDecoder(r.Body)
	defer r.Body.Close()
	if err := decoder.Decode(&resource); err != nil {
		return &RequestParameters{r.Header, r.Form, nil}, nil
	}
	return &RequestParameters{r.Header, r.Form, resource}, nil
}

func (api *API) requestHandler(resource interface{}) http.HandlerFunc {
	return func(rw http.ResponseWriter, request *http.Request) {
		start := time.Now()
		defer fmt.Println(request.Method, request.RequestURI, time.Since(start))
		rp, err := api.parametersFrom(request)
		if err != nil {
			rw.WriteHeader(http.StatusBadRequest)
			fmt.Println(err)
			return
		}
		var handler func(*RequestParameters) (int, interface{}, http.Header, error)
		switch request.Method {
		case GET:
			if resource, ok := resource.(GetSupported); ok {
				handler = resource.Get
			}
		case POST:
			if resource, ok := resource.(PostSupported); ok {
				handler = resource.Post
			}
		case PUT:
			if resource, ok := resource.(PutSupported); ok {
				handler = resource.Put
			}
		case DELETE:
			if resource, ok := resource.(DeleteSupported); ok {
				handler = resource.Delete
			}
		}
		if handler == nil {
			rw.WriteHeader(http.StatusMethodNotAllowed)
			fmt.Println(request.Method, " not allowed")
			return
		}
		code, data, header, err := handler(rp)
		if err != nil {
			rw.WriteHeader(http.StatusInternalServerError)
			rw.Write([]byte(err.Error()))
			fmt.Println("Error: ", http.StatusInternalServerError, " - ", err.Error())
			return
		}
		content, err := json.MarshalIndent(data, "", "  ")
		if err != nil {
			rw.WriteHeader(http.StatusInternalServerError)
			fmt.Println("Marshal JSON failed: ", http.StatusInternalServerError, " - ", err.Error())
			return
		}
		for name, values := range header {
			for _, value := range values {
				rw.Header().Add(name, value)
			}
		}
		rw.WriteHeader(code)
		rw.Write(content)
	}
}

func (api *API) Router() *mux.Router {
	if api.initialized {
		return api.router
	} else {
		api.router = mux.NewRouter().StrictSlash(true)
		api.initialized = true
		return api.router
	}
}

func (api *API) AddResource(resource interface{}, paths ...string) {
	for _, path := range paths {
		api.Router().HandleFunc(path, api.requestHandler(resource))
	}
}

func (api *API) Start(port int) error {
	if !api.initialized {
		return errors.New("please add at least one resource to Restful API")
	}
	portString := fmt.Sprintf(":%d", port)
	return http.ListenAndServe(portString, api.Router())
}
