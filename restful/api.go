package restful

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

const (
	GET    = "GET"
	PUT    = "PUT"
	POST   = "POST"
	DELETE = "DELETE"
)

type Request struct {
	Header http.Header
	Form   url.Values
	Body   interface{}
}

type getSupported interface {
	Get(*Request) (interface{}, error)
}

type postSupported interface {
	Post(*Request) (interface{}, error)
}

type putSupported interface {
	Put(*Request) (interface{}, error)
}

type deleteSupported interface {
	Delete(*Request) (interface{}, error)
}

type API struct {
	Router map[string]http.HandlerFunc
}

func (api *API) requestFrom(r *http.Request) (*Request, error) {
	if err := r.ParseForm(); err != nil {
		return nil, err
	}
	var body interface{}
	decoder := json.NewDecoder(r.Body)
	defer r.Body.Close()
	if err := decoder.Decode(&body); err != nil {
		return &Request{r.Header, r.Form, nil}, nil
	}
	return &Request{r.Header, r.Form, body}, nil
}

func (api *API) handlerFor(resource interface{}, method string) func(*Request) (interface{}, error) {
	optMap := map[string]func(*Request) (interface{}, error){
		GET: func(resource interface{}) func(*Request) (interface{}, error) {
			if r, ok := resource.(getSupported); ok {
				return r.Get
			}
			return nil
		}(resource),

		PUT: func(resource interface{}) func(*Request) (interface{}, error) {
			if r, ok := resource.(putSupported); ok {
				return r.Put
			}
			return nil
		}(resource),

		POST: func(resource interface{}) func(*Request) (interface{}, error) {
			if r, ok := resource.(postSupported); ok {
				return r.Post
			}
			return nil
		}(resource),

		DELETE: func(resource interface{}) func(*Request) (interface{}, error) {
			if r, ok := resource.(deleteSupported); ok {
				return r.Delete
			}
			return nil
		}(resource),
	}

	if fn, ok := optMap[method]; !ok {
		return nil
	} else {
		return fn
	}
}

func (api *API) replyWith(rw http.ResponseWriter, code int, data []byte, err error) {
	rw.WriteHeader(code)
	if err != nil {
		rw.Write([]byte(err.Error()))
	} else {
		rw.Write(data)
	}
}

func (api *API) requestHandler(resource interface{}) http.HandlerFunc {
	return func(rw http.ResponseWriter, request *http.Request) {
		start := time.Now()
		if message, err := api.requestFrom(request); err != nil {
			defer fmt.Printf("%s %s %v \n", request.RequestURI, request.Method, time.Since(start))
			api.replyWith(rw, http.StatusBadRequest, nil, err)
		} else {
			defer fmt.Printf("%s %s \n\tForm: %+v\n\tBody: %+v\n\tCost: %+v\n", request.RequestURI,
				request.Method, message.Form, message.Body, time.Since(start))
			if handler := api.handlerFor(resource, request.Method); handler != nil {
				if data, e := handler(message); e != nil {
					api.replyWith(rw, http.StatusInternalServerError, nil, e)
				} else {
					if content, er := json.MarshalIndent(data, "", "  "); er != nil {
						api.replyWith(rw, http.StatusInternalServerError, nil, er)
					} else {
						api.replyWith(rw, http.StatusOK, content, nil)
					}
				}
			} else {
				api.replyWith(rw, http.StatusMethodNotAllowed, nil, nil)
			}
		}
	}
}

func (api *API) RegisterResource(resource interface{}, paths ...string) *API {
	for _, path := range paths {
		handler := api.requestHandler(resource)
		api.Router[path] = handler
		http.HandleFunc(path, handler)
	}
	return api
}

func (api *API) Start(port int) {
	fmt.Println("server is running on port ", port)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", port), nil); err != nil {
		fmt.Println("restful api error: ", err.Error())
	}
}

func NewAPI() *API { return &API{make(map[string]http.HandlerFunc)} }
