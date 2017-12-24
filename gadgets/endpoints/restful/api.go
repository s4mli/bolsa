package restful

import (
	"encoding/json"
	"net/http"
	"net/url"
	"time"

	"github.com/samwooo/bolsa/gadgets/logging"
)

const (
	GET    = "GET"
	PUT    = "PUT"
	POST   = "POST"
	DELETE = "DELETE"
)

type Context struct {
	head http.Header
	form url.Values
	body interface{}
}

type getSupported interface {
	Get(c *Context) (interface{}, error)
}

type postSupported interface {
	Post(c *Context) (interface{}, error)
}

type putSupported interface {
	Put(c *Context) (interface{}, error)
}

type deleteSupported interface {
	Delete(c *Context) (interface{}, error)
}

type API struct {
	Logger logging.Logger
}

func (api *API) contextFrom(r *http.Request) (*Context, error) {
	if err := r.ParseForm(); err != nil {
		return nil, err
	}
	var body interface{}
	decoder := json.NewDecoder(r.Body)
	defer r.Body.Close()
	if err := decoder.Decode(&body); err != nil {
		return &Context{r.Header, r.Form, nil}, nil
	}
	return &Context{r.Header, r.Form, body}, nil
}

func (api *API) handlerFor(resource interface{}, method string) func(*Context) (interface{}, error) {
	optMap := map[string]func(*Context) (interface{}, error){
		GET: func(resource interface{}) func(*Context) (interface{}, error) {
			if r, ok := resource.(getSupported); ok {
				return r.Get
			}
			return nil
		}(resource),

		PUT: func(resource interface{}) func(*Context) (interface{}, error) {
			if resource, ok := resource.(putSupported); ok {
				return resource.Put
			}
			return nil
		}(resource),

		POST: func(resource interface{}) func(*Context) (interface{}, error) {
			if resource, ok := resource.(postSupported); ok {
				return resource.Post
			}
			return nil
		}(resource),

		DELETE: func(resource interface{}) func(*Context) (interface{}, error) {
			if resource, ok := resource.(deleteSupported); ok {
				return resource.Delete
			}
			return nil
		}(resource),
	}

	fn, ok := optMap[method]
	if !ok {
		return nil
	}
	return fn
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
		defer api.Logger.Debugf("%s %s %v\n", request.Method, request.RequestURI, time.Since(start))
		if context, err := api.contextFrom(request); err != nil {
			api.replyWith(rw, http.StatusBadRequest, nil, err)
		} else {
			api.Logger.Debugf("%s %s \nHead: %+v\nForm: %+v\nBody: %+v", request.Method, request.RequestURI, context.head,
				context.form, context.body)
			if handler := api.handlerFor(resource, request.Method); handler != nil {
				if data, err := handler(context); err != nil {
					api.replyWith(rw, http.StatusInternalServerError, nil, err)
				} else {
					if content, err := json.MarshalIndent(data, "", "  "); err != nil {
						api.replyWith(rw, http.StatusInternalServerError, nil, err)
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

func (api *API) RegisterResource(resource interface{}, paths ...string) {
	for _, path := range paths {
		handler := api.requestHandler(resource)
		http.HandleFunc(path, handler)
	}
}
