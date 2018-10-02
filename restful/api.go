package restful

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/samwooo/bolsa/common"
	"github.com/samwooo/bolsa/logging"
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
	ctx    context.Context
	logger logging.Logger
	mux    *http.ServeMux
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
			defer api.logger.Infof("%s %s %v \n", request.RequestURI, request.Method, time.Since(start))
			api.replyWith(rw, http.StatusBadRequest, nil, err)
		} else {
			defer api.logger.Infof("%s %s \n\tForm: %+v\n\tBody: %+v\n\tCost: %+v\n", request.RequestURI,
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
		api.mux.HandleFunc(path, handler)
	}
	return api
}

func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) { api.mux.ServeHTTP(w, r) }
func (api *API) Start(port int) {
	server := http.Server{Addr: fmt.Sprintf(":%d", port), Handler: api}
	common.TerminateIf(api.ctx,
		func() {
			if err := server.Shutdown(api.ctx); err != nil {
				api.logger.Errorf("cancellation, shutdown failed: %s", err.Error())
			} else {
				api.logger.Errorf("cancellation, terminated")
			}
		},
		func(s os.Signal) {
			if err := server.Shutdown(api.ctx); err != nil {
				api.logger.Errorf("signal ( %+v ), shutdown failed: %s", s, err.Error())
			} else {
				api.logger.Infof("signal ( %+v ), terminated", s)
			}
		})

	api.logger.Infof("listening on :%d", port)
	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		api.logger.Errorf("start failed: ", err.Error())
	}
}

func NewAPI(ctx context.Context) *API {
	return &API{ctx, logging.GetLogger(" ⓡ "), http.NewServeMux()}
}
