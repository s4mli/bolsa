package job

import (
	"context"
	"sync"
)

////////////////////
// Data Supplier //
type dataSupplier struct {
	data  []interface{}
	index int
	mutex sync.Mutex
}

func (ds *dataSupplier) Drain(ctx context.Context) (interface{}, bool) {
	ds.mutex.Lock()
	ok := ds.index < len(ds.data)
	var d interface{} = nil
	if ok {
		d = ds.data[ds.index]
		ds.index++
	}
	defer ds.mutex.Unlock()
	return d, ok
}

func NewDataSupplier(data []interface{}) Supplier {
	return &dataSupplier{data, 0, sync.Mutex{}}
}
