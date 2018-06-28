package job

import (
	"context"
)

////////////////////
// Data Supplier //
type dataSupplier struct {
	data    []interface{}
	current int
}

func (ds *dataSupplier) Drain(ctx context.Context) (interface{}, error) {
	d := ds.data[ds.current]
	ds.current++
	return d, nil
}
func (ds *dataSupplier) Empty() bool {
	return ds.current >= len(ds.data)
}

func NewDataSupplier(data []interface{}) Supplier {
	return &dataSupplier{data, 0}
}
