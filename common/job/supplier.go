package job

import "fmt"

////////////////////
// Data Supplier //
type dataSupplier struct {
	data []interface{}
	in   <-chan Done
}

func (ds *dataSupplier) Name() string       { return fmt.Sprintf("✔ data ( %d )", len(ds.data)) }
func (ds *dataSupplier) Adapt() <-chan Done { return ds.in }
func NewDataSupplier(data []interface{}) supplier {
	in := make(chan Done)
	go func() {
		for _, d := range data {
			in <- newDone(nil, d, nil)
		}
		close(in)
	}()
	return &dataSupplier{data, in}
}
