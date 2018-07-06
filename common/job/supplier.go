package job

////////////////////
// Data Supplier //
type dataSupplier struct {
	data []interface{}
	in   <-chan Done
}

func (ds *dataSupplier) Adapt() <-chan Done {
	return ds.in
}

func NewDataSupplier(data []interface{}) supplier {
	in := make(chan Done)
	go func() {
		for _, d := range data {
			in <- newDone(nil, d, nil, nil)
		}
		close(in)
	}()
	return &dataSupplier{data, in}
}
