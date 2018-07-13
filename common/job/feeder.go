package job

import "fmt"

////////////////////
// Data Feeder //
type dataFeeder struct {
	data []interface{}
	in   <-chan Done
}

func (ds *dataFeeder) Name() string       { return fmt.Sprintf("✔︎ data ( %d )", len(ds.data)) }
func (ds *dataFeeder) Adapt() <-chan Done { return ds.in }
func NewDataFeeder(data []interface{}) feeder {
	in := make(chan Done)
	go func() {
		for _, d := range data {
			in <- newDone(nil, d, nil)
		}
		close(in)
	}()
	return &dataFeeder{data, in}
}
