package processing

import (
	"context"
	"fmt"
)

// Receiver - receives scanning results and stores it in some storage
type Receiver struct {
	storage Storage
}

// New - Receiver constructor
func New(storage Storage) (*Receiver, error) {
	if storage == nil {
		return nil, fmt.Errorf("cannot instantiate a Receiver, no storage provided")
	}
	return &Receiver{storage: storage}, nil
}

// Process - store a scanning result in a storage
// returns OK if the record was updated, otherwise returns false
// in case if storage operation fails - returns an error
func (r *Receiver) Process(ctx context.Context, scn *ScanResult) (int64, error) {
	return r.storage.Put(ctx, scn)
}
