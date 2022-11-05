package iteration

import (
	"github.com/squareup/pranadb/shakti/cmn"
)

type Iterator interface {
	Current() cmn.KV
	Next() error
	IsValid() (bool, error)
	Close() error
}
