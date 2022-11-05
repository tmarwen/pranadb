package iteration

import (
	"github.com/squareup/pranadb/shakti/cmn"
)

type ChainingIterator struct {
	iters []Iterator
	pos   int
}

func NewChainingIterator(iters []Iterator) *ChainingIterator {
	return &ChainingIterator{iters: iters}
}

func (c *ChainingIterator) Current() cmn.KV {
	return c.iters[c.pos].Current()
}

func (c *ChainingIterator) Next() error {
	if err := c.iters[c.pos].Next(); err != nil {
		return err
	}
	valid, err := c.iters[c.pos].IsValid()
	if err != nil {
		return err
	}
	if valid {
		return nil
	}
	c.pos++
	return nil
}

func (c *ChainingIterator) IsValid() (bool, error) {
	if c.pos >= len(c.iters) {
		return false, nil
	}
	return c.iters[c.pos].IsValid()
}

func (c *ChainingIterator) Close() error {
	return nil
}
