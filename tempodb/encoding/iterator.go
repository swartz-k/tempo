package encoding

import (
	"context"
	"io"
)

type Iterator interface {
	Next(ctx context.Context) (ID, []byte, error)
}

type iterator struct {
	reader io.Reader
}

func NewIterator(reader io.Reader) Iterator {
	return &iterator{
		reader: reader,
	}
}

func (i *iterator) Next(_ context.Context) (ID, []byte, error) {
	return unmarshalObjectFromReader(i.reader)
}
