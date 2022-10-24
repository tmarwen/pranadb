package shakti

type CloudStore interface {
	Get(key []byte) ([]byte, error)
	Add(key []byte, value []byte) error
	Delete(key []byte) error
}
