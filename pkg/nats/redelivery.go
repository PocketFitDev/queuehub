package nats

type RedeliveryStorage interface {
	Attempt(key string) (int64, error)
}

type inMemRedeliveryStorage struct {
	m map[string]int64
}

func (mem *inMemRedeliveryStorage) Attempt(key string) (int64, error) {
	attempt := mem.m[key]
	mem.m[key] += 1
	return attempt, nil
}

func NewInMem() RedeliveryStorage {
	return &inMemRedeliveryStorage{
		m: make(map[string]int64),
	}
}
