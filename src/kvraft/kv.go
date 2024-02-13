package kvraft

type KVCache interface {
	Get(key string) (string, Err)
	Put(key, value string) Err
	Append(key, value string) Err
}

type KVMemory struct {
	KV map[string]string
}

func NewKVCache() *KVMemory {
	return &KVMemory{make(map[string]string)}
}

func (memoryKV *KVMemory) Get(key string) (string, Err) {
	if value, ok := memoryKV.KV[key]; ok {
		return value, OK
	}
	return "", ErrNoKey
}

func (memoryKV *KVMemory) Put(key, value string) Err {
	memoryKV.KV[key] = value
	return OK
}

func (memoryKV *KVMemory) Append(key, value string) Err {
	memoryKV.KV[key] += value
	return OK
}
