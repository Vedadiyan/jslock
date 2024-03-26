package main

import (
	"time"

	"github.com/nats-io/nats.go"
)

type (
	JSLock struct {
		locker nats.KeyValue
	}
)

func New(url string, lockerName string, options ...nats.Option) (*JSLock, error) {
	conn, err := nats.Connect(url, options...)
	if err != nil {
		return nil, err
	}
	js, err := conn.JetStream()
	if err != nil {
		return nil, err
	}
	keyValue, err := js.KeyValue(lockerName)
	if err == nats.ErrBucketNotFound {
		keyValue, err = js.CreateKeyValue(&nats.KeyValueConfig{
			Bucket: lockerName,
		})
		if err != nil {
			return nil, err
		}
	}
	locker := JSLock{
		locker: keyValue,
	}
	return &locker, nil
}

func (jsLock *JSLock) Monitor(name string, ttl time.Duration) (bool, error) {
	key, err := jsLock.locker.Get(name)
	if err == nats.ErrKeyNotFound {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if time.Since(key.Created()).Nanoseconds() <= ttl.Nanoseconds() {
		return false, nil
	}
	return true, jsLock.locker.Delete(name)
}

func (jsLock *JSLock) Lock(name string, ttl time.Duration) (bool, error) {
	monitor, err := jsLock.Monitor(name, ttl)
	if err != nil {
		return false, err
	}
	if !monitor {
		return false, nil
	}
	_, err = jsLock.locker.Create(name, []byte{})
	if err != nil {
		if err == nats.ErrKeyExists {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (jsLock *JSLock) UnLock(name string) error {
	return jsLock.locker.Delete(name)
}
