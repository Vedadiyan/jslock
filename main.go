package jslock

import (
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

type (
	JSLock struct {
		locker nats.KeyValue
		nc     *nats.Conn
	}
	Option       func(*JSLock)
	Release      func() error
	UnSubscriber func() error
)

func New(nc *nats.Conn, lockerName string, options ...Option) (*JSLock, error) {
	js, err := nc.JetStream()
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
		nc:     nc,
	}
	for _, option := range options {
		option(&locker)
	}
	return &locker, nil
}

func (jsLock *JSLock) Monitor(name string) (bool, error) {
	key, err := jsLock.locker.Get(name)
	if err == nats.ErrKeyNotFound {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	_, err = jsLock.nc.Request(string(key.Value()), nil, time.Second*2)
	if err == nats.ErrNoResponders {
		return true, jsLock.locker.Delete(name)
	}
	if err != nil {
		return false, err
	}
	return false, nil
}

func (jsLock *JSLock) Lock(name string) (Release, error) {
	ok, err := jsLock.Monitor(name)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("lock is already in use")
	}
	inbox, unsubscriber, err := jsLock.poll()
	if err != nil {
		return nil, err
	}
	_, err = jsLock.locker.Create(name, []byte(inbox))
	if err != nil {
		return nil, err
	}
	return func() error {
		return jsLock.unLock(name, unsubscriber)
	}, nil
}

func (jsLock *JSLock) LockOnBehalf(name string, inbox string) (Release, error) {
	ok, err := jsLock.Monitor(name)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("lock is already in use")
	}
	_, err = jsLock.locker.Create(name, []byte(inbox))
	if err != nil {
		return nil, err
	}
	return func() error {
		return jsLock.unLock(name, func() error {
			msg := nats.Msg{}
			msg.Header = nats.Header{}
			msg.Header.Set("cancel", "true")
			msg.Subject = inbox
			return jsLock.nc.PublishMsg(&msg)
		})
	}, nil
}

func (jsLock *JSLock) poll() (string, UnSubscriber, error) {
	value := ""
	value = nats.NewInbox()
	subs, err := jsLock.nc.Subscribe(value, func(msg *nats.Msg) {
		msg.RespondMsg(nats.NewMsg(msg.Reply))
	})
	if err != nil {
		return value, func() error { return nil }, err
	}
	return value, func() error {
		return subs.Unsubscribe()
	}, nil
}

func (jsLock *JSLock) unLock(name string, unsubscriber UnSubscriber) error {
	unsubscriber()
	return jsLock.locker.Delete(name)
}

func Acquired(r Release) bool {
	return r != nil
}
