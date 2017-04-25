package mocks

import "github.com/stretchr/testify/mock"

type Conn struct {
	mock.Mock
}

// Send provides a mock function with given fields: cmd, args
func (_m *Conn) Send(cmd string, args ...interface{}) error {
	ret := _m.Called(cmd, args)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, ...interface{}) error); ok {
		r0 = rf(cmd, args...)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Flush provides a mock function with given fields:
func (_m *Conn) Flush() error {
	ret := _m.Called()

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}
