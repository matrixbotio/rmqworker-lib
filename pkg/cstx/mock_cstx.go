// Code generated by mockery v2.14.1. DO NOT EDIT.

package cstx

import (
	errs "github.com/matrixbotio/rmqworker-lib/pkg/errs"
	mock "github.com/stretchr/testify/mock"

	structs "github.com/matrixbotio/rmqworker-lib/pkg/structs"
)

// MockCSTX is an autogenerated mock type for the CSTX type
type MockCSTX struct {
	mock.Mock
}

type MockCSTX_Expecter struct {
	mock *mock.Mock
}

func (_m *MockCSTX) EXPECT() *MockCSTX_Expecter {
	return &MockCSTX_Expecter{mock: &_m.Mock}
}

// Commit provides a mock function with given fields:
func (_m *MockCSTX) Commit() error {
	ret := _m.Called()

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MockCSTX_Commit_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Commit'
type MockCSTX_Commit_Call struct {
	*mock.Call
}

// Commit is a helper method to define mock.On call
func (_e *MockCSTX_Expecter) Commit() *MockCSTX_Commit_Call {
	return &MockCSTX_Commit_Call{Call: _e.mock.On("Commit")}
}

func (_c *MockCSTX_Commit_Call) Run(run func()) *MockCSTX_Commit_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *MockCSTX_Commit_Call) Return(_a0 error) *MockCSTX_Commit_Call {
	_c.Call.Return(_a0)
	return _c
}

// GetID provides a mock function with given fields:
func (_m *MockCSTX) GetID() string {
	ret := _m.Called()

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// MockCSTX_GetID_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetID'
type MockCSTX_GetID_Call struct {
	*mock.Call
}

// GetID is a helper method to define mock.On call
func (_e *MockCSTX_Expecter) GetID() *MockCSTX_GetID_Call {
	return &MockCSTX_GetID_Call{Call: _e.mock.On("GetID")}
}

func (_c *MockCSTX_GetID_Call) Run(run func()) *MockCSTX_GetID_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *MockCSTX_GetID_Call) Return(_a0 string) *MockCSTX_GetID_Call {
	_c.Call.Return(_a0)
	return _c
}

// PublishToExchange provides a mock function with given fields: task
func (_m *MockCSTX) PublishToExchange(task structs.PublishToExchangeTask) errs.APIError {
	ret := _m.Called(task)

	var r0 errs.APIError
	if rf, ok := ret.Get(0).(func(structs.PublishToExchangeTask) errs.APIError); ok {
		r0 = rf(task)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(errs.APIError)
		}
	}

	return r0
}

// MockCSTX_PublishToExchange_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'PublishToExchange'
type MockCSTX_PublishToExchange_Call struct {
	*mock.Call
}

// PublishToExchange is a helper method to define mock.On call
//  - task structs.PublishToExchangeTask
func (_e *MockCSTX_Expecter) PublishToExchange(task interface{}) *MockCSTX_PublishToExchange_Call {
	return &MockCSTX_PublishToExchange_Call{Call: _e.mock.On("PublishToExchange", task)}
}

func (_c *MockCSTX_PublishToExchange_Call) Run(run func(task structs.PublishToExchangeTask)) *MockCSTX_PublishToExchange_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(structs.PublishToExchangeTask))
	})
	return _c
}

func (_c *MockCSTX_PublishToExchange_Call) Return(_a0 errs.APIError) *MockCSTX_PublishToExchange_Call {
	_c.Call.Return(_a0)
	return _c
}

// PublishToQueue provides a mock function with given fields: task
func (_m *MockCSTX) PublishToQueue(task structs.RMQPublishRequestTask) errs.APIError {
	ret := _m.Called(task)

	var r0 errs.APIError
	if rf, ok := ret.Get(0).(func(structs.RMQPublishRequestTask) errs.APIError); ok {
		r0 = rf(task)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(errs.APIError)
		}
	}

	return r0
}

// MockCSTX_PublishToQueue_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'PublishToQueue'
type MockCSTX_PublishToQueue_Call struct {
	*mock.Call
}

// PublishToQueue is a helper method to define mock.On call
//  - task structs.RMQPublishRequestTask
func (_e *MockCSTX_Expecter) PublishToQueue(task interface{}) *MockCSTX_PublishToQueue_Call {
	return &MockCSTX_PublishToQueue_Call{Call: _e.mock.On("PublishToQueue", task)}
}

func (_c *MockCSTX_PublishToQueue_Call) Run(run func(task structs.RMQPublishRequestTask)) *MockCSTX_PublishToQueue_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(structs.RMQPublishRequestTask))
	})
	return _c
}

func (_c *MockCSTX_PublishToQueue_Call) Return(_a0 errs.APIError) *MockCSTX_PublishToQueue_Call {
	_c.Call.Return(_a0)
	return _c
}

// Rollback provides a mock function with given fields:
func (_m *MockCSTX) Rollback() error {
	ret := _m.Called()

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MockCSTX_Rollback_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Rollback'
type MockCSTX_Rollback_Call struct {
	*mock.Call
}

// Rollback is a helper method to define mock.On call
func (_e *MockCSTX_Expecter) Rollback() *MockCSTX_Rollback_Call {
	return &MockCSTX_Rollback_Call{Call: _e.mock.On("Rollback")}
}

func (_c *MockCSTX_Rollback_Call) Run(run func()) *MockCSTX_Rollback_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *MockCSTX_Rollback_Call) Return(_a0 error) *MockCSTX_Rollback_Call {
	_c.Call.Return(_a0)
	return _c
}

// Serialize provides a mock function with given fields:
func (_m *MockCSTX) Serialize() string {
	ret := _m.Called()

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// MockCSTX_Serialize_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Serialize'
type MockCSTX_Serialize_Call struct {
	*mock.Call
}

// Serialize is a helper method to define mock.On call
func (_e *MockCSTX_Expecter) Serialize() *MockCSTX_Serialize_Call {
	return &MockCSTX_Serialize_Call{Call: _e.mock.On("Serialize")}
}

func (_c *MockCSTX_Serialize_Call) Run(run func()) *MockCSTX_Serialize_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *MockCSTX_Serialize_Call) Return(_a0 string) *MockCSTX_Serialize_Call {
	_c.Call.Return(_a0)
	return _c
}

type mockConstructorTestingTNewMockCSTX interface {
	mock.TestingT
	Cleanup(func())
}

// NewMockCSTX creates a new instance of MockCSTX. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewMockCSTX(t mockConstructorTestingTNewMockCSTX) *MockCSTX {
	mock := &MockCSTX{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}