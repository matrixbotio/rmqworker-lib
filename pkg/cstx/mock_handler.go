// Code generated by mockery v2.14.1. DO NOT EDIT.

package cstx

import (
	errs "github.com/matrixbotio/rmqworker-lib/pkg/errs"
	mock "github.com/stretchr/testify/mock"

	structs "github.com/matrixbotio/rmqworker-lib/pkg/structs"
)

// MockHandler is an autogenerated mock type for the Handler type
type MockHandler struct {
	mock.Mock
}

type MockHandler_Expecter struct {
	mock *mock.Mock
}

func (_m *MockHandler) EXPECT() *MockHandler_Expecter {
	return &MockHandler_Expecter{mock: &_m.Mock}
}

// PublishCSXTToExchange provides a mock function with given fields: task, cstx
func (_m *MockHandler) PublishCSXTToExchange(task structs.PublishToExchangeTask, cstx CrossServiceTransaction) errs.APIError {
	ret := _m.Called(task, cstx)

	var r0 errs.APIError
	if rf, ok := ret.Get(0).(func(structs.PublishToExchangeTask, CrossServiceTransaction) errs.APIError); ok {
		r0 = rf(task, cstx)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(errs.APIError)
		}
	}

	return r0
}

// MockHandler_PublishCSXTToExchange_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'PublishCSXTToExchange'
type MockHandler_PublishCSXTToExchange_Call struct {
	*mock.Call
}

// PublishCSXTToExchange is a helper method to define mock.On call
//  - task structs.PublishToExchangeTask
//  - cstx CrossServiceTransaction
func (_e *MockHandler_Expecter) PublishCSXTToExchange(task interface{}, cstx interface{}) *MockHandler_PublishCSXTToExchange_Call {
	return &MockHandler_PublishCSXTToExchange_Call{Call: _e.mock.On("PublishCSXTToExchange", task, cstx)}
}

func (_c *MockHandler_PublishCSXTToExchange_Call) Run(run func(task structs.PublishToExchangeTask, cstx CrossServiceTransaction)) *MockHandler_PublishCSXTToExchange_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(structs.PublishToExchangeTask), args[1].(CrossServiceTransaction))
	})
	return _c
}

func (_c *MockHandler_PublishCSXTToExchange_Call) Return(_a0 errs.APIError) *MockHandler_PublishCSXTToExchange_Call {
	_c.Call.Return(_a0)
	return _c
}

// PublishCSXTToQueue provides a mock function with given fields: task, cstx
func (_m *MockHandler) PublishCSXTToQueue(task structs.RMQPublishRequestTask, cstx CrossServiceTransaction) errs.APIError {
	ret := _m.Called(task, cstx)

	var r0 errs.APIError
	if rf, ok := ret.Get(0).(func(structs.RMQPublishRequestTask, CrossServiceTransaction) errs.APIError); ok {
		r0 = rf(task, cstx)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(errs.APIError)
		}
	}

	return r0
}

// MockHandler_PublishCSXTToQueue_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'PublishCSXTToQueue'
type MockHandler_PublishCSXTToQueue_Call struct {
	*mock.Call
}

// PublishCSXTToQueue is a helper method to define mock.On call
//  - task structs.RMQPublishRequestTask
//  - cstx CrossServiceTransaction
func (_e *MockHandler_Expecter) PublishCSXTToQueue(task interface{}, cstx interface{}) *MockHandler_PublishCSXTToQueue_Call {
	return &MockHandler_PublishCSXTToQueue_Call{Call: _e.mock.On("PublishCSXTToQueue", task, cstx)}
}

func (_c *MockHandler_PublishCSXTToQueue_Call) Run(run func(task structs.RMQPublishRequestTask, cstx CrossServiceTransaction)) *MockHandler_PublishCSXTToQueue_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(structs.RMQPublishRequestTask), args[1].(CrossServiceTransaction))
	})
	return _c
}

func (_c *MockHandler_PublishCSXTToQueue_Call) Return(_a0 errs.APIError) *MockHandler_PublishCSXTToQueue_Call {
	_c.Call.Return(_a0)
	return _c
}

// PublishToExchange provides a mock function with given fields: task, additionalHeaders
func (_m *MockHandler) PublishToExchange(task structs.PublishToExchangeTask, additionalHeaders ...structs.RMQHeader) errs.APIError {
	_va := make([]interface{}, len(additionalHeaders))
	for _i := range additionalHeaders {
		_va[_i] = additionalHeaders[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, task)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	var r0 errs.APIError
	if rf, ok := ret.Get(0).(func(structs.PublishToExchangeTask, ...structs.RMQHeader) errs.APIError); ok {
		r0 = rf(task, additionalHeaders...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(errs.APIError)
		}
	}

	return r0
}

// MockHandler_PublishToExchange_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'PublishToExchange'
type MockHandler_PublishToExchange_Call struct {
	*mock.Call
}

// PublishToExchange is a helper method to define mock.On call
//  - task structs.PublishToExchangeTask
//  - additionalHeaders ...structs.RMQHeader
func (_e *MockHandler_Expecter) PublishToExchange(task interface{}, additionalHeaders ...interface{}) *MockHandler_PublishToExchange_Call {
	return &MockHandler_PublishToExchange_Call{Call: _e.mock.On("PublishToExchange",
		append([]interface{}{task}, additionalHeaders...)...)}
}

func (_c *MockHandler_PublishToExchange_Call) Run(run func(task structs.PublishToExchangeTask, additionalHeaders ...structs.RMQHeader)) *MockHandler_PublishToExchange_Call {
	_c.Call.Run(func(args mock.Arguments) {
		variadicArgs := make([]structs.RMQHeader, len(args)-1)
		for i, a := range args[1:] {
			if a != nil {
				variadicArgs[i] = a.(structs.RMQHeader)
			}
		}
		run(args[0].(structs.PublishToExchangeTask), variadicArgs...)
	})
	return _c
}

func (_c *MockHandler_PublishToExchange_Call) Return(_a0 errs.APIError) *MockHandler_PublishToExchange_Call {
	_c.Call.Return(_a0)
	return _c
}

type mockConstructorTestingTNewMockHandler interface {
	mock.TestingT
	Cleanup(func())
}

// NewMockHandler creates a new instance of MockHandler. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewMockHandler(t mockConstructorTestingTNewMockHandler) *MockHandler {
	mock := &MockHandler{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}