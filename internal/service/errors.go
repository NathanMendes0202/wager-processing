package service

import "errors"

var ErrPendingReference = errors.New("wager transaction is waiting for its reference")
