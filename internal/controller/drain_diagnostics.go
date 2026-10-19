package controller

import (
	"fmt"
	"io"
	"time"
)

const (
	drainLogTimeout   = 3 * time.Second
	drainLogByteLimit = 256 * 1024
	drainLogLookback  = 15 * time.Minute
)

func readDrainLogs(r io.Reader) (string, error) {
	b, err := io.ReadAll(io.LimitReader(r, drainLogByteLimit+1))
	if err != nil {
		return "", fmt.Errorf("HCCO log read unavailable")
	}
	if len(b) > drainLogByteLimit {
		return "", fmt.Errorf("HCCO log byte limit exceeded")
	}
	return string(b), nil
}
