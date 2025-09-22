package main

import (
	"errors"
	"strconv"
	"strings"
)

func verifyId(stream string, id string) error {
	sep := strings.Split(id, "-")
	if len(sep) != 2 {
		return errors.New("Invalid ID")
	}
	t, err := strconv.Atoi(sep[0])
	if err != nil {
		return errors.New("Invalid ID")
	}
	n, err := strconv.Atoi(sep[1])
	if err != nil {
		return errors.New("Invalid ID")
	}
	if t <= 0 && n <= 0 {
		return errors.New("ERR The ID specified in XADD must be greater than 0-0")
	}
	last, ok := GlobalStore.streams[stream]
	if !ok {
		return nil
	} else {
		lastEntry := last[len(last)-1]
		sep2 := strings.Split(lastEntry.ID, "-")
		if len(sep2) != 2 {
			return errors.New("Invalid ID")
		}
		t2, err := strconv.Atoi(sep2[0])
		if err != nil {
			return errors.New("Invalid ID")
		}
		n2, err := strconv.Atoi(sep2[1])
		if err != nil {
			return errors.New("Invalid ID")
		}
		if t > t2 {
			return nil
		} else if t == t2 && n > n2 {
			return nil
		}
		return errors.New("ERR The ID specified in XADD is equal or smaller than the target stream top item")
	}
}
