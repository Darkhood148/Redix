package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

func verifyId(stream string, id string) error {
	if id == "*" {
		return nil
	}
	sep := strings.Split(id, "-")
	if len(sep) != 2 {
		return errors.New("Invalid ID")
	}
	t, err := strconv.Atoi(sep[0])
	if err != nil {
		return errors.New("Invalid ID")
	}
	if sep[1] == "*" {
		return nil
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

func completeId(stream string, id string) (string, error) {
	if id == "*" {
		return fmt.Sprintf("%d-0", time.Now().UnixNano()/1e6), nil
	}
	sep := strings.Split(id, "-")
	if sep[1] != "*" {
		return id, nil
	}
	x := GlobalStore.streams[stream]
	if len(x) == 0 {
		if sep[0] != "0" {
			return sep[0] + "-0", nil
		} else {
			return sep[0] + "-1", nil
		}
	} else {
		mx := -1
		for _, entry := range x {
			sep2 := strings.Split(entry.ID, "-")
			if sep2[0] == sep[0] {
				n, _ := strconv.Atoi(sep2[1])
				mx = max(mx, n)
			}
		}
		return sep[0] + "-" + strconv.Itoa(mx+1), nil
	}
}

func idGreaterThan(id, compare string) bool {
	if compare == "-" {
		return true
	}
	idSplit := strings.Split(id, "-")
	compareSplit := strings.Split(compare, "-")
	n1, err := strconv.Atoi(idSplit[0])
	if err != nil {
		return false
	}
	n2, err := strconv.Atoi(compareSplit[0])
	if err != nil {
		return false
	}
	if n1 > n2 {
		return true
	}
	if n1 < n2 {
		return false
	}
	if len(idSplit) == 1 {
		return true
	}
	s1, err := strconv.Atoi(idSplit[1])
	if err != nil {
		return false
	}
	s2, err := strconv.Atoi(compareSplit[1])
	if err != nil {
		return false
	}
	if s1 >= s2 {
		return true
	} else {
		return false
	}
}

func idLessThan(id, compare string) bool {
	idSplit := strings.Split(id, "-")
	compareSplit := strings.Split(compare, "-")
	n1, err := strconv.Atoi(idSplit[0])
	if err != nil {
		return false
	}
	n2, err := strconv.Atoi(compareSplit[0])
	if err != nil {
		return false
	}
	if n1 < n2 {
		return true
	}
	if n1 > n2 {
		return false
	}
	if len(idSplit) == 1 {
		return true
	}
	s1, err := strconv.Atoi(idSplit[1])
	if err != nil {
		return false
	}
	s2, err := strconv.Atoi(compareSplit[1])
	if err != nil {
		return false
	}
	if s1 <= s2 {
		return true
	} else {
		return false
	}
}

func idsInRange(id, start, stop string) bool {
	return idGreaterThan(id, start) && idLessThan(id, stop)
}
