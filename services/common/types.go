package common

import (
	"container/list"
	"fmt"
)

type DataCell struct {
	Id   string   `json:"id,omitempty"`
	Cmd  string   `json:"cmd,omitempty"`
	Day  int32    `json:"day,omitempty"`
	Mdt  string   `json:"mdt,omitempty"`
	Keys []string `json:"keys"`
	Val  int      `json:"val"`
}

func (c *DataCell) StIndex() (int, error) {
	// Keys[4] is st1 or st2
	switch c.Keys[4] {
	case "st1":
		return 0, nil
	case "st2":
		return 1, nil
	}
	return -1, fmt.Errorf("Invalid storage code: %s for cell %v\n", c.Keys[4], c)
}

func (c *DataCell) ReadTask(task *list.Element, mdt string) bool {
	// copy task to cell
	*c = task.Value.(DataCell)
	if c.Keys[1] <= mdt {
		return true
	}
	return false
}
