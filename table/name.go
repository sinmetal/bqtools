package table

import (
	"time"
	"unicode/utf8"
)

// IsYYYYMMDD is tableで指定したテーブル名がstart, endで指定したYYYYMMDDの範囲にあるかを返す
// Ex: table=hoge20190101 start=20180101 end=20190102
func IsYYYYMMDD(table string, start string, end string) (bool, error) {
	s, err := time.Parse("20060102", start)
	if err != nil {
		return false, err
	}
	e, err := time.Parse("20060102", end)
	if err != nil {
		return false, err
	}

	c := utf8.RuneCountInString(table)
	tdate := table[c-8:]
	t, err := time.Parse("20060102", tdate)
	if err != nil {
		return false, err
	}

	if s.Unix() > t.Unix() {
		return false, nil
	}
	if e.Unix() < t.Unix() {
		return false, nil
	}

	return true, nil
}
