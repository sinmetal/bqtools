package table

import (
	"strings"
	"time"
	"unicode/utf8"

	"github.com/morikuni/failure"
)

type SearchOption struct {
	TablePrefix string
	Start       string // YYYYMMDD
	End         string // YYYYMMDD
}

func (o *SearchOption) Check(tableName string) (bool, error) {
	if len(o.TablePrefix) > 0 {
		if !strings.HasPrefix(tableName, o.TablePrefix) {
			return false, nil
		}
	}

	b, err := IsYYYYMMDD(tableName, o.Start, o.End)
	if err != nil {
		return false, failure.Wrap(err)
	}
	return b, nil
}

// IsYYYYMMDD is tableで指定したテーブル名がstart, endで指定したYYYYMMDDの範囲にあるかを返す
// Ex: table=hoge20190101 start=20180101 end=20190102
func IsYYYYMMDD(tableName string, start string, end string) (bool, error) {
	s, err := time.Parse("20060102", start)
	if err != nil {
		return false, failure.Translate(err, InvalidArgument, failure.MessageKV{"start": start})
	}
	e, err := time.Parse("20060102", end)
	if err != nil {
		return false, failure.Translate(err, InvalidArgument, failure.MessageKV{"end": end})
	}

	c := utf8.RuneCountInString(tableName)
	tdate := tableName[c-8:]
	t, err := time.Parse("20060102", tdate)
	if err != nil {
		return false, failure.Translate(err, InvalidArgument, failure.MessageKV{"tdate": tdate})
	}

	if s.Unix() > t.Unix() {
		return false, nil
	}
	if e.Unix() < t.Unix() {
		return false, nil
	}

	return true, nil
}
