package table

import "testing"

func TestIsYYYYMMDD(t *testing.T) {
	{
		b, err := IsYYYYMMDD("hoge20190101", "20180101", "20190102")
		if err != nil {
			t.Fatal(err)
		}
		if !b {
			t.Fatal("miss 1")
		}
	}
	{
		b, err := IsYYYYMMDD("hoge20190101", "20190101", "20190101")
		if err != nil {
			t.Fatal(err)
		}
		if !b {
			t.Fatal("miss 2")
		}
	}
	{
		b, err := IsYYYYMMDD("hoge20190101", "20180101", "20181231")
		if err != nil {
			t.Fatal(err)
		}
		if b {
			t.Fatal("miss 3")
		}
	}
	{
		b, err := IsYYYYMMDD("hoge20190101", "20190102", "20191231")
		if err != nil {
			t.Fatal(err)
		}
		if b {
			t.Fatal("miss 4")
		}
	}
}
