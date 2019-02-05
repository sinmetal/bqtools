package table

import "testing"

func TestIsYYYYMMDD(t *testing.T) {
	cases := []struct {
		name  string
		table string
		start string
		end   string
		want  bool
	}{
		{"範囲内", "hoge20190101", "20180101", "20190102", true},
		{"ジャスト", "hoge20190101", "20180101", "20190101", true},
		{"期間を過ぎてる", "hoge20190101", "20180101", "20181231", false},
		{"期間より前", "hoge20190101", "20190102", "20191231", false},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("unexpected panic: %+v", r)
				}
			}()
			got, err := IsYYYYMMDD(tt.table, tt.start, tt.end)
			if err != nil {
				t.Errorf("unexpected err: %+v", err)
			}
			if w, g := tt.want, got; w != g {
				t.Errorf("want %v but got %v", w, g)
			}
		})
	}
}
