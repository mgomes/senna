package senna

import (
	"context"
	"testing"
)

func FuzzCursorRoundTrip(f *testing.F) {
	f.Add("cursor", int64(42), true)
	f.Add("", int64(0), false)

	f.Fuzz(func(t *testing.T, key string, offset int64, done bool) {
		if !smallValidString(key) {
			t.Skip()
		}

		type position struct {
			Key    string `json:"key"`
			Offset int64  `json:"offset"`
			Done   bool   `json:"done"`
		}

		want := position{Key: key, Offset: offset, Done: done}
		cursor := CursorFrom(want)
		if cursor == nil {
			t.Fatalf("CursorFrom(%+v) = nil, want JSON cursor", want)
		}

		got, err := CursorTo[position](cursor)
		if err != nil {
			t.Fatalf("CursorTo[position](CursorFrom(%+v)) error = %v", want, err)
		}
		if got != want {
			t.Errorf("CursorTo[position](CursorFrom(%+v)) = %+v, want %+v", want, got, want)
		}
	})
}

func FuzzSliceIterator(f *testing.F) {
	f.Add([]byte("abc"), 0)
	f.Add([]byte("abc"), 2)
	f.Add([]byte{}, -1)

	f.Fuzz(func(t *testing.T, items []byte, offset int) {
		if len(items) > 1024 {
			t.Skip()
		}

		start := offset
		if start < 0 {
			start = 0
		}
		if start > len(items) {
			start = len(items)
		}

		iter := SliceIterator(items, offset)
		defer func() {
			if err := iter.Close(); err != nil {
				t.Errorf("SliceIterator(%v, %d).Close() error = %v", items, offset, err)
			}
		}()

		var got []byte
		for iter.Next(context.Background()) {
			item, ok := iter.Item().(byte)
			if !ok {
				t.Fatalf("SliceIterator(%v, %d).Item() = %T, want byte", items, offset, iter.Item())
			}
			got = append(got, item)
		}
		if err := iter.Err(); err != nil {
			t.Fatalf("SliceIterator(%v, %d).Err() error = %v", items, offset, err)
		}

		want := items[start:]
		if string(got) != string(want) {
			t.Errorf("SliceIterator(%v, %d) collected %v, want %v", items, offset, got, want)
		}
	})
}

func FuzzRangeIterator(f *testing.F) {
	f.Add(int64(0), int64(10), uint8(10))
	f.Add(int64(10), int64(-2), uint8(5))
	f.Add(int64(3), int64(0), uint8(4))

	f.Fuzz(func(t *testing.T, rawStart int64, rawStep int64, count uint8) {
		start := rawStart % 1_000
		step := rawStep % 25
		if step == 0 {
			step = 1
		}
		wantCount := int(count % 100)
		end := start + step*int64(wantCount)

		iter := RangeIterator(start, end, step)
		defer func() {
			if err := iter.Close(); err != nil {
				t.Errorf("RangeIterator(%d, %d, %d).Close() error = %v", start, end, step, err)
			}
		}()

		var got []int64
		for iter.Next(context.Background()) {
			item, ok := iter.Item().(int64)
			if !ok {
				t.Fatalf("RangeIterator(%d, %d, %d).Item() = %T, want int64", start, end, step, iter.Item())
			}
			got = append(got, item)

			cursor, err := CursorTo[int64](iter.Cursor())
			if err != nil {
				t.Fatalf("CursorTo[int64](RangeIterator(%d, %d, %d).Cursor()) error = %v", start, end, step, err)
			}
			if cursor != item+step {
				t.Errorf("RangeIterator(%d, %d, %d).Cursor() = %d, want %d", start, end, step, cursor, item+step)
			}
		}
		if err := iter.Err(); err != nil {
			t.Fatalf("RangeIterator(%d, %d, %d).Err() error = %v", start, end, step, err)
		}

		if len(got) != wantCount {
			t.Fatalf("RangeIterator(%d, %d, %d) produced %d items, want %d", start, end, step, len(got), wantCount)
		}
		for i, item := range got {
			want := start + int64(i)*step
			if item != want {
				t.Errorf("RangeIterator(%d, %d, %d) item %d = %d, want %d", start, end, step, i, item, want)
			}
		}
	})
}
