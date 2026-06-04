package senna

import (
	"context"
	"testing"
)

func TestCursorFrom(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		value any
		want  string
	}{
		{"int", 42, "42"},
		{"string", "hello", `"hello"`},
		{"nil", nil, ""},
		{"map", map[string]int{"a": 1}, `{"a":1}`},
		{"slice", []int{1, 2, 3}, "[1,2,3]"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cursor := CursorFrom(tt.value)
			if tt.value == nil {
				if cursor != nil {
					t.Errorf("CursorFrom(nil) = %v, want nil", cursor)
				}
				return
			}
			if string(cursor) != tt.want {
				t.Errorf("CursorFrom(%v) = %s, want %s", tt.value, cursor, tt.want)
			}
		})
	}
}

func TestCursorTo(t *testing.T) {
	t.Parallel()
	t.Run("int", func(t *testing.T) {
		cursor := CursorFrom(42)
		got, err := CursorTo[int](cursor)
		if err != nil {
			t.Errorf("CursorTo[int] error = %v", err)
		}
		if got != 42 {
			t.Errorf("CursorTo[int] = %d, want 42", got)
		}
	})

	t.Run("string", func(t *testing.T) {
		cursor := CursorFrom("hello")
		got, err := CursorTo[string](cursor)
		if err != nil {
			t.Errorf("CursorTo[string] error = %v", err)
		}
		if got != "hello" {
			t.Errorf("CursorTo[string] = %s, want hello", got)
		}
	})

	t.Run("nil cursor", func(t *testing.T) {
		got, err := CursorTo[int](nil)
		if err != nil {
			t.Errorf("CursorTo[int](nil) error = %v", err)
		}
		if got != 0 {
			t.Errorf("CursorTo[int](nil) = %d, want 0", got)
		}
	})

	t.Run("complex type", func(t *testing.T) {
		type position struct {
			Offset int    `json:"offset"`
			Key    string `json:"key"`
		}
		cursor := CursorFrom(position{Offset: 100, Key: "abc"})
		got, err := CursorTo[position](cursor)
		if err != nil {
			t.Errorf("CursorTo[position] error = %v", err)
		}
		if got.Offset != 100 || got.Key != "abc" {
			t.Errorf("CursorTo[position] = %+v, want {Offset:100 Key:abc}", got)
		}
	})
}

func TestSliceIterator(t *testing.T) {
	t.Parallel()
	items := []string{"a", "b", "c", "d", "e"}
	ctx := context.Background()

	t.Run("iterate all from start", func(t *testing.T) {
		iter := SliceIterator(items, 0)
		defer func() { _ = iter.Close() }()

		var collected []string
		for iter.Next(ctx) {
			collected = append(collected, iter.Item().(string))
		}

		if iter.Err() != nil {
			t.Errorf("iterator error = %v", iter.Err())
		}

		if len(collected) != len(items) {
			t.Errorf("collected %d items, want %d", len(collected), len(items))
		}

		for i, item := range collected {
			if item != items[i] {
				t.Errorf("item[%d] = %s, want %s", i, item, items[i])
			}
		}
	})

	t.Run("resume from offset", func(t *testing.T) {
		iter := SliceIterator(items, 2) // Start from index 2 ("c")
		defer func() { _ = iter.Close() }()

		var collected []string
		for iter.Next(ctx) {
			collected = append(collected, iter.Item().(string))
		}

		expected := []string{"c", "d", "e"}
		if len(collected) != len(expected) {
			t.Errorf("collected %d items, want %d", len(collected), len(expected))
		}

		for i, item := range collected {
			if item != expected[i] {
				t.Errorf("item[%d] = %s, want %s", i, item, expected[i])
			}
		}
	})

	t.Run("cursor tracks position", func(t *testing.T) {
		iter := SliceIterator(items, 0)
		defer func() { _ = iter.Close() }()

		i := 0
		for iter.Next(ctx) {
			cursor := iter.Cursor()
			pos, _ := CursorTo[int](cursor)
			// Cursor should be next position to resume from
			if pos != i+1 {
				t.Errorf("cursor after item %d = %d, want %d", i, pos, i+1)
			}
			i++
		}
	})

	t.Run("empty slice", func(t *testing.T) {
		iter := SliceIterator([]string{}, 0)
		defer func() { _ = iter.Close() }()

		if iter.Next(ctx) {
			t.Error("expected Next() to return false for empty slice")
		}
	})
}

func TestRangeIterator(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("ascending range", func(t *testing.T) {
		iter := RangeIterator(0, 5, 1)
		defer func() { _ = iter.Close() }()

		var collected []int64
		for iter.Next(ctx) {
			collected = append(collected, iter.Item().(int64))
		}

		expected := []int64{0, 1, 2, 3, 4}
		if len(collected) != len(expected) {
			t.Errorf("collected %d items, want %d", len(collected), len(expected))
		}

		for i, val := range collected {
			if val != expected[i] {
				t.Errorf("item[%d] = %d, want %d", i, val, expected[i])
			}
		}
	})

	t.Run("step of 2", func(t *testing.T) {
		iter := RangeIterator(0, 10, 2)
		defer func() { _ = iter.Close() }()

		var collected []int64
		for iter.Next(ctx) {
			collected = append(collected, iter.Item().(int64))
		}

		expected := []int64{0, 2, 4, 6, 8}
		if len(collected) != len(expected) {
			t.Errorf("collected %d items, want %d", len(collected), len(expected))
		}
	})

	t.Run("cursor tracks position", func(t *testing.T) {
		iter := RangeIterator(10, 15, 1)
		defer func() { _ = iter.Close() }()

		expectedCursors := []int64{11, 12, 13, 14, 15}
		i := 0
		for iter.Next(ctx) {
			cursor := iter.Cursor()
			pos, _ := CursorTo[int64](cursor)
			if pos != expectedCursors[i] {
				t.Errorf("cursor at step %d = %d, want %d", i, pos, expectedCursors[i])
			}
			i++
		}
	})

	t.Run("empty range", func(t *testing.T) {
		iter := RangeIterator(5, 5, 1) // start == end
		defer func() { _ = iter.Close() }()

		if iter.Next(ctx) {
			t.Error("expected Next() to return false for empty range")
		}
	})
}

func TestIterableFunc(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	job := NewJob("test_job", nil)

	buildCalled := false
	processCalled := 0

	handler := IterableFunc(
		func(ctx context.Context, job *Job, cursor Cursor) (Iterator, error) {
			buildCalled = true
			offset := 0
			if cursor != nil {
				offset, _ = CursorTo[int](cursor)
			}
			return SliceIterator([]int{1, 2, 3}, offset), nil
		},
		func(ctx context.Context, job *Job, item any) error {
			processCalled++
			return nil
		},
	)

	iter, err := handler.BuildIterator(ctx, job, nil)
	if err != nil {
		t.Fatalf("BuildIterator error = %v", err)
	}
	defer func() { _ = iter.Close() }()

	if !buildCalled {
		t.Error("build function not called")
	}

	for iter.Next(ctx) {
		if err := handler.ProcessItem(ctx, job, iter.Item()); err != nil {
			t.Errorf("ProcessItem error = %v", err)
		}
	}

	if processCalled != 3 {
		t.Errorf("process called %d times, want 3", processCalled)
	}
}

func TestInterrupted(t *testing.T) {
	t.Parallel()
	t.Run("not interrupted", func(t *testing.T) {
		ctx := context.Background()
		if Interrupted(ctx) {
			t.Error("expected Interrupted to return false for active context")
		}
	})

	t.Run("cancelled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		if !Interrupted(ctx) {
			t.Error("expected Interrupted to return true for cancelled context")
		}
	})
}

func TestSkipItemError(t *testing.T) {
	t.Parallel()
	err := &SkipItemError{Reason: "invalid format"}
	expected := "skipping item: invalid format"
	if err.Error() != expected {
		t.Errorf("Error() = %s, want %s", err.Error(), expected)
	}
}

func TestStopIterationError(t *testing.T) {
	t.Parallel()
	err := &StopIterationError{Reason: "found target"}
	expected := "stopping iteration: found target"
	if err.Error() != expected {
		t.Errorf("Error() = %s, want %s", err.Error(), expected)
	}
}

func TestInterruptedError(t *testing.T) {
	t.Parallel()
	err := &InterruptedError{}
	expected := "job interrupted"
	if err.Error() != expected {
		t.Errorf("Error() = %s, want %s", err.Error(), expected)
	}
}
