package senna

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// IterableHandler processes jobs that iterate over large datasets.
// Jobs can be interrupted (shutdown, max items) and resumed from the last cursor.
type IterableHandler interface {
	// BuildIterator creates an iterator for the job.
	// cursor is nil on first run, or the last saved cursor on resume.
	BuildIterator(ctx context.Context, job *Job, cursor Cursor) (Iterator, error)

	// ProcessItem handles a single item from the iterator.
	// Return nil to continue, SkipItemError to skip, StopIterationError to complete early.
	ProcessItem(ctx context.Context, job *Job, item any) error
}

// Iterator provides items one at a time with cursor tracking.
// Follows the pull-based pattern like database/sql.Rows.
type Iterator interface {
	// Next advances to the next item. Returns false when exhausted or on error.
	Next(ctx context.Context) bool

	// Item returns the current item. Only valid after Next returns true.
	Item() any

	// Cursor returns the current position for resumption.
	// Called after each successful ProcessItem to save progress.
	Cursor() Cursor

	// Err returns any error that occurred during iteration.
	Err() error

	// Close releases any resources held by the iterator.
	Close() error
}

// Cursor is a JSON-serializable position marker for resumable iteration.
// Stored as json.RawMessage to avoid type drift during deserialization.
type Cursor = json.RawMessage

// CursorFrom marshals any value into a Cursor.
// Returns nil if marshaling fails.
func CursorFrom(v any) Cursor {
	if v == nil {
		return nil
	}
	data, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	return data
}

// CursorTo unmarshals a Cursor into the specified type.
// Returns the zero value and an error if unmarshaling fails.
func CursorTo[T any](c Cursor) (T, error) {
	var result T
	if c == nil {
		return result, nil
	}
	err := json.Unmarshal(c, &result)
	return result, err
}

// IterationState is stored in Redis for resumable iteration.
// Key: {namespace}:iteration:{job_id}
type IterationState struct {
	JobID      string          `json:"jid"`
	Cursor     json.RawMessage `json:"cursor,omitempty"`
	Executions int             `json:"executions"` // Incremented each run
	TotalItems int64           `json:"total_items"`
	TotalTime  time.Duration   `json:"total_time_ns"`
	StartedAt  time.Time       `json:"started_at"`
	LastRunAt  time.Time       `json:"last_run_at"`
	Cancelled  bool            `json:"cancelled,omitempty"`
}

// IterableCallbacks provides lifecycle hooks for iterable jobs.
type IterableCallbacks struct {
	// OnStart is called when the job starts for the first time (Executions == 1).
	OnStart func(ctx context.Context, job *Job) error

	// OnResume is called when the job resumes from a saved cursor (Executions > 1).
	OnResume func(ctx context.Context, job *Job, state *IterationState) error

	// OnStop is called when the job is interrupted (shutdown or max items).
	// Also called after OnCancel.
	OnStop func(ctx context.Context, job *Job, state *IterationState) error

	// OnComplete is called when iteration finishes successfully.
	// Not called if job is cancelled.
	OnComplete func(ctx context.Context, job *Job, state *IterationState) error

	// OnCancel is called when the job is explicitly cancelled.
	// OnStop is called after OnCancel.
	OnCancel func(ctx context.Context, job *Job, state *IterationState) error
}

// SkipItemError indicates the current item should be skipped and iteration should continue.
type SkipItemError struct {
	Reason string
}

// Error implements error.
func (e *SkipItemError) Error() string {
	return fmt.Sprintf("skipping item: %s", e.Reason)
}

// StopIterationError indicates iteration should stop successfully.
// Use this when you've found what you're looking for or want to complete early.
type StopIterationError struct {
	Reason string
}

// Error implements error.
func (e *StopIterationError) Error() string {
	return fmt.Sprintf("stopping iteration: %s", e.Reason)
}

// InterruptedError signals the job should be requeued with the same ID.
// Does not increment retry count or trigger batch failure callbacks.
type InterruptedError struct{}

// Error implements error.
func (e *InterruptedError) Error() string {
	return "job interrupted"
}

// Interrupted returns true if the context is done (worker shutting down).
// Use this in ProcessItem for long operations that should checkpoint early.
func Interrupted(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

// IterableFunc creates an IterableHandler from functions for simpler cases.
func IterableFunc(
	build func(ctx context.Context, job *Job, cursor Cursor) (Iterator, error),
	process func(ctx context.Context, job *Job, item any) error,
) IterableHandler {
	return &iterableFunc{build: build, process: process}
}

type iterableFunc struct {
	build   func(ctx context.Context, job *Job, cursor Cursor) (Iterator, error)
	process func(ctx context.Context, job *Job, item any) error
}

func (f *iterableFunc) BuildIterator(ctx context.Context, job *Job, cursor Cursor) (Iterator, error) {
	return f.build(ctx, job, cursor)
}

func (f *iterableFunc) ProcessItem(ctx context.Context, job *Job, item any) error {
	return f.process(ctx, job, item)
}

// SliceIterator creates an iterator over a slice starting at the given offset.
func SliceIterator[T any](items []T, offset int) Iterator {
	return &sliceIterator[T]{
		items: items,
		index: offset - 1, // -1 because Next() increments before returning
	}
}

type sliceIterator[T any] struct {
	items []T
	index int
}

func (it *sliceIterator[T]) Next(ctx context.Context) bool {
	it.index++
	return it.index < len(it.items)
}

func (it *sliceIterator[T]) Item() any {
	return it.items[it.index]
}

func (it *sliceIterator[T]) Cursor() Cursor {
	return CursorFrom(it.index + 1) // Next position to resume from
}

func (it *sliceIterator[T]) Err() error {
	return nil
}

func (it *sliceIterator[T]) Close() error {
	return nil
}

// RangeIterator creates an iterator over a numeric range [start, end) with the given step.
func RangeIterator(start, end, step int64) Iterator {
	if step == 0 {
		step = 1
	}
	return &rangeIterator{
		current: start - step, // -step because Next() increments before returning
		end:     end,
		step:    step,
	}
}

type rangeIterator struct {
	current int64
	end     int64
	step    int64
}

func (it *rangeIterator) Next(ctx context.Context) bool {
	it.current += it.step
	if it.step > 0 {
		return it.current < it.end
	}
	return it.current > it.end
}

func (it *rangeIterator) Item() any {
	return it.current
}

func (it *rangeIterator) Cursor() Cursor {
	return CursorFrom(it.current + it.step) // Next value to resume from
}

func (it *rangeIterator) Err() error {
	return nil
}

func (it *rangeIterator) Close() error {
	return nil
}
