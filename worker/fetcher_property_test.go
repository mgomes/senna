package worker

import (
	"testing"

	"pgregory.net/rapid"
)

func TestSelectWeightedQueueProperties(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		queues := rapid.SliceOfNDistinct(queueInfoGenerator(), 0, 20, func(q queueInfo) string {
			return q.name
		}).Draw(t, "queues")
		totalWeight := totalQueueWeight(queues)

		got, ok := selectWeightedQueue(queues, totalWeight)
		if len(queues) == 0 {
			if ok {
				t.Fatalf("selectWeightedQueue(%v, %d) ok = true, want false", queues, totalWeight)
			}
			return
		}
		if !ok {
			t.Fatalf("selectWeightedQueue(%v, %d) ok = false, want true", queues, totalWeight)
		}
		if !queueIn(got, queues) {
			t.Fatalf("selectWeightedQueue(%v, %d) = %v, want member of input queues", queues, totalWeight, got)
		}
	})
}

func TestSelectWeightedQueueSkippingProperties(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		queues := rapid.SliceOfNDistinct(queueInfoGenerator(), 0, 20, func(q queueInfo) string {
			return q.name
		}).Draw(t, "queues")
		skipped := drawSkippedQueueNames(t, queues)

		got, ok := selectWeightedQueueSkipping(queues, skipped)
		if unskippedQueueCount(queues, skipped) == 0 {
			if ok {
				t.Fatalf("selectWeightedQueueSkipping(%v, %v) ok = true, want false", queues, skipped)
			}
			return
		}
		if !ok {
			t.Fatalf("selectWeightedQueueSkipping(%v, %v) ok = false, want true", queues, skipped)
		}
		if !queueIn(got, queues) {
			t.Fatalf("selectWeightedQueueSkipping(%v, %v) = %v, want member of input queues", queues, skipped, got)
		}
		if queueNameSkipped(got.name, skipped) {
			t.Fatalf("selectWeightedQueueSkipping(%v, %v) = %v, want unskipped queue", queues, skipped, got)
		}
	})
}

func queueInfoGenerator() *rapid.Generator[queueInfo] {
	return rapid.Custom(func(t *rapid.T) queueInfo {
		return queueInfo{
			name:     rapid.StringMatching(`[a-z][a-z0-9_-]{0,12}`).Draw(t, "name"),
			priority: rapid.IntRange(0, 20).Draw(t, "priority"),
		}
	})
}

func drawSkippedQueueNames(t *rapid.T, queues []queueInfo) []string {
	t.Helper()

	var skipped []string
	for i, queue := range queues {
		if rapid.Bool().Draw(t, "skip_"+queue.name) {
			skipped = append(skipped, queue.name)
		}
		if rapid.Bool().Draw(t, "duplicate_skip_"+queue.name) {
			skipped = append(skipped, queue.name)
		}
		if rapid.Bool().Draw(t, "unknown_skip_after_"+queue.name) {
			skipped = append(skipped, rapid.StringMatching(`[a-z][a-z0-9_-]{0,12}`).Draw(t, "unknown_skip_name_"+queue.name))
		}
		if i >= 5 && len(skipped) > len(queues) {
			break
		}
	}
	return skipped
}

func totalQueueWeight(queues []queueInfo) int {
	var total int
	for _, queue := range queues {
		total += queue.priority
	}
	return total
}

func queueIn(queue queueInfo, queues []queueInfo) bool {
	for _, candidate := range queues {
		if candidate.name == queue.name {
			return true
		}
	}
	return false
}

func unskippedQueueCount(queues []queueInfo, skipped []string) int {
	var count int
	for _, queue := range queues {
		if !queueNameSkipped(queue.name, skipped) {
			count++
		}
	}
	return count
}
