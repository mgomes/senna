package batch

import (
	"context"
	"maps"
	"time"

	"github.com/redis/go-redis/v9"

	senna "github.com/mgomes/senna"
	"github.com/mgomes/senna/internal/keys"
)

// EnqueueCallback creates and enqueues a batch callback job.
func EnqueueCallback(ctx context.Context, redisClient *redis.Client, k *keys.Keys,
	jobType, batchID, parentID string, options map[string]any, queue string, ttl time.Duration) {

	args := map[string]any{
		"batch_id": batchID,
	}
	if parentID != "" {
		args["parent_id"] = parentID
	}
	maps.Copy(args, options)

	job := senna.NewJob(jobType, args)
	job.Queue = queue
	job.CallbackBatchID = batchID
	data, _ := job.Marshal()

	// Track callback job ID for idempotent completion handling
	redisClient.SAdd(ctx, k.BatchCallbacks(batchID), job.ID)
	redisClient.Expire(ctx, k.BatchCallbacks(batchID), ttl)
	redisClient.LPush(ctx, k.Queue(queue), string(data))
}
