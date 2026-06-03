package periodic

import (
	"testing"

	"github.com/mgomes/senna"
	"github.com/mgomes/senna/internal/keys"
)

func FuzzSchedulerRegister(f *testing.F) {
	f.Add("* * * * *", "sync_user", "default", 25)
	f.Add("0 9 * * 1", "weekly_report", "critical", 3)
	f.Add("not a schedule", "", "", 0)

	f.Fuzz(func(t *testing.T, schedule string, jobType string, queue string, retry int) {
		if len(schedule) > 128 || len(jobType) > 512 || len(queue) > 512 {
			t.Skip()
		}

		scheduler := NewScheduler(nil, keys.New("test"))
		err := scheduler.Register(
			schedule,
			jobType,
			WithQueue(queue),
			WithRetry(retry),
			WithArgs(map[string]any{"job_type": jobType}),
		)
		jobs := scheduler.Jobs()

		if err != nil {
			if len(jobs) != 0 {
				t.Fatalf("Scheduler.Register(%q, %q) error = %v and stored %d jobs, want 0", schedule, jobType, err, len(jobs))
			}
			return
		}

		if len(jobs) != 1 {
			t.Fatalf("Scheduler.Register(%q, %q) stored %d jobs, want 1", schedule, jobType, len(jobs))
		}
		job := jobs[0]
		if job.Name != jobType {
			t.Errorf("Scheduler.Register(%q, %q).Name = %q, want %q", schedule, jobType, job.Name, jobType)
		}
		if job.Schedule != schedule {
			t.Errorf("Scheduler.Register(%q, %q).Schedule = %q, want %q", schedule, jobType, job.Schedule, schedule)
		}
		if job.JobType != jobType {
			t.Errorf("Scheduler.Register(%q, %q).JobType = %q, want %q", schedule, jobType, job.JobType, jobType)
		}
		if job.Queue != queue {
			t.Errorf("Scheduler.Register(%q, %q).Queue = %q, want %q", schedule, jobType, job.Queue, queue)
		}
		if job.Retry != retry {
			t.Errorf("Scheduler.Register(%q, %q).Retry = %d, want %d", schedule, jobType, job.Retry, retry)
		}
		if job.Args["job_type"] != jobType {
			t.Errorf("Scheduler.Register(%q, %q).Args[job_type] = %v, want %q", schedule, jobType, job.Args["job_type"], jobType)
		}

		defaultScheduler := NewScheduler(nil, keys.New("test"))
		if err := defaultScheduler.Register(schedule, jobType); err != nil {
			t.Fatalf("Scheduler.Register(%q, %q) without options error = %v after optioned register succeeded", schedule, jobType, err)
		}
		defaultJob := defaultScheduler.Jobs()[0]
		if defaultJob.Queue != senna.DefaultQueueName {
			t.Errorf("Scheduler.Register(%q, %q) default Queue = %q, want %q", schedule, jobType, defaultJob.Queue, senna.DefaultQueueName)
		}
		if defaultJob.Retry != senna.DefaultRetryCount {
			t.Errorf("Scheduler.Register(%q, %q) default Retry = %d, want %d", schedule, jobType, defaultJob.Retry, senna.DefaultRetryCount)
		}
	})
}
