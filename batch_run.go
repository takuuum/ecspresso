package ecspresso

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/batch"
	batchTypes "github.com/aws/aws-sdk-go-v2/service/batch/types"
)

// defaultBatchLogGroup is where the awslogs driver of AWS Batch sends
// container logs unless awslogs-group is configured explicitly.
const defaultBatchLogGroup = "/aws/batch/job"

var jobPollingInterval = 10 * time.Second

// BatchRun submits a job to the configured job queue and (by default)
// waits until the job finishes, tailing its CloudWatch Logs.
func (d *App) BatchRun(ctx context.Context, opt RunOption) error {
	ctx, cancel := d.Start(ctx)
	defer cancel()

	d.LogInfo("Running job")
	if d.config.JobQueue == "" {
		return fmt.Errorf("job_queue is required in the configuration file to run a job")
	}
	ov := batchTypes.ContainerOverrides{}
	if opt.TaskOverrideStr != "" {
		if err := json.Unmarshal([]byte(opt.TaskOverrideStr), &ov); err != nil {
			return fmt.Errorf("invalid overrides: %w", err)
		}
	} else if ovFile := opt.TaskOverrideFile; ovFile != "" {
		src, err := d.readDefinitionFile(ovFile)
		if err != nil {
			return fmt.Errorf("failed to read overrides-file %s: %w", ovFile, err)
		}
		if err := unmarshalJSON(src, &ov, ovFile); err != nil {
			return fmt.Errorf("failed to read overrides-file %s: %w", ovFile, err)
		}
	}

	jdName, jd, err := d.resolveJobDefinitionForRun(ctx, opt)
	if err != nil {
		return err
	}

	if opt.DryRun {
		d.LogInfo("job definition:")
		d.LogJSON(jd)
		d.LogInfo("job will be submitted", "job_queue", d.config.JobQueue)
		d.LogInfo("DRY RUN OK")
		return nil
	}

	tags, err := parseTagsMap(opt.Tags)
	if err != nil {
		return fmt.Errorf("failed to run job. invalid tags: %w", err)
	}
	in := &batch.SubmitJobInput{
		JobDefinition:      aws.String(jdName),
		JobName:            jd.JobDefinitionName,
		JobQueue:           aws.String(d.config.JobQueue),
		ContainerOverrides: &ov,
		Tags:               tags,
	}
	if opt.PropagateTags != "" && opt.PropagateTags != "NONE" {
		in.PropagateTags = aws.Bool(true)
	}
	if opt.Count > 1 {
		// two or more jobs are submitted as an array job
		in.ArrayProperties = &batchTypes.ArrayProperties{Size: aws.Int32(opt.Count)}
	}
	d.LogInfo("[DEBUG] submit job input")
	d.LogJSON(in)

	out, err := d.batch.SubmitJob(ctx, in)
	if err != nil {
		return fmt.Errorf("failed to submit job: %w", err)
	}
	d.LogInfo("job submitted", "job_id", aws.ToString(out.JobId), "job_arn", aws.ToString(out.JobArn))
	if !opt.Wait {
		d.LogInfo("Run job invoked")
		return nil
	}
	if err := d.WaitJob(ctx, aws.ToString(out.JobId), batchLogGroupOf(jd), time.Now(), opt.waitUntilRunning()); err != nil {
		return err
	}
	d.LogInfo("Run job completed!")
	return nil
}

// resolveJobDefinitionForRun resolves which job definition revision to
// submit, following the same options as the ECS run command.
func (d *App) resolveJobDefinitionForRun(ctx context.Context, opt RunOption) (string, *JobDefinitionInput, error) {
	loadLocal := func() (*JobDefinitionInput, error) {
		jdPath := opt.TaskDefinition
		if jdPath == "" {
			jdPath = d.config.JobDefinitionPath
		}
		return d.LoadJobDefinition(jdPath)
	}
	switch {
	case opt.Revision != nil && *opt.Revision > 0:
		if opt.LatestTaskDefinition {
			return "", nil, ErrConflictOptions("revision and latest-task-definition are exclusive")
		}
		local, err := loadLocal()
		if err != nil {
			return "", nil, err
		}
		name := fmt.Sprintf("%s:%d", local.Name(), *opt.Revision)
		jd, err := d.DescribeJobDefinition(ctx, name)
		if err != nil {
			return "", nil, err
		}
		return name, jobDefinitionToInput(jd), nil
	case opt.LatestTaskDefinition || opt.SkipTaskDefinition:
		local, err := loadLocal()
		if err != nil {
			return "", nil, err
		}
		d.LogInfo("using latest active job definition", "job_definition", local.Name())
		jd, err := d.findLatestJobDefinition(ctx, local.Name())
		if err != nil {
			return "", nil, err
		}
		return jd.Name(), jobDefinitionToInput(jd), nil
	default:
		local, err := loadLocal()
		if err != nil {
			return "", nil, err
		}
		if opt.DryRun {
			return local.Name(), local, nil
		}
		newJd, err := d.RegisterJobDefinition(ctx, local)
		if err != nil {
			return "", nil, err
		}
		return newJd.Name(), local, nil
	}
}

// batchLogGroupOf returns the CloudWatch Logs group that the job's
// container logs to when the awslogs driver is used, or "" when logs
// are not available on CloudWatch Logs.
func batchLogGroupOf(jd *JobDefinitionInput) string {
	cp := jd.ContainerProperties
	if cp == nil {
		return ""
	}
	lc := cp.LogConfiguration
	if lc == nil {
		// Batch uses the awslogs driver by default
		return defaultBatchLogGroup
	}
	if lc.LogDriver != batchTypes.LogDriverAwslogs {
		return ""
	}
	if group := lc.Options["awslogs-group"]; group != "" {
		return group
	}
	return defaultBatchLogGroup
}

// WaitJob polls the job until it reaches RUNNING (when untilRunning)
// or a terminal status, tailing CloudWatch Logs of the job container.
func (d *App) WaitJob(ctx context.Context, jobID string, logGroup string, startedAt time.Time, untilRunning bool) error {
	d.LogInfo("Waiting for job...(it may take a while)", "job_id", jobID)
	var nextToken *string
	var logStream string
	for {
		out, err := d.batch.DescribeJobs(ctx, &batch.DescribeJobsInput{
			Jobs: []string{jobID},
		})
		if err != nil {
			return fmt.Errorf("failed to describe job %s: %w", jobID, err)
		}
		if len(out.Jobs) == 0 {
			return ErrNotFound(fmt.Sprintf("job %s is not found", jobID))
		}
		job := out.Jobs[0]

		if logGroup != "" && logStream == "" && job.Container != nil {
			if ls := aws.ToString(job.Container.LogStreamName); ls != "" {
				logStream = ls
				d.LogInfo("log configuration", "log_group", logGroup, "log_stream", logStream)
			}
		}
		if logStream != "" {
			token, err := d.GetLogEvents(ctx, logGroup, logStream, startedAt, nextToken)
			if err != nil {
				if errors.As(err, &errPermissionDenied) {
					d.LogWarn("failed to get log events: check logs:GetLogEvents permission", "error", err.Error())
					logStream = "" // stop tailing
				} else if !errors.As(err, &errNotFound) {
					d.LogWarn("failed to get log events", "error", err.Error())
				}
			} else {
				nextToken = token
			}
		}

		switch job.Status {
		case batchTypes.JobStatusSucceeded:
			d.LogInfo("job succeeded", "job_id", jobID)
			return nil
		case batchTypes.JobStatusFailed:
			return fmt.Errorf("job failed: %s", jobFailureReason(&job))
		case batchTypes.JobStatusRunning:
			if untilRunning {
				d.LogInfo("job is running", "job_id", jobID)
				return nil
			}
		}
		d.LogDebug("job status: %s", job.Status)
		sleepContext(ctx, jobPollingInterval)
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("failed to wait job: %w", err)
		}
	}
}

func jobFailureReason(job *batchTypes.JobDetail) string {
	reason := aws.ToString(job.StatusReason)
	if c := job.Container; c != nil {
		if c.ExitCode != nil {
			reason += fmt.Sprintf(", exit code: %d", aws.ToInt32(c.ExitCode))
		}
		if r := aws.ToString(c.Reason); r != "" {
			reason += ", container reason: " + r
		}
	}
	return reason
}

func parseTagsMap(s string) (map[string]string, error) {
	tags, err := parseTags(s)
	if err != nil {
		return nil, err
	}
	if len(tags) == 0 {
		return nil, nil
	}
	m := make(map[string]string, len(tags))
	for _, t := range tags {
		m[aws.ToString(t.Key)] = aws.ToString(t.Value)
	}
	return m, nil
}
