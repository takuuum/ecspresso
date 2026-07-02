package ecspresso

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/batch"
	batchTypes "github.com/aws/aws-sdk-go-v2/service/batch/types"
	"github.com/olekukonko/tablewriter"
	"github.com/olekukonko/tablewriter/tw"
)

// jobStatusesForStatus is the order in which recent jobs are listed
// by the status command.
var jobStatusesForStatus = []batchTypes.JobStatus{
	batchTypes.JobStatusSubmitted,
	batchTypes.JobStatusPending,
	batchTypes.JobStatusRunnable,
	batchTypes.JobStatusStarting,
	batchTypes.JobStatusRunning,
	batchTypes.JobStatusSucceeded,
	batchTypes.JobStatusFailed,
}

type DescribeJobStatusOutput struct {
	JobDefinition   string
	LatestRevision  string
	ActiveRevisions int
	JobQueue        *batchTypes.JobQueueDetail
	RecentJobs      []batchTypes.JobSummary
}

func (s *DescribeJobStatusOutput) String() string {
	buf := &strings.Builder{}
	fmt.Fprintln(buf, "JobDefinition:", s.JobDefinition)
	fmt.Fprintln(buf, "LatestActiveRevision:", s.LatestRevision)
	fmt.Fprintln(buf, "ActiveRevisions:", s.ActiveRevisions)
	if q := s.JobQueue; q != nil {
		fmt.Fprintln(buf, "JobQueue:", aws.ToString(q.JobQueueName))
		fmt.Fprintf(buf, "%sState: %s\n", spcIndent, q.State)
		fmt.Fprintf(buf, "%sStatus: %s\n", spcIndent, q.Status)
	}
	if len(s.RecentJobs) > 0 {
		fmt.Fprintln(buf, "Jobs:")
		for _, j := range s.RecentJobs {
			fmt.Fprint(buf, spcIndent+formatJobSummary(j))
		}
	}
	return buf.String()
}

func formatJobSummary(j batchTypes.JobSummary) string {
	buf := &strings.Builder{}
	fmt.Fprintf(buf, "%s %s %s", j.Status, aws.ToString(j.JobId), aws.ToString(j.JobName))
	if jd := aws.ToString(j.JobDefinition); jd != "" {
		fmt.Fprintf(buf, " %s", arnToName(jd))
	}
	if j.CreatedAt != nil {
		fmt.Fprintf(buf, " created:%s", time.UnixMilli(aws.ToInt64(j.CreatedAt)).Local().Format(time.RFC3339))
	}
	if reason := aws.ToString(j.StatusReason); reason != "" {
		fmt.Fprintf(buf, " (%s)", reason)
	}
	fmt.Fprintln(buf)
	return buf.String()
}

// BatchStatus shows the status of the job definition, the job queue
// and recent jobs on the queue.
func (d *App) BatchStatus(ctx context.Context, opt StatusOption) error {
	ctx, cancel := d.Start(ctx)
	defer cancel()

	jd, err := d.LoadJobDefinition(d.config.JobDefinitionPath)
	if err != nil {
		return err
	}
	jds, err := d.listJobDefinitions(ctx, jd.Name(), true)
	if err != nil {
		return err
	}
	out := &DescribeJobStatusOutput{
		JobDefinition:   jd.Name(),
		ActiveRevisions: len(jds),
	}
	if len(jds) > 0 {
		out.LatestRevision = jds[0].Name()
	}

	if d.config.JobQueue != "" {
		q, err := d.describeJobQueue(ctx, d.config.JobQueue)
		if err != nil {
			return err
		}
		out.JobQueue = q
		jobs, err := d.listRecentJobs(ctx, opt.Events)
		if err != nil {
			return err
		}
		out.RecentJobs = jobs
	}

	if _, err := WriteOutput(out); err != nil {
		return fmt.Errorf("failed to write output: %w", err)
	}
	return nil
}

func (d *App) describeJobQueue(ctx context.Context, name string) (*batchTypes.JobQueueDetail, error) {
	out, err := d.batch.DescribeJobQueues(ctx, &batch.DescribeJobQueuesInput{
		JobQueues: []string{name},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to describe job queue %s: %w", name, err)
	}
	if len(out.JobQueues) == 0 {
		return nil, ErrNotFound(fmt.Sprintf("job queue %s is not found", name))
	}
	return &out.JobQueues[0], nil
}

// listRecentJobs lists up to `limit` jobs for each status on the
// configured job queue.
func (d *App) listRecentJobs(ctx context.Context, limit int) ([]batchTypes.JobSummary, error) {
	if limit <= 0 {
		return nil, nil
	}
	jobs := []batchTypes.JobSummary{}
	for _, status := range jobStatusesForStatus {
		res, err := d.batch.ListJobs(ctx, &batch.ListJobsInput{
			JobQueue:   aws.String(d.config.JobQueue),
			JobStatus:  status,
			MaxResults: aws.Int32(int32(limit)),
		})
		if err != nil {
			return nil, fmt.Errorf("failed to list jobs: %w", err)
		}
		jobs = append(jobs, res.JobSummaryList...)
	}
	return jobs, nil
}

type batchRevision struct {
	Name   string `json:"name"`
	Status string `json:"status"`
}

type batchRevisions []batchRevision

func (revs batchRevisions) Header() []string {
	return []string{"Name", "Status"}
}

func (revs batchRevisions) OutputJSON(w io.Writer) error {
	for _, r := range revs {
		b, err := MarshalJSONForAPI(r)
		if err != nil {
			return err
		}
		if _, err := w.Write(b); err != nil {
			return err
		}
	}
	return nil
}

func (revs batchRevisions) OutputTSV(w io.Writer) error {
	for _, r := range revs {
		if _, err := fmt.Fprintln(w, strings.Join([]string{r.Name, r.Status}, "\t")); err != nil {
			return err
		}
	}
	return nil
}

func (revs batchRevisions) OutputTable(w io.Writer) error {
	t := tablewriter.NewTable(w,
		tablewriter.WithRendition(tw.Rendition{
			Symbols: tw.NewSymbols(tw.StyleASCII),
			Borders: tw.Border{Left: tw.On, Top: tw.Off, Right: tw.On, Bottom: tw.Off},
		}),
	)
	t.Header(revs.Header())
	for _, r := range revs {
		t.Append([]string{r.Name, r.Status})
	}
	return t.Render()
}

// BatchRevisions shows revisions of the job definition.
func (d *App) BatchRevisions(ctx context.Context, opt RevisionsOption) error {
	ctx, cancel := d.Start(ctx)
	defer cancel()

	jd, err := d.LoadJobDefinition(d.config.JobDefinitionPath)
	if err != nil {
		return err
	}

	if opt.Revision != "" {
		return d.batchDumpRevision(ctx, jd.Name(), opt.Revision)
	}

	jds, err := d.listJobDefinitions(ctx, jd.Name(), false)
	if err != nil {
		return err
	}
	revs := batchRevisions{}
	for _, jd := range jds {
		revs = append(revs, batchRevision{
			Name:   jd.Name(),
			Status: aws.ToString(jd.Status),
		})
	}
	outputFormat := opt.Output
	if outputFormat == "" && logFormat == logFormatJSON {
		outputFormat = logFormatJSON
	}
	switch outputFormat {
	case "json":
		return revs.OutputJSON(os.Stdout)
	case "table", "":
		return revs.OutputTable(os.Stdout)
	case "tsv":
		return revs.OutputTSV(os.Stdout)
	}
	return nil
}

func (d *App) batchDumpRevision(ctx context.Context, name string, rv string) error {
	var jd *JobDefinition
	var err error
	switch rv {
	case "current", "latest":
		// jobs submitted without a revision run on the latest ACTIVE
		// revision, so current and latest are the same in batch mode
		jd, err = d.findLatestJobDefinition(ctx, name)
	default:
		rint64, perr := strconv.ParseInt(rv, 10, 32)
		if perr != nil {
			return fmt.Errorf("invalid revision: %s", rv)
		}
		jd, err = d.DescribeJobDefinition(ctx, fmt.Sprintf("%s:%d", name, rint64))
	}
	if err != nil {
		return err
	}
	b, err := MarshalJSONForAPI(jd)
	if err != nil {
		return err
	}
	_, err = os.Stdout.Write(b)
	return err
}
