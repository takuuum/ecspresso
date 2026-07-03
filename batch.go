package ecspresso

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/batch"
	batchTypes "github.com/aws/aws-sdk-go-v2/service/batch/types"
)

const jobDefinitionStatusActive = "ACTIVE"

// dispatchBatch routes subcommands to AWS Batch mode handlers.
// The batch mode is enabled by defining job_definition in the
// configuration file. Commands that have no Batch equivalent
// (e.g. scale, exec, tasks) return an error.
func dispatchBatch(ctx context.Context, sub string, app *App, opts *CLIOptions) error {
	switch sub {
	case "deploy":
		return app.BatchDeploy(ctx, *opts.Deploy)
	case "status":
		return app.BatchStatus(ctx, *opts.Status)
	case "rollback":
		return app.BatchRollback(ctx, *opts.Rollback)
	case "run":
		return app.BatchRun(ctx, *opts.Run)
	case "register":
		return app.BatchRegister(ctx, *opts.Register)
	case "deregister":
		return app.BatchDeregister(ctx, *opts.Deregister)
	case "revisions":
		return app.BatchRevisions(ctx, *opts.Revisions)
	case "init":
		return app.BatchInit(ctx, *opts.Init)
	case "diff":
		return app.Diff(ctx, *opts.Diff)
	case "render":
		return app.Render(ctx, *opts.Render)
	default:
		return fmt.Errorf("command %s is not supported in batch mode", sub)
	}
}

// JobDefinitionInput wraps batch.RegisterJobDefinitionInput as
// TaskDefinitionInput wraps ecs.RegisterTaskDefinitionInput.
type JobDefinitionInput batch.RegisterJobDefinitionInput

func (jd *JobDefinitionInput) Name() string {
	return aws.ToString(jd.JobDefinitionName)
}

// JobDefinition wraps batchTypes.JobDefinition as TaskDefinition
// wraps ecs types.TaskDefinition.
type JobDefinition batchTypes.JobDefinition

func (jd *JobDefinition) Name() string {
	return fmt.Sprintf("%s:%d", aws.ToString(jd.JobDefinitionName), aws.ToInt32(jd.Revision))
}

// LoadJobDefinition loads a job definition file. Both a bare
// RegisterJobDefinition input and an object wrapped in a
// "jobDefinition" key (as returned by aws batch
// describe-job-definitions) are accepted.
func (d *App) LoadJobDefinition(path string) (*JobDefinitionInput, error) {
	if path == "" {
		return nil, fmt.Errorf("job_definition is not defined")
	}
	src, err := d.readDefinitionFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to load job definition %s: %w", path, err)
	}
	c := struct {
		JobDefinition json.RawMessage `json:"jobDefinition"`
	}{}
	dec := json.NewDecoder(bytes.NewReader(src))
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("failed to load job definition %s: %w", path, err)
	}
	if c.JobDefinition != nil {
		src = c.JobDefinition
	}
	var jd JobDefinitionInput
	if err := UnmarshalJSONForStruct(src, &jd, path); err != nil {
		return nil, fmt.Errorf("failed to load job definition %s: %w", path, err)
	}
	if jd.JobDefinitionName == nil {
		return nil, fmt.Errorf("jobDefinitionName is not defined in %s", path)
	}
	return &jd, nil
}

func (d *App) RegisterJobDefinition(ctx context.Context, jd *JobDefinitionInput) (*JobDefinition, error) {
	d.LogInfo("Registering a new job definition...")
	if len(jd.Tags) == 0 {
		jd.Tags = nil
	}
	in := batch.RegisterJobDefinitionInput(*jd)
	out, err := d.batch.RegisterJobDefinition(ctx, &in)
	if err != nil {
		return nil, fmt.Errorf("failed to register job definition: %w", err)
	}
	newJd := JobDefinition{
		JobDefinitionArn:  out.JobDefinitionArn,
		JobDefinitionName: out.JobDefinitionName,
		Revision:          out.Revision,
	}
	d.LogInfo("job definition registered", "job_definition", newJd.Name())
	return &newJd, nil
}

func (d *App) DeregisterJobDefinition(ctx context.Context, name string) error {
	d.LogInfo("deregistering", "job_definition", name)
	if _, err := d.batch.DeregisterJobDefinition(ctx, &batch.DeregisterJobDefinitionInput{
		JobDefinition: aws.String(name),
	}); err != nil {
		return fmt.Errorf("failed to deregister job definition: %w", err)
	}
	d.LogInfo("job definition deregistered", "job_definition", name)
	return nil
}

// DescribeJobDefinition describes a single job definition by
// name:revision or ARN.
func (d *App) DescribeJobDefinition(ctx context.Context, name string) (*JobDefinition, error) {
	out, err := d.batch.DescribeJobDefinitions(ctx, &batch.DescribeJobDefinitionsInput{
		JobDefinitions: []string{name},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to describe job definition %s: %w", name, err)
	}
	if len(out.JobDefinitions) == 0 {
		return nil, ErrNotFound(fmt.Sprintf("job definition %s is not found", name))
	}
	jd := JobDefinition(out.JobDefinitions[0])
	return &jd, nil
}

// listJobDefinitions lists all revisions of the named job definition,
// sorted by revision descending (the latest first). When activeOnly is
// true, only ACTIVE revisions are returned.
func (d *App) listJobDefinitions(ctx context.Context, name string, activeOnly bool) ([]JobDefinition, error) {
	in := &batch.DescribeJobDefinitionsInput{
		JobDefinitionName: aws.String(name),
	}
	if activeOnly {
		in.Status = aws.String(jobDefinitionStatusActive)
	}
	jds := []JobDefinition{}
	p := batch.NewDescribeJobDefinitionsPaginator(d.batch, in)
	for p.HasMorePages() {
		res, err := p.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to describe job definitions %s: %w", name, err)
		}
		for _, jd := range res.JobDefinitions {
			jds = append(jds, JobDefinition(jd))
		}
	}
	sort.SliceStable(jds, func(i, j int) bool {
		return aws.ToInt32(jds[i].Revision) > aws.ToInt32(jds[j].Revision)
	})
	return jds, nil
}

// findLatestJobDefinition returns the latest ACTIVE revision of the
// named job definition. This is the revision that Batch uses when a
// job is submitted without an explicit revision.
func (d *App) findLatestJobDefinition(ctx context.Context, name string) (*JobDefinition, error) {
	jds, err := d.listJobDefinitions(ctx, name, true)
	if err != nil {
		return nil, err
	}
	if len(jds) == 0 {
		return nil, ErrNotFound(fmt.Sprintf("no active job definitions %s are found", name))
	}
	return &jds[0], nil
}

// jobDefinitionToInput converts a described job definition to a
// RegisterJobDefinition input, as tdToTaskDefinitionInput does for
// ECS task definitions.
func jobDefinitionToInput(jd *JobDefinition) *JobDefinitionInput {
	in := &JobDefinitionInput{
		JobDefinitionName:            jd.JobDefinitionName,
		Type:                         batchTypes.JobDefinitionType(aws.ToString(jd.Type)),
		ConsumableResourceProperties: jd.ConsumableResourceProperties,
		ContainerProperties:          jd.ContainerProperties,
		EcsProperties:                jd.EcsProperties,
		EksProperties:                jd.EksProperties,
		NodeProperties:               jd.NodeProperties,
		Parameters:                   jd.Parameters,
		PlatformCapabilities:         jd.PlatformCapabilities,
		PropagateTags:                jd.PropagateTags,
		RetryStrategy:                jd.RetryStrategy,
		SchedulingPriority:           jd.SchedulingPriority,
		Timeout:                      jd.Timeout,
	}
	if len(jd.Tags) > 0 {
		in.Tags = jd.Tags
	}
	return in
}
