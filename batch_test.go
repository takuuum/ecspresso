package ecspresso_test

import (
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	batchTypes "github.com/aws/aws-sdk-go-v2/service/batch/types"
	"github.com/google/go-cmp/cmp"
	"github.com/kayac/ecspresso/v2"
)

func TestLoadJobDefinition(t *testing.T) {
	ctx := t.Context()
	app, err := ecspresso.New(ctx, &ecspresso.CLIOptions{ConfigFilePath: "tests/batch.yaml"})
	if err != nil {
		t.Fatal(err)
	}
	c := app.Config()
	jd, err := app.LoadJobDefinition(c.JobDefinitionPath)
	if err != nil {
		t.Fatalf("%s load failed: %s", c.JobDefinitionPath, err)
	}
	if jd.Name() != "test" {
		t.Errorf("unexpected job definition name: %s", jd.Name())
	}
	if jd.Type != batchTypes.JobDefinitionTypeContainer {
		t.Errorf("unexpected type: %s", jd.Type)
	}
	// map keys must not be case-rewritten
	if v := jd.Parameters["inputFile"]; v != "default.txt" {
		t.Errorf("unexpected parameters: %#v", jd.Parameters)
	}
	if v := jd.Tags["env"]; v != "test" {
		t.Errorf("unexpected tags: %#v", jd.Tags)
	}
	cp := jd.ContainerProperties
	if cp == nil {
		t.Fatal("containerProperties is nil")
	}
	if aws.ToString(cp.Image) != "busybox:latest" {
		t.Errorf("unexpected image: %s", aws.ToString(cp.Image))
	}
	if aws.ToString(cp.Environment[0].Name) != "FOO" || aws.ToString(cp.Environment[0].Value) != "bar" {
		t.Errorf("unexpected environment: %#v", cp.Environment)
	}
	if got := ecspresso.BatchLogGroupOf(jd); got != "/custom/group" {
		t.Errorf("unexpected log group: %s", got)
	}

	// map keys must not be case-rewritten on marshaling for API too
	b, err := ecspresso.MarshalJSONForAPI(jd)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"inputFile"`) {
		t.Errorf("parameters key is rewritten: %s", string(b))
	}
	if !strings.Contains(string(b), `"awslogs-group"`) {
		t.Errorf("logConfiguration options key is rewritten: %s", string(b))
	}
}

func TestLoadJobDefinitionWrapped(t *testing.T) {
	ctx := t.Context()
	app, err := ecspresso.New(ctx, &ecspresso.CLIOptions{ConfigFilePath: "tests/batch.yaml"})
	if err != nil {
		t.Fatal(err)
	}
	jd, err := app.LoadJobDefinition("tests/batch-job-def-wrapped.json")
	if err != nil {
		t.Fatal(err)
	}
	if jd.Name() != "wrapped" {
		t.Errorf("unexpected job definition name: %s", jd.Name())
	}
	if got := ecspresso.BatchLogGroupOf(jd); got != "/aws/batch/job" {
		t.Errorf("unexpected log group: %s", got)
	}
}

func TestBatchModeConfigConflict(t *testing.T) {
	ctx := t.Context()
	loader := ecspresso.NewConfigLoader(nil, nil)
	_, err := loader.Load(ctx, "tests/batch_conflict.yaml", "")
	if err == nil {
		t.Fatal("expected an error to occur, but it didn't")
	}
	if !strings.Contains(err.Error(), "job_definition can not be used with") {
		t.Errorf("unexpected error: %s", err)
	}
}

func TestDiffJobDefs(t *testing.T) {
	ctx := t.Context()
	app, err := ecspresso.New(ctx, &ecspresso.CLIOptions{ConfigFilePath: "tests/batch.yaml"})
	if err != nil {
		t.Fatal(err)
	}
	c := app.Config()
	local, err := app.LoadJobDefinition(c.JobDefinitionPath)
	if err != nil {
		t.Fatal(err)
	}
	remote, err := app.LoadJobDefinition(c.JobDefinitionPath)
	if err != nil {
		t.Fatal(err)
	}

	opt := &ecspresso.DiffOption{Unified: true}
	opt.SetWriter(io.Discard)
	changed, err := ecspresso.DiffJobDefs(ctx, local, remote, "local.json", "remote-arn", opt)
	if err != nil {
		t.Fatal(err)
	}
	if changed {
		t.Error("identical job definitions must not be changed")
	}

	remote.ContainerProperties.Image = aws.String("busybox:stable")
	changed, err = ecspresso.DiffJobDefs(ctx, local, remote, "local.json", "remote-arn", opt)
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Error("different job definitions must be changed")
	}
}

func TestSortJobDefinition(t *testing.T) {
	jd := &ecspresso.JobDefinitionInput{
		JobDefinitionName: aws.String("test"),
		ContainerProperties: &batchTypes.ContainerProperties{
			Environment: []batchTypes.KeyValuePair{
				{Name: aws.String("B"), Value: aws.String("2")},
				{Name: aws.String("A"), Value: aws.String("1")},
			},
			ResourceRequirements: []batchTypes.ResourceRequirement{
				{Type: batchTypes.ResourceTypeVcpu, Value: aws.String("1")},
				{Type: batchTypes.ResourceTypeGpu, Value: aws.String("1")},
			},
		},
		PlatformCapabilities: []batchTypes.PlatformCapability{
			batchTypes.PlatformCapabilityFargate,
			batchTypes.PlatformCapabilityEc2,
		},
	}
	ecspresso.SortJobDefinition(jd)
	if aws.ToString(jd.ContainerProperties.Environment[0].Name) != "A" {
		t.Errorf("environment is not sorted: %#v", jd.ContainerProperties.Environment)
	}
	if jd.ContainerProperties.ResourceRequirements[0].Type != batchTypes.ResourceTypeGpu {
		t.Errorf("resourceRequirements is not sorted: %#v", jd.ContainerProperties.ResourceRequirements)
	}
	if jd.PlatformCapabilities[0] != batchTypes.PlatformCapabilityEc2 {
		t.Errorf("platformCapabilities is not sorted: %#v", jd.PlatformCapabilities)
	}
}

func TestJobDefinitionToInput(t *testing.T) {
	jd := &ecspresso.JobDefinition{
		JobDefinitionArn:  aws.String("arn:aws:batch:ap-northeast-1:123456789012:job-definition/test:3"),
		JobDefinitionName: aws.String("test"),
		Revision:          aws.Int32(3),
		Status:            aws.String("ACTIVE"),
		Type:              aws.String("container"),
		ContainerProperties: &batchTypes.ContainerProperties{
			Image:   aws.String("busybox:latest"),
			Command: []string{"true"},
		},
		Parameters:         map[string]string{"inputFile": "default.txt"},
		SchedulingPriority: aws.Int32(10),
		Tags:               map[string]string{"env": "test"},
	}
	if jd.Name() != "test:3" {
		t.Errorf("unexpected job definition name: %s", jd.Name())
	}
	in := ecspresso.JobDefinitionToInput(jd)
	want := &ecspresso.JobDefinitionInput{
		JobDefinitionName: aws.String("test"),
		Type:              batchTypes.JobDefinitionTypeContainer,
		ContainerProperties: &batchTypes.ContainerProperties{
			Image:   aws.String("busybox:latest"),
			Command: []string{"true"},
		},
		Parameters:         map[string]string{"inputFile": "default.txt"},
		SchedulingPriority: aws.Int32(10),
		Tags:               map[string]string{"env": "test"},
	}
	// compare via JSON to avoid unexported fields in SDK types
	gotJSON, err := ecspresso.MarshalJSONForAPI(in)
	if err != nil {
		t.Fatal(err)
	}
	wantJSON, err := ecspresso.MarshalJSONForAPI(want)
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(string(wantJSON), string(gotJSON)); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}
}

func TestParseTagsMap(t *testing.T) {
	m, err := ecspresso.ParseTagsMap("Env=dev,Team=sre")
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"Env": "dev", "Team": "sre"}
	if diff := cmp.Diff(want, m); diff != "" {
		t.Errorf("mismatch (-want +got):\n%s", diff)
	}

	m, err = ecspresso.ParseTagsMap("")
	if err != nil {
		t.Fatal(err)
	}
	if m != nil {
		t.Errorf("empty tags must be nil: %#v", m)
	}

	if _, err := ecspresso.ParseTagsMap("invalid"); err == nil {
		t.Error("expected an error to occur, but it didn't")
	}
}
