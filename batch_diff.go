package ecspresso

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"

	"github.com/aws/aws-sdk-go-v2/aws"
	batchTypes "github.com/aws/aws-sdk-go-v2/service/batch/types"
	"github.com/hexops/gotextdiff"
	"github.com/hexops/gotextdiff/myers"
	"github.com/hexops/gotextdiff/span"
	"github.com/kylelemons/godebug/diff"
)

// BatchDiff shows the diff between the local job definition and the
// latest ACTIVE revision registered in AWS Batch.
func (d *App) BatchDiff(ctx context.Context, opt DiffOption) error {
	ctx, cancel := d.Start(ctx)
	defer cancel()
	if opt.w == nil {
		opt.w = os.Stdout
	}

	newJd, err := d.LoadJobDefinition(d.config.JobDefinitionPath)
	if err != nil {
		return err
	}

	var remoteJd *JobDefinitionInput
	var remoteArn string
	latest, err := d.findLatestJobDefinition(ctx, newJd.Name())
	if err != nil {
		if errors.As(err, &errNotFound) {
			d.LogInfo("job definition not found, will register a new job definition")
		} else {
			return err
		}
	} else {
		remoteArn = aws.ToString(latest.JobDefinitionArn)
		d.LogDebug("diff job definition compare with %s", remoteArn)
		remoteJd = jobDefinitionToInput(latest)
	}

	_, err = diffJobDefs(ctx, newJd, remoteJd, d.config.JobDefinitionPath, remoteArn, &opt)
	return err
}

func diffJobDefs(ctx context.Context, local, remote *JobDefinitionInput, localPath, remoteArn string, opt *DiffOption) (bool, error) {
	sortJobDefinition(local)
	sortJobDefinition(remote)

	newJdBytes, err := MarshalJSONForAPI(local)
	if err != nil {
		return false, fmt.Errorf("failed to marshal new job definition: %w", err)
	}

	remoteJdBytes, err := MarshalJSONForAPI(remote)
	if err != nil {
		return false, fmt.Errorf("failed to marshal remote job definition: %w", err)
	}

	remoteJd := toDiffString(remoteJdBytes)
	newJd := toDiffString(newJdBytes)
	if opt.Jsonnet {
		if remoteJd, err = toJsonnetString(remoteJd, remoteArn); err != nil {
			return false, fmt.Errorf("failed to format remote job definition as jsonnet: %w", err)
		}
		if newJd, err = toJsonnetString(newJd, localPath); err != nil {
			return false, fmt.Errorf("failed to format local job definition as jsonnet: %w", err)
		}
	}
	if remoteJd == newJd {
		return false, nil
	}

	switch {
	case opt.External != "":
		return true, diffExternal(ctx, opt.External, "jobdef", remoteJd, newJd, opt)
	case opt.Unified:
		edits := myers.ComputeEdits(span.URIFromPath(remoteArn), remoteJd, newJd)
		ds := fmt.Sprint(gotextdiff.ToUnified(remoteArn, localPath, remoteJd, edits))
		fmt.Fprint(opt.w, coloredDiff(ds))
		return true, nil
	default:
		ds := diff.Diff(remoteJd, newJd)
		fmt.Fprint(opt.w, coloredDiff(fmt.Sprintf("--- %s\n+++ %s\n%s", remoteArn, localPath, ds)))
		return true, nil
	}
}

// sortJobDefinition sorts slice elements that AWS Batch may return in
// an arbitrary order, to avoid false positives on diff.
func sortJobDefinition(jd *JobDefinitionInput) {
	if jd == nil {
		return
	}
	sortBatchContainerProperties(jd.ContainerProperties)
	if ecsProps := jd.EcsProperties; ecsProps != nil {
		for _, tp := range ecsProps.TaskProperties {
			sort.SliceStable(tp.Containers, func(i, j int) bool {
				return aws.ToString(tp.Containers[i].Name) < aws.ToString(tp.Containers[j].Name)
			})
			for i := range tp.Containers {
				c := &tp.Containers[i]
				sort.SliceStable(c.Environment, func(i, j int) bool {
					return aws.ToString(c.Environment[i].Name) < aws.ToString(c.Environment[j].Name)
				})
				sort.SliceStable(c.Secrets, func(i, j int) bool {
					return aws.ToString(c.Secrets[i].Name) < aws.ToString(c.Secrets[j].Name)
				})
			}
		}
	}
	if nodeProps := jd.NodeProperties; nodeProps != nil {
		for _, nr := range nodeProps.NodeRangeProperties {
			sortBatchContainerProperties(nr.Container)
		}
	}
	sort.SliceStable(jd.PlatformCapabilities, func(i, j int) bool {
		return jd.PlatformCapabilities[i] < jd.PlatformCapabilities[j]
	})
}

func sortBatchContainerProperties(cp *batchTypes.ContainerProperties) {
	if cp == nil {
		return
	}
	sort.SliceStable(cp.Environment, func(i, j int) bool {
		return aws.ToString(cp.Environment[i].Name) < aws.ToString(cp.Environment[j].Name)
	})
	sort.SliceStable(cp.Secrets, func(i, j int) bool {
		return aws.ToString(cp.Secrets[i].Name) < aws.ToString(cp.Secrets[j].Name)
	})
	sort.SliceStable(cp.MountPoints, func(i, j int) bool {
		return jsonStr(cp.MountPoints[i]) < jsonStr(cp.MountPoints[j])
	})
	sort.SliceStable(cp.Ulimits, func(i, j int) bool {
		return aws.ToString(cp.Ulimits[i].Name) < aws.ToString(cp.Ulimits[j].Name)
	})
	sort.SliceStable(cp.ResourceRequirements, func(i, j int) bool {
		return cp.ResourceRequirements[i].Type < cp.ResourceRequirements[j].Type
	})
	sort.SliceStable(cp.Volumes, func(i, j int) bool {
		return jsonStr(cp.Volumes[i]) < jsonStr(cp.Volumes[j])
	})
}
