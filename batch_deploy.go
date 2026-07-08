package ecspresso

import (
	"context"
	"fmt"
	"os"
)

// BatchDeploy registers a new revision of the job definition.
// Batch runs jobs submitted without an explicit revision on the
// latest ACTIVE revision, so registering a new revision is the
// whole deployment.
func (d *App) BatchDeploy(ctx context.Context, opt DeployOption) error {
	ctx, cancel := d.Start(ctx)
	defer cancel()

	d.LogInfo("Starting deploy", withDryRun(opt.DryRun)...)
	if opt.SkipTaskDefinition || opt.LatestTaskDefinition {
		return fmt.Errorf("--skip-task-definition and --latest-task-definition are not supported in batch mode")
	}
	jd, err := d.LoadJobDefinition(d.config.JobDefinitionPath)
	if err != nil {
		return err
	}
	if opt.DryRun {
		d.LogInfo("job definition:")
		if _, err := OutputJSONForAPI(os.Stdout, jd); err != nil {
			return err
		}
		d.LogInfo("DRY RUN OK")
		return nil
	}
	newJd, err := d.RegisterJobDefinition(ctx, jd)
	if err != nil {
		return err
	}
	d.LogInfo("Deploy completed!", "job_definition", newJd.Name())
	return nil
}

// BatchRollback deregisters the latest ACTIVE revision of the job
// definition, so that the previous ACTIVE revision becomes the one
// used for jobs submitted without an explicit revision.
func (d *App) BatchRollback(ctx context.Context, opt RollbackOption) error {
	ctx, cancel := d.Start(ctx)
	defer cancel()

	d.LogInfo("Starting rollback", withDryRun(opt.DryRun)...)
	jd, err := d.LoadJobDefinition(d.config.JobDefinitionPath)
	if err != nil {
		return err
	}
	jds, err := d.listJobDefinitions(ctx, jd.Name(), true)
	if err != nil {
		return err
	}
	if len(jds) < 2 {
		return fmt.Errorf("no active revision to rollback to")
	}
	current, previous := &jds[0], &jds[1]
	d.LogInfo("rolling back", "from", current.Name(), "to", previous.Name())

	if opt.DryRun {
		d.LogInfo("DRY RUN OK")
		return nil
	}
	if err := d.DeregisterJobDefinition(ctx, current.Name()); err != nil {
		return err
	}
	d.LogInfo("Rollback completed!", "job_definition", previous.Name())
	return nil
}
