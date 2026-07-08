package ecspresso

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/Songmu/prompter"
	"github.com/aws/aws-sdk-go-v2/aws"
)

func (d *App) BatchRegister(ctx context.Context, opt RegisterOption) error {
	ctx, cancel := d.Start(ctx)
	defer cancel()

	d.LogInfo("Starting register job definition", withDryRun(opt.DryRun)...)
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

	if opt.Output {
		_, err := OutputJSONForAPI(os.Stdout, newJd)
		return err
	}
	return nil
}

func (d *App) BatchDeregister(ctx context.Context, opt DeregisterOption) error {
	ctx, cancel := d.Start(ctx)
	defer cancel()
	d.LogInfo("Starting deregister job definition", withDryRun(opt.DryRun)...)

	if opt.Delete {
		// Batch has no DeleteJobDefinition API. INACTIVE revisions are
		// removed by AWS automatically after retention period.
		return fmt.Errorf("--delete is not supported in batch mode")
	}

	if opt.Revision != "" {
		return d.batchDeregisterRevision(ctx, opt)
	} else if opt.Keeps != nil && *opt.Keeps > 0 {
		return d.batchDeregisterKeeps(ctx, opt)
	}
	return fmt.Errorf("--revision or --keeps required")
}

func (d *App) batchDeregisterRevision(ctx context.Context, opt DeregisterOption) error {
	jd, err := d.LoadJobDefinition(d.config.JobDefinitionPath)
	if err != nil {
		return err
	}
	var name string
	switch opt.Revision {
	case "latest":
		latest, err := d.findLatestJobDefinition(ctx, jd.Name())
		if err != nil {
			return err
		}
		name = latest.Name()
	default:
		rv, err := strconv.ParseInt(opt.Revision, 10, 32)
		if err != nil {
			return fmt.Errorf("invalid revision number: %w", err)
		}
		name = fmt.Sprintf("%s:%d", jd.Name(), rv)
	}

	if opt.DryRun {
		d.LogInfo("job definition will be deregistered", "job_definition", name)
		d.LogInfo("DRY RUN OK")
		return nil
	}
	confirmed := opt.Force || prompter.YesNo(fmt.Sprintf("Deregister %s ?", name), false)
	if !confirmed {
		d.LogInfo("Aborted")
		return fmt.Errorf("confirmation failed")
	}
	return d.DeregisterJobDefinition(ctx, name)
}

func (d *App) batchDeregisterKeeps(ctx context.Context, opt DeregisterOption) error {
	keeps := aws.ToInt(opt.Keeps)
	jd, err := d.LoadJobDefinition(d.config.JobDefinitionPath)
	if err != nil {
		return err
	}
	jds, err := d.listJobDefinitions(ctx, jd.Name(), true)
	if err != nil {
		return err
	}
	if len(jds) <= keeps {
		d.LogInfo("No need to deregister job definitions")
		return nil
	}
	// jds is sorted by revision descending: keep the newest `keeps`
	// revisions and deregister the rest.
	deregs := []string{}
	for _, jd := range jds[keeps:] {
		name := jd.Name()
		d.LogInfo("job definition will be deregistered", "job_definition", name)
		deregs = append(deregs, name)
	}
	if opt.DryRun {
		d.LogInfo("DRY RUN OK")
		return nil
	}

	confirmed := opt.Force || prompter.YesNo(fmt.Sprintf("Deregister %d revisons?", len(deregs)), false)
	if !confirmed {
		d.LogInfo("Aborted")
		return fmt.Errorf("confirmation failed")
	}
	deregistered := 0
	for _, name := range deregs {
		if err := d.DeregisterJobDefinition(ctx, name); err != nil {
			return err
		}
		sleepContext(ctx, time.Second)
		deregistered++
	}
	d.LogInfo("job definitions deregistered", "count", deregistered)
	return nil
}
