package ecspresso

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/goccy/go-yaml"
)

type RenderOption struct {
	Targets *[]string `arg:"" help:"target to render (config, service-definition, servicedef, task-definition, taskdef, express-definition, expressdef, job-definition, jobdef)" enum:"config,service-definition,servicedef,task-definition,taskdef,express-definition,expressdef,job-definition,jobdef"`
	Jsonnet bool      `help:"render as jsonnet format" default:"false"`
}

func (d *App) Render(ctx context.Context, opt RenderOption) error {
	out := bufio.NewWriter(os.Stdout)
	defer out.Flush()
	d.LogDebug("targets %v", opt.Targets)
	for _, target := range *opt.Targets {
		switch target {
		case "config":
			if opt.Jsonnet {
				b, err := json.MarshalIndent(d.config, "", "  ")
				if err != nil {
					return fmt.Errorf("unable to marshal config to JSON: %w", err)
				}
				s, err := toJsonnetString(string(b), "")
				if err != nil {
					return fmt.Errorf("unable to format config as Jsonnet: %w", err)
				}
				if _, err := out.WriteString(s); err != nil {
					return err
				}
			} else {
				if err := yaml.NewEncoder(out).Encode(d.config); err != nil {
					return err
				}
			}
		case "service-definition", "servicedef":
			sv, err := d.LoadServiceDefinition(d.config.ServiceDefinitionPath)
			if err != nil {
				return err
			}
			s := MustMarshalJSONStringForAPI(sv)
			if opt.Jsonnet {
				s, err = toJsonnetString(s, d.config.ServiceDefinitionPath)
				if err != nil {
					return fmt.Errorf("unable to format service definition as Jsonnet: %w", err)
				}
			}
			if _, err = out.WriteString(s); err != nil {
				return err
			}
		case "task-definition", "taskdef":
			td, err := d.LoadTaskDefinition(d.config.TaskDefinitionPath)
			if err != nil {
				return err
			}
			s := MustMarshalJSONStringForAPI(td)
			if opt.Jsonnet {
				s, err = toJsonnetString(s, d.config.TaskDefinitionPath)
				if err != nil {
					return fmt.Errorf("unable to format task definition as Jsonnet: %w", err)
				}
			}
			if _, err := out.WriteString(s); err != nil {
				return err
			}
		case "express-definition", "expressdef":
			sv, err := d.LoadExpressDefinition(d.config.ExpressDefinitionPath)
			if err != nil {
				return err
			}
			s := MustMarshalJSONStringForAPI(sv)
			if opt.Jsonnet {
				s, err = toJsonnetString(s, d.config.ExpressDefinitionPath)
				if err != nil {
					return fmt.Errorf("unable to format express gateway service definition as Jsonnet: %w", err)
				}
			}
			if _, err := out.WriteString(s); err != nil {
				return err
			}
		case "job-definition", "jobdef":
			jd, err := d.LoadJobDefinition(d.config.JobDefinitionPath)
			if err != nil {
				return err
			}
			s := MustMarshalJSONStringForAPI(jd)
			if opt.Jsonnet {
				s, err = toJsonnetString(s, d.config.JobDefinitionPath)
				if err != nil {
					return fmt.Errorf("unable to format job definition as Jsonnet: %w", err)
				}
			}
			if _, err := out.WriteString(s); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unknown target: %s", target)
		}
	}
	return nil
}
