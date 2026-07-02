package ecspresso

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/goccy/go-yaml"
)

// BatchInit creates configuration files from an existing AWS Batch
// job definition.
func (d *App) BatchInit(ctx context.Context, opt InitOption) error {
	conf := d.config
	d.LogJSON(opt)
	d.LogJSON(conf)
	if opt.Jsonnet {
		if ext := filepath.Ext(conf.JobDefinitionPath); ext == jsonExt {
			conf.JobDefinitionPath = strings.TrimSuffix(conf.JobDefinitionPath, ext) + jsonnetExt
		}
		if ext := filepath.Ext(conf.path); ext == ymlExt || ext == yamlExt {
			conf.path = strings.TrimSuffix(conf.path, ext) + jsonnetExt
		}
	}

	name := opt.JobDefinition
	var jd *JobDefinition
	var err error
	if strings.Contains(arnToName(name), ":") {
		jd, err = d.DescribeJobDefinition(ctx, name)
	} else {
		jd, err = d.findLatestJobDefinition(ctx, name)
	}
	if err != nil {
		return err
	}

	in := jobDefinitionToInput(jd)
	if opt.Sort {
		sortJobDefinition(in)
	}
	{
		b, err := MarshalJSONForAPI(in)
		if err != nil {
			return fmt.Errorf("unable to marshal job definition to JSON: %w", err)
		}
		if opt.Jsonnet {
			out, err := toJsonnetString(string(b), conf.JobDefinitionPath)
			if err != nil {
				return fmt.Errorf("unable to format job definition as Jsonnet: %w", err)
			}
			b = []byte(out)
		}
		d.LogInfo("saving job definition", "job_definition", jd.Name(), "path", conf.JobDefinitionPath)
		if err := d.saveFile(conf.JobDefinitionPath, b, CreateFileMode, opt.ForceOverwrite); err != nil {
			return err
		}
	}

	// write configuration file
	d.LogInfo("initializing configuration file", "path", conf.path)
	conf.Service = ""
	conf.TaskDefinitionPath = ""
	conf.ServiceDefinitionPath = ""
	conf.ExpressDefinitionPath = ""
	{
		var b []byte
		var err error
		if opt.Jsonnet {
			b, err = json.MarshalIndent(conf, "", "  ")
			if err != nil {
				return fmt.Errorf("unable to marshal config to JSON: %w", err)
			}
			out, err := toJsonnetString(string(b), conf.path)
			if err != nil {
				return fmt.Errorf("unable to format config as Jsonnet: %w", err)
			}
			b = []byte(out)
		} else {
			b, err = yaml.Marshal(conf)
			if err != nil {
				return fmt.Errorf("unable to marshal config to YAML: %w", err)
			}
		}
		d.LogInfo("saving config", "path", conf.path)
		if err := d.saveFile(conf.path, b, CreateFileMode, opt.ForceOverwrite); err != nil {
			return err
		}
	}
	return nil
}
