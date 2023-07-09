package cloudtasks

import (
	"fmt"
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/spf13/viper"
)

type Config struct {
	TaskQueues map[string]TaskQueue `mapstructure:"cloudTasks" validate:"dive"`
}

type TaskQueue struct {
	Name                    string  `json:"name" validate:"required"`
	Region                  string  `json:"region" validate:"required"`
	ProjectId               string  `json:"projectId" validate:"required"`
	MinBackoff              string  `json:"minBackoff" validate:"required"`
	MaxBackoff              string  `json:"maxBackoff" validate:"required"`
	MaxConcurrentDispatches int64   `json:"maxConcurrentDispatches" validate:"required"`
	MaxDispatchesPerSecond  float64 `json:"maxDispatchesPerSecond" validate:"required"`
	ClientName              string
}

func GetTasksConfig(viperConfig *viper.Viper, clientName string) (*Config, error) {
	if viperConfig == nil {
		return nil, nil
	}

	var tasksConfig Config
	err := viperConfig.Unmarshal(&tasksConfig)
	if err != nil {
		return nil, err
	}

	for name, queue := range tasksConfig.TaskQueues {
		queue.Name = strings.Join([]string{clientName, name, queue.ProjectId}, "-")
		if err != nil {
			return nil, err
		}
		queue.ClientName = clientName

		tasksConfig.TaskQueues[name] = queue
	}
	return &tasksConfig, nil
}

func ValidateConfig(config *Config) error {
	v := validator.New()
	if err := v.Struct(config); err != nil {
		for _, err := range err.(validator.ValidationErrors) {
			return fmt.Errorf("%s validate failed on the %s rule", err.Namespace(), err.Tag())
		}
	}
	return nil
}
