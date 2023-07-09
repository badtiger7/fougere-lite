// ©Copyright 2022 Metrio
package cloudstorage

import (
	"fmt"
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/spf13/viper"
)

type Config struct {
	StorageBuckets map[string]StorageBucket `mapstructure:"storageBucket" validate:"dive"`
}

// StorageBucket contains the information required to create a Cloud Storage in gcp.
// A storage bucket is used to store all kinds of objects.
type StorageBucket struct {
	Name       string `json:"name" validate:"required"`
	Region     string `json:"region" validate:"required"`
	ProjectId  string `json:"projectId" validate:"required"`
	ClientName string
}

func GetStorageConfig(viperConfig *viper.Viper, clientName string) (*Config, error) {
	if viperConfig == nil {
		return nil, nil
	}

	var storageConfig Config
	err := viperConfig.Unmarshal(&storageConfig)
	if err != nil {
		return nil, err
	}

	for name, bucket := range storageConfig.StorageBuckets {
		bucket.Name = strings.Join([]string{clientName, name, bucket.ProjectId}, "-")
		if err != nil {
			return nil, err
		}
		bucket.ClientName = clientName

		storageConfig.StorageBuckets[name] = bucket
	}
	return &storageConfig, nil
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
