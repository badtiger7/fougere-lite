package cloudstorage

import (
	"context"
	"net/http"

	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	"google.golang.org/api/storage/v1"
	"metrio.net/fougere-lite/internal/common"
	"metrio.net/fougere-lite/internal/utils"
)

type Client struct {
	storageService *storage.Service
}

func NewClient(ctx context.Context, opts ...option.ClientOption) (*Client, error) {
	storageService, err := storage.NewService(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return &Client{
		storageService: storageService,
	}, nil
}

func (c *Client) Create(config *Config) error {
	createChannel := make(chan common.Response, len(config.StorageBuckets))
	for _, bucket := range config.StorageBuckets {
		go func(resp chan common.Response, bucket StorageBucket) {
			_, err := c.get(bucket.Name)
			if err != nil {
				if e, ok := err.(*googleapi.Error); ok && e.Code == http.StatusNotFound {
					utils.Logger.Debug("[%s] bucket not found", bucket.Name)

					if err := c.create(bucket); err != nil {
						resp <- common.Response{Err: err}
						return
					}
				} else {
					utils.Logger.Errorf("[%s] error getting bucket: %s", bucket.Name, err)
					resp <- common.Response{Err: err}
					return
				}
			} else {
				if err := c.update(bucket); err != nil {
					resp <- common.Response{Err: err}
					return
				}
			}
			resp <- common.Response{}
		}(createChannel, bucket)
	}
	for range config.StorageBuckets {
		resp := <-createChannel
		if resp.Err != nil {
			return resp.Err
		}
	}
	return nil
}

func (c *Client) get(name string) (*storage.Bucket, error) {
	utils.Logger.Debug("[%s] getting bucket", name)
	bucket, err := c.storageService.Buckets.Get(name).Do()
	if err != nil {
		return nil, err
	}
	return bucket, nil
}

func (c *Client) create(bucket StorageBucket) error {
	utils.Logger.Infof("[%s] creating bucket", bucket.Name)
	spec := c.createStorageSpec(bucket)
	_, err := c.storageService.Buckets.Insert(bucket.ProjectId, spec).Do()
	if err != nil {
		utils.Logger.Errorf("[%s] error creating bucket: %s", spec.Name, err)
		return err
	}

	return nil
}

func (c *Client) update(bucket StorageBucket) error {
	spec := c.createStorageSpec(bucket)
	utils.Logger.Infof("[%s] updating bucket", spec.Name)
	_, err := c.storageService.Buckets.Update(spec.Name, spec).Do()
	if err != nil {
		utils.Logger.Errorf("[%s] error updating bucket: %s", spec.Name, err)
		return err
	}
	return nil
}

func (c *Client) createStorageSpec(storageBucket StorageBucket) *storage.Bucket {
	return &storage.Bucket{
		Name:         storageBucket.Name,
		StorageClass: "MULTI_REGIONAL",
		Versioning: &storage.BucketVersioning{
			Enabled: false,
		},
	}
}
