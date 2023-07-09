package cloudtasks

import (
	"context"
	"net/http"
	"strings"

	"google.golang.org/api/googleapi"

	"google.golang.org/api/cloudtasks/v2"
	"google.golang.org/api/option"
	"metrio.net/fougere-lite/internal/common"
	"metrio.net/fougere-lite/internal/utils"
)

type Client struct {
	tasksService *cloudtasks.Service
}

func NewClient(ctx context.Context, opts ...option.ClientOption) (*Client, error) {
	tasksService, err := cloudtasks.NewService(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return &Client{
		tasksService: tasksService,
	}, nil
}

func (c *Client) Create(config *Config) error {
	createChannel := make(chan common.Response, len(config.TaskQueues))
	for _, queue := range config.TaskQueues {
		go func(resp chan common.Response, queue TaskQueue) {
			_, err := c.get(strings.Join([]string{"projects", queue.ProjectId, "locations", queue.Region, "queues", queue.Name}, "/"))
			if err != nil {
				if e, ok := err.(*googleapi.Error); ok && e.Code == http.StatusNotFound {
					utils.Logger.Debug("[%s] queue not found", queue.Name)
					if err := c.create(queue); err != nil {
						resp <- common.Response{Err: err}
						return
					}
				} else {
					utils.Logger.Errorf("[%s] error getting queue: %s", queue.Name, err)
					resp <- common.Response{Err: err}
					return
				}
			} else {
				if err := c.update(queue); err != nil {
					resp <- common.Response{Err: err}
					return
				}
			}
			resp <- common.Response{}
		}(createChannel, queue)
	}
	for range config.TaskQueues {
		resp := <-createChannel
		if resp.Err != nil {
			return resp.Err
		}
	}
	return nil
}

func (c *Client) get(name string) (*cloudtasks.Queue, error) {
	utils.Logger.Debug("[%s] getting queue", name)
	queue, err := c.tasksService.Projects.Locations.Queues.Get(name).Do()
	if err != nil {
		return nil, err
	}
	return queue, nil
}

func (c *Client) create(queue TaskQueue) error {
	utils.Logger.Infof("[%s] creating queue", queue.Name)
	spec := c.createQueueSpec(queue)
	_, err := c.tasksService.Projects.Locations.Queues.Create(strings.Join([]string{"projects", queue.ProjectId, "locations", queue.Region}, "/"), spec).Do()
	if err != nil {
		utils.Logger.Errorf("[%s] error creating queue: %s", spec.Name, err)
		return err
	}
	return nil
}

func (c *Client) update(queue TaskQueue) error {
	spec := c.createQueueSpec(queue)
	utils.Logger.Infof("[%s] updating queue", spec.Name)
	_, err := c.tasksService.Projects.Locations.Queues.Patch(spec.Name, spec).Do()
	if err != nil {
		utils.Logger.Errorf("[%s] error updating queue: %s", spec.Name, err)
		return err
	}
	return nil
}

func (c *Client) createQueueSpec(queue TaskQueue) *cloudtasks.Queue {
	name := strings.Join([]string{"projects", queue.ProjectId, "locations", queue.Region, "queues", queue.Name}, "/")
	return &cloudtasks.Queue{
		Name: name,
		RateLimits: &cloudtasks.RateLimits{
			MaxConcurrentDispatches: queue.MaxConcurrentDispatches,
			MaxDispatchesPerSecond:  queue.MaxDispatchesPerSecond,
		},
		RetryConfig: &cloudtasks.RetryConfig{
			MinBackoff: queue.MinBackoff,
			MaxBackoff: queue.MaxBackoff,
		},
	}
}
