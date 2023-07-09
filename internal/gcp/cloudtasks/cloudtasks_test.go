package cloudtasks

import (
	"context"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/api/option"
	"metrio.net/fougere-lite/internal/utils"
)

// Helper method to create client
func getMockedClient(url string) *Client {
	client, err := NewClient(context.Background(), option.WithoutAuthentication(), option.WithEndpoint(url))
	if err != nil {
		Fail(err.Error())
	}
	return client
}

var _ = Describe("Task client", func() {
	var taskQueueConfig TaskQueue

	BeforeEach(func() {
		taskQueueConfig = TaskQueue{
			Name:                    "task-queue-1",
			Region:                  "northamerica-northeast1",
			ProjectId:               "project-123",
			ClientName:              "client1",
			MinBackoff:              "2",
			MaxBackoff:              "10",
			MaxConcurrentDispatches: 1000,
			MaxDispatchesPerSecond:  500.0,
		}
	})
	Describe("create task queue", func() {
		It("successfully creates the task queue", func() {
			mockServerCalls := make(chan utils.MockServerCall, 1)
			mockServerCalls <- utils.MockServerCall{
				UrlMatchFunc: func(url string) bool {
					return strings.HasPrefix(url, "/v2/projects/project-123/locations/northamerica-northeast1/queues?")
				},
				Method: "post",
			}
			mockServer := utils.NewMockServer(mockServerCalls)
			defer mockServer.Close()

			client := getMockedClient(mockServer.URL)

			err := client.create(taskQueueConfig)
			Expect(err).ToNot(HaveOccurred())
		})
	})
	Describe("update task queue", func() {
		It("successfully updates the task queue", func() {
			mockServerCalls := make(chan utils.MockServerCall, 1)
			mockServerCalls <- utils.MockServerCall{
				UrlMatchFunc: func(url string) bool {
					return strings.HasPrefix(url, "/v2/projects/project-123/locations/northamerica-northeast1/queues/task-queue-1?")
				},
				Method: "patch",
			}
			mockServer := utils.NewMockServer(mockServerCalls)
			defer mockServer.Close()

			client := getMockedClient(mockServer.URL)

			err := client.update(taskQueueConfig)
			Expect(err).ToNot(HaveOccurred())
		})
	})
})
