package cloudtasks

import (
	"bytes"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/spf13/viper"
)

var validTaskQueueConfig = []byte(`
cloudTasks:
  queue1:
    region: us-central1
    projectId: some-project
    minBackoff: 1s
    maxBackoff: 10s
    maxConcurrentDispatches: 1000
    maxDispatchesPerSecond: 500.0`)

var invalidConfig = []byte(`
cloudTasks:
  queue1:
    region:
      - should_not_be_an_array`)

var _ = Describe("config", func() {
	BeforeEach(func() {
		viper.Reset()
		viper.SetConfigType("yaml")
	})
	Describe("GetCloudTasksConfig", func() {
		It("should successfully parse a cloud task queue config", func() {
			err := viper.ReadConfig(bytes.NewBuffer(validTaskQueueConfig))
			Expect(err).ToNot(HaveOccurred())
			tasksConfig, err := GetTasksConfig(viper.GetViper(), "metrio-client")
			Expect(err).To(BeNil())
			Expect(len(tasksConfig.TaskQueues)).To(Equal(1))
			queue := tasksConfig.TaskQueues["queue1"]
			Expect(queue.Region).To(Equal("us-central1"))
			Expect(queue.ProjectId).To(Equal("some-project"))
		})
		It("returns an error if cannot parse the config", func() {
			err := viper.ReadConfig(bytes.NewBuffer(invalidConfig))
			Expect(err).ToNot(HaveOccurred())
			_, err = GetTasksConfig(viper.GetViper(), "metrio-client")
			Expect(err).NotTo(BeNil())
		})
	})
	Context("validates task queues", func() {
		It("should not detect error", func() {
			config := &Config{
				TaskQueues: map[string]TaskQueue{
					"foo": {
						Region:                  "us-central1",
						ProjectId:               "mock-project",
						Name:                    "queue1",
						MinBackoff:              "1s",
						MaxBackoff:              "10s",
						MaxConcurrentDispatches: 1000,
						MaxDispatchesPerSecond:  500.0,
					},
				},
			}
			err := ValidateConfig(config)
			Expect(err).ShouldNot(HaveOccurred())
		})
	})
})
