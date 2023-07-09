// ©Copyright 2022 Metrio
package cloudstorage

import (
	"bytes"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/spf13/viper"
)

var validBucketConfig = []byte(`
storageBucket:
  metrio-test:
    region: us-central1
    projectId: some-project`)

var invalidConfig = []byte(`
storageBucket:
  some-bucket:
    region:
      - should_not_be_an_array`)

var _ = Describe("config", func() {
	BeforeEach(func() {
		viper.Reset()
		viper.SetConfigType("yaml")
	})
	Describe("GetStorageConfig", func() {
		It("should successfully parse a storage bucket config", func() {
			err := viper.ReadConfig(bytes.NewBuffer(validBucketConfig))
			Expect(err).ToNot(HaveOccurred())
			storageConfig, err := GetStorageConfig(viper.GetViper(), "metrio-client")
			Expect(err).To(BeNil())
			Expect(len(storageConfig.StorageBuckets)).To(Equal(1))
			bucket := storageConfig.StorageBuckets["metrio-test"]
			Expect(bucket.Region).To(Equal("us-central1"))
			Expect(bucket.ProjectId).To(Equal("some-project"))
		})
		It("returns an error if cannot parse the config", func() {
			err := viper.ReadConfig(bytes.NewBuffer(invalidConfig))
			Expect(err).ToNot(HaveOccurred())
			_, err = GetStorageConfig(viper.GetViper(), "metrio-client")
			Expect(err).NotTo(BeNil())
		})
	})
	Context("validates storage buckets", func() {
		It("should not detect error", func() {
			config := &Config{
				StorageBuckets: map[string]StorageBucket{
					"foooo": {
						Region:    "us-central1",
						ProjectId: "mock-project",
						Name:      "foooo",
					},
				},
			}
			err := ValidateConfig(config)
			Expect(err).ShouldNot(HaveOccurred())
		})
		It("should detect empty name storage buckets", func() {
			config := &Config{
				StorageBuckets: map[string]StorageBucket{
					"foooo": {
						Region:    "us-central1",
						ProjectId: "mock-project",
					},
				},
			}
			err := ValidateConfig(config)
			Expect(err).Should(MatchError(ContainSubstring("validate failed on the required rule")))
		})
		It("should detect an empty region", func() {
			config := &Config{
				StorageBuckets: map[string]StorageBucket{
					"foooo": {
						ProjectId: "mock-project",
						Name:      "foooo",
					},
				},
			}
			err := ValidateConfig(config)
			Expect(err).Should(MatchError(ContainSubstring("validate failed on the required rule")))
		})
		It("should detect a missing project id", func() {
			config := &Config{
				StorageBuckets: map[string]StorageBucket{
					"foooo": {
						Name: "foooo",
					},
				},
			}
			err := ValidateConfig(config)
			Expect(err).Should(MatchError(ContainSubstring("validate failed on the required rule")))
		})
	})
})
