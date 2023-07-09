// ©Copyright 2022 Metrio
package cloudstorage

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

var _ = Describe("Storage client", func() {
	var bucketConfig StorageBucket

	BeforeEach(func() {
		bucketConfig = StorageBucket{
			Name:       "patate-23423k",
			Region:     "northamerica-northeast1",
			ProjectId:  "projet-123",
			ClientName: "banane",
		}
	})
	Describe("create bucket", func() {
		It("successfully creates the bucket", func() {
			mockServerCalls := make(chan utils.MockServerCall, 1)
			mockServerCalls <- utils.MockServerCall{
				UrlMatchFunc: func(url string) bool {
					return strings.HasPrefix(url, "/b?")
				},
				Method: "post",
			}
			mockServer := utils.NewMockServer(mockServerCalls)
			defer mockServer.Close()

			client := getMockedClient(mockServer.URL)

			err := client.create(bucketConfig)
			Expect(err).ToNot(HaveOccurred())
		})
	})
	Describe("update bucket", func() {
		It("successfully updates the bucket", func() {
			mockServerCalls := make(chan utils.MockServerCall, 1)
			mockServerCalls <- utils.MockServerCall{
				UrlMatchFunc: func(url string) bool {
					return strings.HasPrefix(url, "/b/patate-23423k?")
				},
				Method: "put",
			}
			mockServer := utils.NewMockServer(mockServerCalls)
			defer mockServer.Close()

			client := getMockedClient(mockServer.URL)

			err := client.update(bucketConfig)
			Expect(err).ToNot(HaveOccurred())
		})
	})
})
