// ©Copyright 2022 Metrio
package cloudstorage_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestCloudstorage(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Cloudstorage Suite")
}
