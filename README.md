# Fougere-Lite
## Context

This is Fougere-Lite, a CLI integration tool to create and update resources on Google Cloud Platform.

## Setup

### Google Cloud Project
First of all, you will need to create a google project on Google Cloud Platform to test the CLI and your work. You can create an account and get free credits [here](https://console.cloud.google.com/getting-started?pli=1).

### Local installation

#### gcloud

You will need to install the `gcloud` CLI tool. Follow the steps here: https://cloud.google.com/sdk/docs/quickstart

#### go

You will also need to install `go`. Follow the installation instructions here: https://go.dev/doc/install

#### project dependencies

To install the project's dependencies run `go mod download` at the root of the project.


## How It Works

The first step to use this CLI is to `make build` at the root of the project.

There is also a `make test` command to run all the tests.

### The CLI

The entrypoint for fougere-lite is in `cmd/fougere-lite.go` and is defined as `fougere-lite`. The commands defined in `internal/client/cli.go` are added to this base command. 

To create or update existing resources, the command is `./fougere-lite clients create -c PATH-TO-CONFIG-FILE`

The default config file is `fougere-lite.template.yaml`. All the resources to create are defined in that file.
### GCP Resources

The code for creating the resources is found in `internal/gcp/`. Right now, there is only a folder `cloudstorage` inside because it's the only resource we manage.

## To-do
#### 1. Add the necessary code to support the resource management of GCP's Cloud Task product.
The code for managing Cloud Storage resources is already given. We want you to add some code to be able to [create](https://cloud.google.com/tasks/docs/reference/rest/v2/projects.locations.queues/create) and [update](https://cloud.google.com/tasks/docs/reference/rest/v2/projects.locations.queues/patch) Cloud Task queues based on the `fougere-lite.template.yaml` file using this [library](https://pkg.go.dev/google.golang.org/api@v0.107.0/cloudtasks/v2).

The queues that we want to be able to create are already defined in the `fougere-lite.template.yaml` file.

The objective here is to be able to manage those resources the same way the `cloudstorage` resources are being managed in `fougere-lite`.
#### 2. Add some tests for your code.
Some basic tests have been included in the code for `cloudstorage`. We want you to also add some tests for your code to make sure it's working as intended.
