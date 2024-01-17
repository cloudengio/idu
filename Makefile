.PHONY: pr

pr:
	go run cloudeng.io/go/cmd/gousage@latest --overwrite .
	go run cloudeng.io/go/cmd/goannotate@latest --config=copyright-annotation.yaml --annotation=cloudeng-copyright ./...
	go run cloudeng.io/go/cmd/gomarkdown@latest  --circleci=cloudengio/go.gotools --goreportcard --overwrite ./internal/...
	echo > go.sum
	go mod tidy

test:
	go test ./...
	golangci-lint run
