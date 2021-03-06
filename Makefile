DEPS = $(shell go list -f '{{range .Imports}}{{.}} {{end}}' ./...)
TESTDEPS = $(shell go list -f '{{range .TestImports}}{{.}} {{end}}' ./...)
TOOLDEPS = github.com/golang/lint/golint
# TOOLDEPS += code.google.com/p/go.tools/cmd/vet

PRINTFUNCS = Debug:0,Debugf:1,Info:0,Infof:1,Warn:0,Warnf:1,Error:0,Errorf:1,\
	Fatal:0,Fatalf:1,Panic:0,Panicf:1

# GOPATH isn't in bin on travis
LINT=$(shell echo $$GOPATH | cut -d ":" -f1)/bin/golint

VERSION = $(shell git describe --tags | sed 's/^v//')

default:
	go build -ldflags "-X main.version $(VERSION)"

clean:
	rm -f delayd

deps:
	go get -d -v ./...
	echo $(TESTDEPS) | xargs -n1 go get -d -v
	echo $(TOOLDEPS) | xargs -n1 go get -v

update-deps:
	echo $(DEPS) | xargs -n1 go get -u -d -v
	echo $(TESTDEPS) | xargs -n1 go get -u -d -v
	echo $(TOOLDEPS) | xargs -n1 go get -u -v


test:
	go test -race -short -timeout=1s ./...

test_amqp:
	go test ./cmd/delayd/ -race -timeout=60s -amqp

test_amqp_consul:
	go test ./cmd/delayd/ -run='TestAMQP_Multiple' -race -timeout=120s -amqp -consul

test_sqs:
	go test ./cmd/delayd/ -race -timeout=60s -sqs

test_sqs_consul:
	go test ./cmd/delayd/ -run='TestSQS_Multiple' -race -timeout=120s -sqs -consul

check: lint
	gofmt -l .
	[ -z "$$(gofmt -l .)" ]

# XXX vet fails to build at the moment. revisit later.
vet:
	go tool vet -printfuncs="$(PRINTFUNCS)" .

# golint has no options or ways to ignore values, so if we start getting false
# positives, just take it out of the build flow.
lint:
	$(LINT) .
	[ -z "$$($(LINT) .)" ]

config:
	if [ -f delayd.toml.local ]; then \
	  cp delayd.toml.local delayd.toml; \
	else \
	  cp delayd.toml.sample delayd.toml; \
	fi
	cp delayd.toml cmd/delayd/delayd.toml

cover:
	go test -cover ./...

# has to use the full package name for me
htmlcov:
	go test -coverprofile /tmp/delayd-coverprof.cov ./...
	go tool cover -html /tmp/delayd-coverprof.cov

# has to use the full package name for me
funccov:
	go test -coverprofile /tmp/delayd-coverprof.cov ./...
	go tool cover -func /tmp/delayd-coverprof.cov

ci: config check test test_amqp test_amqp_consul

release-builder:
	docker pull golang:1.4
	docker pull debian:jessie
	docker pull nabeken/delayd:latest
	docker build --no-cache -t nabeken/delayd:release-build .

# Need Docker 1.5 or later
release:
	-rm -rf bin
	docker run -it --rm nabeken/delayd:release-build sh -c 'tar -C /go -cf - bin | base64' | openssl enc -d -base64 | tar -xvf -
	docker build --no-cache -f Dockerfile.release -t nabeken/delayd-release:latest .
