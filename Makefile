BIN=$(GOPATH)/bin
GOFILES=src/*/*.go src/*/*/*.go

$(BIN)/boilerdb: *.go
	go get main
	go build -o $(BIN)/boilerdb

$(BIN)/test-logger: *.go
	go test -c logger
	mv logger.test $(BIN)/test-logger

# TODO test more shit!
test: $(BIN)/test-logger
ifdef TEST
	$(BIN)/test-logger -test.run="$(TEST)"
else
	$(BIN)/test-logger -test.v
endif

format:
	#echo $(GOFILES)
	gofmt -w $(GOFILES)

lint:
	go get github.com/golang/lint/golint
	$(BIN)/golint *.go
	go vet

.PHONY: test format lint
