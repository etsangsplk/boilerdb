BIN=$(GOPATH)/bin
GOFILES=src/*/*.go src/*/*/*.go
PACKAGES=adapters adapters/redis config db logging main plugins/builtin plugins/hash_table plugins/json plugins/prefix_tree plugins/replication plugins/simple plugins util

$(BIN)/boilerdb: $(GOFILES)
	go get main
	go build -o $(BIN)/boilerdb main

# TODO test more shit!
test:
	go test $(PACKAGES)

format:
	#echo $(GOFILES)
	gofmt -w $(GOFILES)

lint:
	go get github.com/golang/lint/golint
	$(BIN)/golint $(GOFILES)
	go vet $(PACKAGES)

.PHONY: test format lint
