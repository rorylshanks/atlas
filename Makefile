BIN_DIR := ../.bin
ATLAS_BIN := $(BIN_DIR)/atlas

.PHONY: build
build:
	@mkdir -p $(BIN_DIR)
	cd cmd/atlas && go build -o ../$(ATLAS_BIN) .
