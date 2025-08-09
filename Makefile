PROTO_DIR := pkg/pb
PROTO_FILES := $(shell find $(PROTO_DIR) -name '*.proto')

.PHONY: proto
proto:
	@echo "Generating Go code from Protobuf..."
	protoc \
		--go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		$(PROTO_FILES)
	@echo "Protobuf generation complete."
