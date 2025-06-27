PROTOC_GEN_GO := protoc-gen-go
PROTOC_GEN_GO_GRPC := protoc-gen-go-grpc
PROTO_DIR := pkg/pb
PROTO_FILE := $(PROTO_DIR)/data_service/data.proto
OUT_DIR := $(PROTO_DIR)

.PHONY: proto
proto:
	@echo "Generating Go code from Protobuf..."
	protoc \
		--go_out=$(OUT_DIR) --go_opt=paths=source_relative \
		--go-grpc_out=$(OUT_DIR) --go-grpc_opt=paths=source_relative \
		$(PROTO_FILE)
	@echo "Protobuf generation complete."
