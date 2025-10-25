# Generate Python gRPC code from proto files
echo "Generating protobuf code..."

python3 -m grpc_tools.protoc \
    -I./protos \
    --python_out=./protos \
    --grpc_python_out=./protos \
    ./protos/mapreduce.proto

echo " Protobuf code generated successfully"