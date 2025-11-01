FROM python:3.9-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY protos/ ./protos/
COPY src/ ./src/

# Generate protobuf code
RUN python -m grpc_tools.protoc \
    -I./protos \
    --python_out=./protos \
    --grpc_python_out=./protos \
    ./protos/mapreduce.proto

# Default command (overridden in docker-compose)
CMD ["python3", "-m", "src.master.server"]