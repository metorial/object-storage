# Object Storage Client for Python

Python client library for interacting with the Object Storage Service.

## Usage

```python
from object_storage import ObjectStorageClient

client = ObjectStorageClient("http://localhost:8080")

# Create a bucket
bucket = client.create_bucket("my-bucket")
print(f"Created bucket: {bucket.name}")

# Upload an object
data = b"Hello, World!"
obj = client.put_object(
    "my-bucket",
    "hello.txt",
    data,
    content_type="text/plain",
    metadata={"key1": "value1"}
)
print(f"Uploaded object: {obj.key} (ETag: {obj.etag})")

# Download an object
obj_data = client.get_object("my-bucket", "hello.txt")
print(f"Downloaded {len(obj_data.data)} bytes")

# List objects
objects = client.list_objects("my-bucket")
for obj in objects:
    print(f"{obj.key}: {obj.size} bytes")

# Delete object
client.delete_object("my-bucket", "hello.txt")

# Delete bucket
client.delete_bucket("my-bucket")
```

## API Reference

### Client Creation

```python
client = ObjectStorageClient("http://localhost:8080")
```

With custom timeout:

```python
client = ObjectStorageClient("http://localhost:8080", timeout=60)
```

### Bucket Operations

**Create Bucket**
```python
bucket = client.create_bucket("bucket-name")
```

**List Buckets**
```python
buckets = client.list_buckets()
```

**Delete Bucket**
```python
client.delete_bucket("bucket-name")
```

### Object Operations

**Put Object**
```python
data = b"content"
obj = client.put_object(
    "bucket-name",
    "object-key",
    data,
    content_type="application/json",
    metadata={"key": "value"}
)
```

**Get Object**
```python
obj_data = client.get_object("bucket-name", "object-key")
# obj_data.data contains the object bytes
# obj_data.metadata contains metadata
```

**Head Object**
```python
metadata = client.head_object("bucket-name", "object-key")
```

**Delete Object**
```python
client.delete_object("bucket-name", "object-key")
```

**List Objects**
```python
# List all objects
objects = client.list_objects("bucket-name")

# List with prefix
objects = client.list_objects("bucket-name", prefix="prefix/")

# List with prefix and max keys
objects = client.list_objects("bucket-name", prefix="prefix/", max_keys=100)
```

## Error Handling

The client raises `ObjectStorageError` for API errors:

```python
from object_storage import ObjectStorageError

try:
    obj = client.get_object("bucket", "key")
except ObjectStorageError as e:
    print(f"Status: {e.status_code}, Message: {e.message}")
```
