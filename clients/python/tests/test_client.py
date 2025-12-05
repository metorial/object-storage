import pytest
import requests_mock
from object_storage import ObjectStorageClient, ObjectStorageError, Bucket, ObjectMetadata


def test_create_client():
    client = ObjectStorageClient("http://localhost:8080")
    assert client.base_url == "http://localhost:8080"


def test_create_bucket():
    with requests_mock.Mocker() as m:
        m.post(
            "http://localhost:8080/buckets",
            json={"name": "test-bucket", "created_at": "2024-01-01T00:00:00Z"},
            status_code=200,
        )

        client = ObjectStorageClient("http://localhost:8080")
        bucket = client.create_bucket("test-bucket")

        assert bucket.name == "test-bucket"
        assert bucket.created_at == "2024-01-01T00:00:00Z"


def test_create_bucket_error():
    with requests_mock.Mocker() as m:
        m.post(
            "http://localhost:8080/buckets",
            text="Bucket already exists",
            status_code=409,
        )

        client = ObjectStorageClient("http://localhost:8080")
        with pytest.raises(ObjectStorageError) as exc_info:
            client.create_bucket("test-bucket")

        assert exc_info.value.status_code == 409


def test_list_buckets():
    with requests_mock.Mocker() as m:
        m.get(
            "http://localhost:8080/buckets",
            json={
                "buckets": [
                    {"name": "bucket1", "created_at": "2024-01-01T00:00:00Z"},
                    {"name": "bucket2", "created_at": "2024-01-02T00:00:00Z"},
                ]
            },
            status_code=200,
        )

        client = ObjectStorageClient("http://localhost:8080")
        buckets = client.list_buckets()

        assert len(buckets) == 2
        assert buckets[0].name == "bucket1"
        assert buckets[1].name == "bucket2"


def test_delete_bucket():
    with requests_mock.Mocker() as m:
        m.delete("http://localhost:8080/buckets/test-bucket", status_code=204)

        client = ObjectStorageClient("http://localhost:8080")
        client.delete_bucket("test-bucket")


def test_delete_bucket_not_found():
    with requests_mock.Mocker() as m:
        m.delete(
            "http://localhost:8080/buckets/test-bucket",
            text="Bucket not found",
            status_code=404,
        )

        client = ObjectStorageClient("http://localhost:8080")
        with pytest.raises(ObjectStorageError) as exc_info:
            client.delete_bucket("test-bucket")

        assert exc_info.value.status_code == 404


def test_put_object():
    with requests_mock.Mocker() as m:
        m.put(
            "http://localhost:8080/buckets/test-bucket/objects/test-key",
            json={
                "key": "test-key",
                "size": 13,
                "content_type": "text/plain",
                "etag": "abc123",
                "last_modified": "2024-01-01T00:00:00Z",
                "metadata": {"key1": "value1"},
            },
            status_code=200,
        )

        client = ObjectStorageClient("http://localhost:8080")
        obj = client.put_object(
            "test-bucket",
            "test-key",
            b"Hello, World!",
            content_type="text/plain",
            metadata={"key1": "value1"},
        )

        assert obj.key == "test-key"
        assert obj.size == 13
        assert obj.etag == "abc123"
        assert obj.content_type == "text/plain"


def test_get_object():
    with requests_mock.Mocker() as m:
        m.get(
            "http://localhost:8080/buckets/test-bucket/objects/test-key",
            content=b"Hello, World!",
            headers={
                "content-type": "text/plain",
                "content-length": "13",
                "etag": "abc123",
                "last-modified": "2024-01-01T00:00:00Z",
            },
            status_code=200,
        )

        client = ObjectStorageClient("http://localhost:8080")
        obj = client.get_object("test-bucket", "test-key")

        assert obj.metadata.key == "test-key"
        assert obj.metadata.size == 13
        assert obj.metadata.etag == "abc123"
        assert obj.data == b"Hello, World!"


def test_get_object_not_found():
    with requests_mock.Mocker() as m:
        m.get(
            "http://localhost:8080/buckets/test-bucket/objects/test-key",
            text="Object not found",
            status_code=404,
        )

        client = ObjectStorageClient("http://localhost:8080")
        with pytest.raises(ObjectStorageError) as exc_info:
            client.get_object("test-bucket", "test-key")

        assert exc_info.value.status_code == 404


def test_head_object():
    with requests_mock.Mocker() as m:
        m.head(
            "http://localhost:8080/buckets/test-bucket/objects/test-key",
            headers={
                "content-type": "text/plain",
                "content-length": "13",
                "etag": "abc123",
                "last-modified": "2024-01-01T00:00:00Z",
            },
            status_code=200,
        )

        client = ObjectStorageClient("http://localhost:8080")
        obj = client.head_object("test-bucket", "test-key")

        assert obj.key == "test-key"
        assert obj.size == 13
        assert obj.etag == "abc123"


def test_delete_object():
    with requests_mock.Mocker() as m:
        m.delete(
            "http://localhost:8080/buckets/test-bucket/objects/test-key",
            status_code=204,
        )

        client = ObjectStorageClient("http://localhost:8080")
        client.delete_object("test-bucket", "test-key")


def test_list_objects():
    with requests_mock.Mocker() as m:
        m.get(
            "http://localhost:8080/buckets/test-bucket/objects",
            json={
                "objects": [
                    {
                        "key": "prefix/obj1",
                        "size": 100,
                        "etag": "etag1",
                        "last_modified": "2024-01-01T00:00:00Z",
                        "metadata": {},
                    },
                    {
                        "key": "prefix/obj2",
                        "size": 200,
                        "etag": "etag2",
                        "last_modified": "2024-01-02T00:00:00Z",
                        "metadata": {},
                    },
                ]
            },
            status_code=200,
        )

        client = ObjectStorageClient("http://localhost:8080")
        objects = client.list_objects("test-bucket", prefix="prefix/", max_keys=10)

        assert len(objects) == 2
        assert objects[0].key == "prefix/obj1"
        assert objects[1].key == "prefix/obj2"


def test_list_objects_empty():
    with requests_mock.Mocker() as m:
        m.get(
            "http://localhost:8080/buckets/test-bucket/objects",
            json={"objects": []},
            status_code=200,
        )

        client = ObjectStorageClient("http://localhost:8080")
        objects = client.list_objects("test-bucket")

        assert len(objects) == 0
