from typing import Dict, List, Optional
from dataclasses import dataclass
import requests


class ObjectStorageError(Exception):
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message
        super().__init__(f"Object storage error (status {status_code}): {message}")


@dataclass
class Bucket:
    name: str
    created_at: str


@dataclass
class ObjectMetadata:
    key: str
    size: int
    content_type: Optional[str]
    etag: str
    last_modified: str
    metadata: Dict[str, str]


@dataclass
class ObjectData:
    metadata: ObjectMetadata
    data: bytes


class ObjectStorageClient:
    def __init__(self, base_url: str, timeout: int = 30):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()

    def create_bucket(self, name: str) -> Bucket:
        url = f"{self.base_url}/buckets"
        response = self.session.post(
            url,
            json={"name": name},
            timeout=self.timeout
        )

        if response.status_code != 200:
            raise ObjectStorageError(response.status_code, response.text)

        data = response.json()
        return Bucket(name=data["name"], created_at=data["created_at"])

    def list_buckets(self) -> List[Bucket]:
        url = f"{self.base_url}/buckets"
        response = self.session.get(url, timeout=self.timeout)

        if response.status_code != 200:
            raise ObjectStorageError(response.status_code, response.text)

        data = response.json()
        return [Bucket(name=b["name"], created_at=b["created_at"]) for b in data["buckets"]]

    def delete_bucket(self, name: str) -> None:
        url = f"{self.base_url}/buckets/{name}"
        response = self.session.delete(url, timeout=self.timeout)

        if response.status_code != 204:
            raise ObjectStorageError(response.status_code, response.text)

    def put_object(
        self,
        bucket: str,
        key: str,
        data: bytes,
        content_type: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
    ) -> ObjectMetadata:
        url = f"{self.base_url}/buckets/{bucket}/objects/{key}"
        headers = {}

        if content_type:
            headers["content-type"] = content_type

        if metadata:
            for k, v in metadata.items():
                headers[f"x-object-meta-{k}"] = v

        response = self.session.put(
            url,
            data=data,
            headers=headers,
            timeout=self.timeout
        )

        if response.status_code != 200:
            raise ObjectStorageError(response.status_code, response.text)

        obj = response.json()
        return ObjectMetadata(
            key=obj["key"],
            size=obj["size"],
            content_type=obj.get("content_type"),
            etag=obj["etag"],
            last_modified=obj["last_modified"],
            metadata=obj.get("metadata", {}),
        )

    def get_object(self, bucket: str, key: str) -> ObjectData:
        url = f"{self.base_url}/buckets/{bucket}/objects/{key}"
        response = self.session.get(url, timeout=self.timeout)

        if response.status_code != 200:
            raise ObjectStorageError(response.status_code, response.text)

        size = int(response.headers.get("content-length", 0))
        content_type = response.headers.get("content-type")
        etag = response.headers.get("etag", "")
        last_modified = response.headers.get("last-modified", "")

        metadata = ObjectMetadata(
            key=key,
            size=size,
            content_type=content_type,
            etag=etag,
            last_modified=last_modified,
            metadata={},
        )

        return ObjectData(metadata=metadata, data=response.content)

    def head_object(self, bucket: str, key: str) -> ObjectMetadata:
        url = f"{self.base_url}/buckets/{bucket}/objects/{key}"
        response = self.session.head(url, timeout=self.timeout)

        if response.status_code != 200:
            raise ObjectStorageError(response.status_code, "Object not found")

        size = int(response.headers.get("content-length", 0))
        content_type = response.headers.get("content-type")
        etag = response.headers.get("etag", "")
        last_modified = response.headers.get("last-modified", "")

        return ObjectMetadata(
            key=key,
            size=size,
            content_type=content_type,
            etag=etag,
            last_modified=last_modified,
            metadata={},
        )

    def delete_object(self, bucket: str, key: str) -> None:
        url = f"{self.base_url}/buckets/{bucket}/objects/{key}"
        response = self.session.delete(url, timeout=self.timeout)

        if response.status_code != 204:
            raise ObjectStorageError(response.status_code, response.text)

    def list_objects(
        self,
        bucket: str,
        prefix: Optional[str] = None,
        max_keys: Optional[int] = None,
    ) -> List[ObjectMetadata]:
        url = f"{self.base_url}/buckets/{bucket}/objects"
        params = {}

        if prefix is not None:
            params["prefix"] = prefix
        if max_keys is not None:
            params["max_keys"] = max_keys

        response = self.session.get(url, params=params, timeout=self.timeout)

        if response.status_code != 200:
            raise ObjectStorageError(response.status_code, response.text)

        data = response.json()
        return [
            ObjectMetadata(
                key=obj["key"],
                size=obj["size"],
                content_type=obj.get("content_type"),
                etag=obj["etag"],
                last_modified=obj["last_modified"],
                metadata=obj.get("metadata", {}),
            )
            for obj in data["objects"]
        ]
