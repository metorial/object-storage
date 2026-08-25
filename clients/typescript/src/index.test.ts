import MockAdapter from 'axios-mock-adapter';
import { ObjectStorageClient, ObjectStorageError } from './index';

describe('ObjectStorageClient', () => {
  let client: ObjectStorageClient;
  let mock: MockAdapter;

  beforeEach(() => {
    client = new ObjectStorageClient('http://localhost:8080');
    mock = new MockAdapter((client as any).client);
  });

  afterEach(() => {
    mock.restore();
  });

  describe('createBucket', () => {
    it('should create a bucket', async () => {
      mock.onPost('/buckets').reply(200, {
        name: 'test-bucket',
        created_at: '2024-01-01T00:00:00Z',
      });

      const bucket = await client.createBucket('test-bucket');
      expect(bucket.name).toBe('test-bucket');
      expect(bucket.created_at).toBe('2024-01-01T00:00:00Z');
    });

    it('should throw error on conflict', async () => {
      mock.onPost('/buckets').reply(409, 'Bucket already exists');

      await expect(client.createBucket('test-bucket')).rejects.toThrow(
        ObjectStorageError
      );
    });
  });

  describe('listBuckets', () => {
    it('should list buckets', async () => {
      mock.onGet('/buckets').reply(200, {
        buckets: [
          { name: 'bucket1', created_at: '2024-01-01T00:00:00Z' },
          { name: 'bucket2', created_at: '2024-01-02T00:00:00Z' },
        ],
      });

      const buckets = await client.listBuckets();
      expect(buckets).toHaveLength(2);
      expect(buckets[0].name).toBe('bucket1');
      expect(buckets[1].name).toBe('bucket2');
    });
  });

  describe('deleteBucket', () => {
    it('should delete a bucket', async () => {
      mock.onDelete('/buckets/test-bucket').reply(204);

      await expect(client.deleteBucket('test-bucket')).resolves.toBeUndefined();
    });

    it('should throw error on not found', async () => {
      mock.onDelete('/buckets/test-bucket').reply(404, 'Bucket not found');

      await expect(client.deleteBucket('test-bucket')).rejects.toThrow(
        ObjectStorageError
      );
    });
  });

  describe('putObject', () => {
    it('should put an object', async () => {
      mock.onPut('/buckets/test-bucket/objects/test-key').reply(200, {
        key: 'test-key',
        size: 13,
        content_type: 'text/plain',
        etag: 'abc123',
        last_modified: '2024-01-01T00:00:00Z',
        metadata: { key1: 'value1' },
      });

      const data = Buffer.from('Hello, World!');
      const obj = await client.putObject(
        'test-bucket',
        'test-key',
        data,
        'text/plain',
        { key1: 'value1' }
      );

      expect(obj.key).toBe('test-key');
      expect(obj.size).toBe(13);
      expect(obj.etag).toBe('abc123');
    });
  });

  describe('getObject', () => {
    it('should get an object', async () => {
      const data = Buffer.from('Hello, World!');
      mock.onGet('/buckets/test-bucket/objects/test-key').reply(200, data, {
        'content-type': 'text/plain',
        'content-length': '13',
        'etag': 'abc123',
        'last-modified': '2024-01-01T00:00:00Z',
      });

      const obj = await client.getObject('test-bucket', 'test-key');
      expect(obj.metadata.key).toBe('test-key');
      expect(obj.metadata.size).toBe(13);
      expect(obj.metadata.etag).toBe('abc123');
      expect(obj.data).toEqual(data);
    });

    it('should throw error on not found', async () => {
      mock.onGet('/buckets/test-bucket/objects/test-key').reply(404, 'Object not found');

      await expect(client.getObject('test-bucket', 'test-key')).rejects.toThrow(
        ObjectStorageError
      );
    });
  });

  describe('headObject', () => {
    it('should head an object', async () => {
      mock.onHead('/buckets/test-bucket/objects/test-key').reply(200, undefined, {
        'content-type': 'text/plain',
        'content-length': '13',
        'etag': 'abc123',
        'last-modified': '2024-01-01T00:00:00Z',
      });

      const obj = await client.headObject('test-bucket', 'test-key');
      expect(obj.key).toBe('test-key');
      expect(obj.size).toBe(13);
      expect(obj.etag).toBe('abc123');
    });
  });

  describe('deleteObject', () => {
    it('should delete an object', async () => {
      mock.onDelete('/buckets/test-bucket/objects/test-key').reply(204);

      await expect(client.deleteObject('test-bucket', 'test-key')).resolves.toBeUndefined();
    });
  });

  describe('listObjects', () => {
    it('should list objects', async () => {
      mock.onGet('/buckets/test-bucket/objects').reply(200, {
        objects: [
          {
            key: 'prefix/obj1',
            size: 100,
            etag: 'etag1',
            last_modified: '2024-01-01T00:00:00Z',
            metadata: {},
          },
          {
            key: 'prefix/obj2',
            size: 200,
            etag: 'etag2',
            last_modified: '2024-01-02T00:00:00Z',
            metadata: {},
          },
        ],
      });

      const objects = await client.listObjects('test-bucket', 'prefix/', 10);
      expect(objects).toHaveLength(2);
      expect(objects[0].key).toBe('prefix/obj1');
      expect(objects[1].key).toBe('prefix/obj2');
    });

    it('should list objects without params', async () => {
      mock.onGet('/buckets/test-bucket/objects').reply(200, {
        objects: [],
      });

      const objects = await client.listObjects('test-bucket');
      expect(objects).toHaveLength(0);
    });

    it('should follow continuation tokens until exhausted', async () => {
      const seenTokens: (string | undefined)[] = [];

      mock.onGet('/buckets/test-bucket/objects').reply(config => {
        const token = config.params?.continuation_token;
        seenTokens.push(token);

        if (!token) {
          return [200, { objects: [{ key: 'obj1' }], next_continuation_token: 'page-2' }];
        }
        if (token === 'page-2') {
          return [200, { objects: [{ key: 'obj2' }], next_continuation_token: 'page-3' }];
        }
        return [200, { objects: [{ key: 'obj3' }], next_continuation_token: null }];
      });

      const objects = await client.listObjects('test-bucket');

      expect(seenTokens).toEqual([undefined, 'page-2', 'page-3']);
      expect(objects.map(o => o.key)).toEqual(['obj1', 'obj2', 'obj3']);
    });

    it('should stop paging once maxKeys is reached', async () => {
      let calls = 0;

      mock.onGet('/buckets/test-bucket/objects').reply(() => {
        calls++;
        return [
          200,
          {
            objects: [{ key: 'obj1' }, { key: 'obj2' }, { key: 'obj3' }],
            next_continuation_token: 'more',
          },
        ];
      });

      const objects = await client.listObjects('test-bucket', undefined, 2);

      expect(calls).toBe(1);
      expect(objects).toHaveLength(2);
    });
  });

  describe('deleteObjects', () => {
    it('should delete many objects and surface per-key results', async () => {
      mock.onPost('/buckets/test-bucket/objects/delete').reply(config => {
        expect(JSON.parse(config.data)).toEqual({ keys: ['a.txt', 'b.txt'] });

        return [
          200,
          {
            results: [
              { key: 'a.txt', deleted: true, error: null },
              { key: 'b.txt', deleted: false, error: 'boom' },
            ],
            deleted: 1,
            failed: 1,
          },
        ];
      });

      const results = await client.deleteObjects('test-bucket', ['a.txt', 'b.txt']);

      expect(results).toHaveLength(2);
      expect(results[0].deleted).toBe(true);
      expect(results[1].error).toBe('boom');
    });

    it('should not issue a request for an empty key list', async () => {
      const results = await client.deleteObjects('test-bucket', []);
      expect(results).toEqual([]);
      expect(mock.history.post).toHaveLength(0);
    });
  });

  describe('copyObject', () => {
    it('should copy an object from another logical bucket', async () => {
      mock
        .onPost('/buckets/code-bucket/copy-object/skills/demo/asset.bin')
        .reply(config => {
          expect(JSON.parse(config.data)).toEqual({
            source_bucket: 'files',
            source_key: 'abc123',
          });

          return [200, { key: 'skills/demo/asset.bin', size: 42, etag: 'abc' }];
        });

      const metadata = await client.copyObject(
        'code-bucket',
        'skills/demo/asset.bin',
        'files',
        'abc123'
      );

      expect(metadata.key).toBe('skills/demo/asset.bin');
      expect(metadata.size).toBe(42);
    });

    it('should handle not found error', async () => {
      mock.onPost('/buckets/code-bucket/copy-object/a.txt').reply(404);

      await expect(
        client.copyObject('code-bucket', 'a.txt', 'files', 'missing')
      ).rejects.toThrow(ObjectStorageError);
    });
  });

  describe('getPublicURL', () => {
    it('should get public URL with custom expiration', async () => {
      mock.onGet('/buckets/test-bucket/public-url/test-key').reply(200, {
        url: 'https://example.com/signed-url?signature=abc123',
        expires_in: 7200,
      });

      const response = await client.getPublicURL('test-bucket', 'test-key', 7200);
      expect(response.url).toBe('https://example.com/signed-url?signature=abc123');
      expect(response.expires_in).toBe(7200);
    });

    it('should get public URL with default expiration', async () => {
      mock.onGet('/buckets/test-bucket/public-url/test-key').reply(200, {
        url: 'https://example.com/signed-url?signature=xyz789',
        expires_in: 3600,
      });

      const response = await client.getPublicURL('test-bucket', 'test-key');
      expect(response.url).toBe('https://example.com/signed-url?signature=xyz789');
      expect(response.expires_in).toBe(3600);
    });

    it('should handle not found error', async () => {
      mock.onGet('/buckets/test-bucket/public-url/test-key').reply(404, 'Object not found');

      await expect(client.getPublicURL('test-bucket', 'test-key')).rejects.toThrow('Object not found');
    });
  });
});
