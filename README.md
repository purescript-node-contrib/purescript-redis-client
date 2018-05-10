# purescript-redis

Redis client library for PureScript. This library depends on the npm library
ioredis. To use this library, you must manually install ioredis.

## Patched ioredis behavior

For compatibility reasons with redis protocol this library mutates globaly response type of internal ioredis javascript `hgetall` method to:

``` purescript
Array { key ∷ ByteString, value ∷ ByteString }
```

If you provide better way to handle this (custom method) I would happily merge it...

## Testing

Please run `redis-server` on port 43210 with clear redis database - test suite refuse to run if db is no empty.

Basic workflow can look like this:

```shell
redis-server --port 43210 &
```

and running tests with db cleanup can be done with:

```shell
echo flushdb | redis-cli -p 43210 | pulp test
```
