# purescript-redis-client

Redis client library for PureScript. This library depends on the npm library
ioredis. To use this library, you must manually install ioredis.

## API notes

### Sorted sets `score` values

In case of sorted set methods which operate on `score` value (like `zrange`, `zadd` etc.) we are using `Int53` type to represent `score` values. However all functions which take `score` as an input accept values of all types which implement `Int53Value` instance. In other words you can pass just `Int` score values to them.

### Patched ioredis behavior

For compatibility reasons with raw redis protocol this library mutates globaly response type of javascript `hgetall` method to:

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

## Credits

This library started as a fork of [`TinkerTravel/purescript-redis`](https://github.com/TinkerTravel/purescript-redis) which is not maintained any more.
