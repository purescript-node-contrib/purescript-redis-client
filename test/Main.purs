module Test.Main
  ( main
  ) where

import Prelude

import Control.Monad.Aff (Aff, Milliseconds(Milliseconds), delay)
import Control.Monad.Eff (Eff)
import Control.Monad.Eff.AVar (AVAR)
import Control.Monad.Eff.Console (CONSOLE)
import Control.Monad.Except (catchError, throwError)
import Data.Array (filter, sort, sortWith, take)
import Data.ByteString (ByteString)
import Data.ByteString as ByteString
import Data.Foldable (length)
import Data.Maybe (Maybe(..))
import Database.Redis (Connection, Expire(..), REDIS, Write(..), flushdb, keys)
import Database.Redis as Redis
import Test.Unit (TestSuite, suite)
import Test.Unit as Test.Unit
import Test.Unit.Assert as Assert
import Test.Unit.Console (TESTOUTPUT)
import Test.Unit.Main (runTest)

b :: String -> ByteString
b = ByteString.toUTF8

test
  :: forall t39 t40
   . String
  -> String
  -> (Connection -> Aff ( redis :: REDIS | t39) t40)
  -> TestSuite (redis :: REDIS | t39)
test s title action =
  Test.Unit.test title $ do
    withFlushdb s action

withFlushdb
  :: ∀ a eff
   . String
  -> (Connection -> Aff ( redis :: REDIS | eff) a)
  -> Aff ( redis :: REDIS | eff) Unit
withFlushdb s action = Redis.withConnection s \conn -> do
  k <- keys conn (b "*")
  -- Safe guard
  Assert.assert  "Test database should be empty" (length k == 0)
  catchError (action conn >>= const (flushdb conn)) (\e -> flushdb conn >>= const (throwError e))

main
  :: forall eff
  . Eff
    ( console :: CONSOLE
    , testOutput :: TESTOUTPUT
    , avar :: AVAR
    , redis :: REDIS
    | eff
    )
    Unit
main = runTest $ do
  let
    addr = "redis://127.0.0.1:43210"
    key1 = b "purescript-redis:test:key1"
    key2 = b "purescript-redis:test:key2"

  suite "Database.Redis" do
    test addr "set and get" $ \conn -> do
      let set = b "value1"
      Redis.set conn key1 set Nothing Nothing
      got <- Redis.get conn key1
      Assert.equal (Just set) got

    test addr "incr on empty value" $ \conn -> do
      got <- Redis.incr conn key2
      Assert.equal 1 got

    test addr "keys *" $ \conn -> do
      void $ Redis.incr conn key1
      void $ Redis.incr conn key2
      got <- Redis.keys conn (b "*")
      Assert.equal (sort [key1, key2]) (sort got)

    test addr "key expiration" $ \conn -> do
      let set = b "value1"
      Redis.set conn key1 set (Just (EX 1)) Nothing
      got1 <- Redis.get conn key1
      Assert.equal (Just set) got1
      delay (Milliseconds 1000.0)
      got2 <- Redis.get conn key1
      Assert.equal Nothing got2

    test addr "set with XX" $ \conn -> do
      let set = b "value1"
      Redis.del conn [key1]
      Redis.set conn key1 set Nothing (Just XX)
      got1 <- Redis.get conn key1
      Assert.equal Nothing got1
      Redis.set conn key1 set Nothing (Just NX)
      got2 <- Redis.get conn key1
      Assert.equal (Just set) got2

    test addr "set with NX" $ \conn -> do
     let set = b "value1"
     Redis.del conn [key1]
     Redis.set conn key1 set Nothing (Just NX)
     got1 <- Redis.get conn key1
     Assert.equal (Just set) got1
     Redis.set conn key1 (b "new") Nothing (Just NX)
     got2 <- Redis.get conn key1
     Assert.equal (Just set) got2

    test addr "mget" $ \conn -> do
      void $ Redis.incr conn key1
      void $ Redis.incr conn key2
      got <- Redis.mget conn [key1, key2]
      Assert.equal [b "1", b "1"] got

    suite "sorted set" do
      let testSet = b "testSet"
      test addr "zadd" $ \conn -> do
        let
          members =
            [{member: b "m1", score: 1.5}, { member: b "m2", score: 2.2} , { member: b "m3", score: 3.0}]
        count <- Redis.zadd
          conn
          testSet
          (Redis.ZaddAll Redis.Added)
          members
        Assert.equal count 3
        got <- Redis.zrange conn testSet 0 1
        Assert.equal (map _.member <<< take 2 $ members) (map _.member got)
        Assert.equal (map _.score <<< take 2 $ members) (map _.score got)

      test addr "zadd XX" $ \conn -> do
        let
          members =
            [{member: b "m1", score: 1.5}, { member: b "m2", score: 2.2} , { member: b "m3", score: 3.0}]
        void $ Redis.zadd
          conn
          testSet
          (Redis.ZaddAll Redis.Added)
          members

        let
          updated =
            [ { member: b "m1", score: 1.2}
            , { member: b "m2", score: 2.2}
            , { member: b "m3", score: 3.1}
            , { member: b "new", score: 3.2 }
            ]
        count <- Redis.zadd
          conn
          testSet
          (Redis.ZaddRestrict Redis.XX)
          updated

        Assert.equal 2 count
        got <- Redis.zrange conn testSet 0 100
        -- | We should only modify existing items and not add new ones
        let updated' = filter ((_ /= b "new") <<< _.member ) updated
        Assert.equal (map _.member updated') (map _.member got)
        Assert.equal (map _.score updated') (map _.score got)

      test addr "zadd NX" $ \conn -> do
        let
          members =
            [{member: b "m1", score: 1.5}, { member: b "m2", score: 2.2} , { member: b "m3", score: 3.0}]
        void $ Redis.zadd
          conn
          testSet
          (Redis.ZaddAll Redis.Added)
          members

        let
          updated =
            [ { member: b "m1", score: 1.2}
            , { member: b "m2", score: 2.2}
            , { member: b "m3", score: 3.1}
            , { member: b "new", score: 3.2 }
            ]
        count <- Redis.zadd
          conn
          testSet
          (Redis.ZaddRestrict Redis.NX)
          updated

        Assert.equal 1 count
        got <- Redis.zrange conn testSet 0 100
        -- | We should only add new items and not modify existing ones
        let updated' = members <> [{ member: b "new", score: 3.2 }]
        Assert.equal (map _.member updated') (map _.member got)
        Assert.equal (map _.score updated') (map _.score got)

      test addr "zcard" $ \conn -> do
        let
          members =
            [{member: b "m1", score: 1.5}, { member: b "m2", score: 2.2} , { member: b "m3", score: 3.0}]
        void $ Redis.zadd
          conn
          testSet
          (Redis.ZaddAll Redis.Added)
          members
        count ← Redis.zcard conn testSet
        Assert.equal count 3

      test addr "zincrby/zrank" $ \conn -> do
        let
          member1 = { member: b "m1", score: 1.5 }
          member2 = { member: b "m2", score: 2.5 }

        m1Rank <- Redis.zrank conn testSet member1.member
        Assert.equal Nothing m1Rank

        count <- Redis.zadd
          conn
          testSet
          (Redis.ZaddAll Redis.Added)
          [member1, member2]

        m1Rank' <- Redis.zrank conn testSet member1.member
        Assert.equal (Just 0) m1Rank'

        got <- Redis.zincrby conn testSet 1.5 member1.member
        Assert.equal 3.0 got

        m1Rank'' <- Redis.zrank conn testSet member1.member
        Assert.equal (Just 1) m1Rank''

    suite "hash" do
      let
        testHash = b "testHash"
        value1 = { key: b "key1", value: b "val1" }
        value2 = { key: b "key2", value: b "val2" }
        value3 = { key: b "key3", value: b "val3" }
      test addr "hset return value" $ \conn -> do
        s1 <- Redis.hset conn testHash value1.key value1.value
        s2 <- Redis.hset conn testHash value2.key value2.value
        s2' <- Redis.hset conn testHash value2.key value2.value
        Assert.equal 1 s1
        Assert.equal 1 s2
        Assert.equal 0 s2'

      test addr "hset / hget" $ \conn -> do
        s1 <- Redis.hset conn testHash value1.key value1.value
        s2 <- Redis.hset conn testHash value2.key value2.value

        v1 <- Redis.hget conn testHash value1.key
        v2 <- Redis.hget conn testHash value2.key

        Assert.equal (Just value1.value) v1
        Assert.equal (Just value2.value) v2

      test addr "hgetall" $ \conn -> do
        void $ Redis.hset conn testHash value1.key value1.value
        void $ Redis.hset conn testHash value2.key value2.value

        values <- Redis.hgetall conn testHash

        Assert.equal
          [value1.value, value2.value]
          (map _.value <<< sortWith _.key $ values)

    suite "list" do
      let
        testList = b "testList"
        value1 = b "val1"
        value2 = b "val2"
        value3 = b "val3"

      test addr "lpush / lpop" $ \conn -> do
        void $ Redis.lpush conn testList value1
        void $ Redis.lpush conn testList value2
        v2 <- Redis.lpop conn testList
        v1 <- Redis.lpop conn testList
        Assert.equal (Just value2) v2
        Assert.equal (Just value1) v1

      test addr "lrange" $ \conn -> do
        void $ Redis.lpush conn testList value3
        void $ Redis.lpush conn testList value2
        void $ Redis.lpush conn testList value1
        g1 <- Redis.lrange conn testList 0 1
        Assert.equal [value1, value2] g1
        g2 <- Redis.lrange conn testList (-3) (-1)
        Assert.equal [value1, value2, value3] g2
