module Test.Main
  ( main
  ) where

import Prelude

import Control.Monad.Aff (Milliseconds(Milliseconds), delay)
import Control.Monad.Except (catchError, throwError)
import Data.Array (sort)
import Data.ByteString (ByteString)
import Data.ByteString as ByteString
import Data.Foldable (length)
import Data.Maybe (Maybe(..))
import Database.Redis (Expire(..), Write(..), flushdb, keys)
import Database.Redis as Redis
import Test.Unit (suite)
import Test.Unit as Test.Unit
import Test.Unit.Assert as Assert
import Test.Unit.Main (runTest)

b :: String -> ByteString
b = ByteString.toUTF8

test s title action =
  Test.Unit.test title $ do
    withFlushdb s action

withFlushdb s action = Redis.withConnection s \conn -> do
  k <- keys conn (b "*")
  -- Safe guard
  Assert.assert  "Test database should be empty" (length k == 0)
  catchError (action conn >>= const (flushdb conn)) (\e -> flushdb conn >>= const (throwError e))

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
      got <- Redis.get conn key1
      Assert.equal (Just set) got
      delay (Milliseconds 1000.0)
      got <- Redis.get conn key1
      Assert.equal Nothing got

    test addr "set with XX" $ \conn -> do
      let set = b "value1"
      Redis.del conn [key1]
      Redis.set conn key1 set Nothing (Just XX)
      got <- Redis.get conn key1
      Assert.equal Nothing got
      Redis.set conn key1 set Nothing (Just NX)
      got <- Redis.get conn key1
      Assert.equal (Just set) got

    test addr "set with NX" $ \conn -> do
     let set = b "value1"
     Redis.del conn [key1]
     Redis.set conn key1 set Nothing (Just NX)
     got <- Redis.get conn key1
     Assert.equal (Just set) got
     Redis.set conn key1 (b "new") Nothing (Just NX)
     got <- Redis.get conn key1
     Assert.equal (Just set) got

    test addr "mget" $ \conn -> do
      void $ Redis.incr conn key1
      void $ Redis.incr conn key2
      got <- Redis.mget conn [key1, key2]
      Assert.equal [b "1", b "1"] got

