module Test.Main
  ( main
  ) where

import Prelude

import Control.Monad.Aff (Milliseconds(..), delay)
import Data.Array (sort)
import Data.ByteString as ByteString
import Data.Maybe (Maybe(..))
import Database.Redis (Expire(..), Write(..))
import Database.Redis as Redis
import Test.Unit (test)
import Test.Unit.Assert as Assert
import Test.Unit.Main (runTest)

main = runTest do
  test "Database.Redis" do
    Redis.withConnection "redis://localhost:6379" \redis -> do
      let key1 = b "purescript-redis:test:key1"
      let key2 = b "purescript-redis:test:key2"

      Redis.del redis [key1, key2]

      do
        let set = b "value1"
        Redis.set redis key1 set Nothing Nothing
        got <- Redis.get redis key1
        Assert.equal (Just set) got

      do
        got <- Redis.incr redis key2
        Assert.equal 1 got

      do
        got <- Redis.keys redis (b "*")
        Assert.equal (sort [key1, key2]) (sort got)

      do
        let set = b "value1"
        Redis.set redis key1 set (Just (EX 1)) Nothing
        got <- Redis.get redis key1
        Assert.equal (Just set) got
        delay (Milliseconds 1000.0)
        got <- Redis.get redis key1
        Assert.equal Nothing got

      do
        let set = b "value1"
        Redis.del redis [key1]
        Redis.set redis key1 set Nothing (Just XX)
        got <- Redis.get redis key1
        Assert.equal Nothing got
        Redis.set redis key1 set Nothing (Just NX)
        got <- Redis.get redis key1
        Assert.equal (Just set) got

      do
        let set = b "value1"
        Redis.del redis [key1]
        Redis.set redis key1 set Nothing (Just NX)
        got <- Redis.get redis key1
        Assert.equal (Just set) got
        Redis.set redis key1 (b "new") Nothing (Just NX)
        got <- Redis.get redis key1
        Assert.equal (Just set) got

  where
  b = ByteString.toUTF8
