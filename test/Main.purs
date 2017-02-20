module Test.Main
  ( main
  ) where

import Data.Array (sort)
import Data.ByteString as ByteString
import Data.Maybe (Maybe(..))
import Database.Redis as Redis
import Node.Encoding (Encoding(UTF8))
import Prelude
import Test.Unit (test)
import Test.Unit.Assert as Assert
import Test.Unit.Main (runTest)

main = runTest do
  test "Database.Redis" do
    Redis.withConnection "redis://localhost" \redis -> do
      let key1 = b "purescript-redis:test:key1"
      let key2 = b "purescript-redis:test:key2"

      Redis.del redis [key1, key2]

      do
        let set = b "value1"
        Redis.set redis key1 set
        got <- Redis.get redis key1
        Assert.equal (Just set) got

      do
        got <- Redis.incr redis key2
        Assert.equal 1 got

      do
        got <- Redis.keys redis (b "*")
        Assert.equal (sort [key1, key2]) (sort got)
  where
  b = ByteString.fromString `flip` UTF8
