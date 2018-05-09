module Database.Redis
  ( REDIS
  , Connection
  , Expire(..)
  , Write(..)
  , Zadd(..)
  , ZaddReturn(..)

  , connect
  , del
  , disconnect
  , flushdb
  , get
  , incr
  , keys
  , lpop
  , lpush
  , mget
  , set
  , withConnection
  , zadd
  , zrank
  , zincrby
  , zrange
  ) where

import Prelude

import Control.Monad.Aff (Aff, bracket)
import Control.Monad.Aff.Compat (EffFnAff, fromEffFnAff)
import Control.Monad.Eff (kind Effect)
import Data.ByteString (ByteString, toUTF8)
import Data.Maybe (Maybe(..))
import Data.Nullable (Nullable, toMaybe, toNullable)
import Data.Tuple (Tuple(..))

--------------------------------------------------------------------------------

foreign import data REDIS :: Effect

foreign import data Connection :: Type

--------------------------------------------------------------------------------

withConnection
  :: ∀ eff a
   . String
  -> (Connection -> Aff (redis :: REDIS | eff) a)
  -> Aff (redis :: REDIS | eff) a
withConnection s = bracket (connect s) disconnect

foreign import connectImpl :: ∀ eff. String -> EffFnAff (redis :: REDIS | eff) Connection
foreign import disconnectImpl :: ∀ eff. Connection -> EffFnAff (redis :: REDIS | eff) Unit

connect :: ∀ eff. String -> Aff (redis :: REDIS | eff) Connection
connect = fromEffFnAff <<< connectImpl
disconnect :: ∀ eff. Connection -> Aff (redis :: REDIS | eff) Unit
disconnect = fromEffFnAff <<< disconnectImpl

--------------------------------------------------------------------------------

data Expire = PX Int | EX Int
data Write = NX | XX

serWrite :: Write -> ByteString
serWrite NX = toUTF8 "NX"
serWrite XX = toUTF8 "XX"

foreign import delImpl
  :: ∀ eff
   . Connection
  -> Array ByteString
  -> EffFnAff (redis :: REDIS | eff) Unit
foreign import flushdbImpl
  :: ∀ eff
   . Connection
  -> EffFnAff (redis :: REDIS | eff) Unit
foreign import setImpl
  :: ∀ eff
  . Connection
  -> ByteString
  -> ByteString
  -> Nullable { unit :: ByteString, value :: Int }
  -> Nullable ByteString
  -> EffFnAff (redis :: REDIS | eff) Unit
foreign import getImpl
  :: ∀ eff
   . Connection
  -> ByteString
  -> EffFnAff (redis :: REDIS | eff) (Maybe ByteString)
foreign import incrImpl
  :: ∀ eff
   . Connection
  -> ByteString
  -> EffFnAff (redis :: REDIS | eff) Int
foreign import keysImpl
  :: ∀ eff
   . Connection
  -> ByteString
  -> EffFnAff (redis :: REDIS | eff) (Array ByteString)
foreign import lpopImpl
  :: ∀ eff
   . Connection
  -> ByteString
  -> EffFnAff (redis :: REDIS | eff) (Nullable ByteString)
foreign import lpushImpl
  :: ∀ eff
   . Connection
  -> ByteString
  -> ByteString
  -> EffFnAff (redis :: REDIS | eff) Int
foreign import mgetImpl
  :: ∀ eff
   . Connection
  -> Array ByteString
  -> EffFnAff (redis :: REDIS | eff) (Array ByteString)
-- | ZADD key [NX|XX] [CH]
-- | INCR mode would be supported by `zaddIncrImpl`
foreign import zaddImpl
  :: ∀ eff
   . Connection
  -> ByteString
  -> Nullable ByteString
  -> Nullable ByteString
  -> Array SortedSetItem
  -> EffFnAff (redis :: REDIS | eff) Int
foreign import zrangeImpl
  :: ∀ eff
   . Connection
  -> ByteString
  -> Int
  -> Int
  -> EffFnAff (redis :: REDIS | eff) (Array SortedSetItem)
foreign import zincrbyImpl
  :: ∀ eff
   . Connection
  -> ByteString
  -> Number
  -> ByteString
  -> EffFnAff (redis :: REDIS | eff) Number
foreign import zrankImpl
  :: ∀ eff
   . Connection
  -> ByteString
  -> ByteString
  -> EffFnAff (redis :: REDIS | eff) (Nullable Int)

del :: ∀ eff. Connection -> Array ByteString -> Aff (redis :: REDIS | eff) Unit
del conn = fromEffFnAff <<< delImpl conn
flushdb :: ∀ eff. Connection -> Aff (redis :: REDIS | eff) Unit
flushdb = fromEffFnAff <<< flushdbImpl
set
  :: ∀ eff
  . Connection
  -> ByteString
  -> ByteString
  -> Maybe Expire
  -> Maybe Write
  -> Aff (redis :: REDIS | eff) Unit
set conn key value expire write = fromEffFnAff $ setImpl conn key value expire' write'
  where
  serExpire (PX v) = { unit: toUTF8 "PX", value: v }
  serExpire (EX v) = { unit: toUTF8 "EX", value: v }
  expire' = toNullable $ serExpire <$> expire
  write' = toNullable $ serWrite <$> write
get  :: ∀ eff. Connection -> ByteString -> Aff (redis :: REDIS | eff) (Maybe ByteString)
get conn = fromEffFnAff <<< getImpl conn
incr :: ∀ eff. Connection -> ByteString -> Aff (redis :: REDIS | eff) Int
incr conn = fromEffFnAff <<< incrImpl conn
keys :: ∀ eff. Connection -> ByteString -> Aff (redis :: REDIS | eff) (Array ByteString)
keys conn = fromEffFnAff <<< keysImpl conn
mget :: ∀ eff. Connection -> Array ByteString -> Aff (redis :: REDIS | eff) (Array ByteString)
mget conn = fromEffFnAff <<< mgetImpl conn
lpop :: forall t10 . Connection -> ByteString -> Aff (redis :: REDIS | t10) (Maybe ByteString)
lpop conn key = toMaybe <$> (fromEffFnAff $ lpopImpl conn key)
lpush :: forall t10 . Connection -> ByteString -> ByteString -> Aff (redis :: REDIS | t10) Int
lpush conn key = fromEffFnAff <<< lpushImpl conn key

type SortedSetItem = { member :: ByteString, score :: Number }
data ZaddReturn = Changed | Added
data Zadd = ZaddAll ZaddReturn | ZaddRestrict Write
-- | This API allows you to build only these
-- | combination of write modes and return values:
-- | ```
-- | ZADD key CH score member [score member]
-- | ZADD key score member [score member]
-- | ZADD key XX CH score member [score member]
-- | ZADD key NX score member [score member]
zadd
  :: forall t81
   . Connection
  -> ByteString
  -> Zadd
  -> Array SortedSetItem
  -> Aff ( redis :: REDIS | t81) Int
zadd conn key mode = fromEffFnAff <<< zaddImpl conn key write' return'
  where
  Tuple write' return' = case mode of
    ZaddAll Changed -> Tuple (toNullable Nothing) (toNullable $ Just (toUTF8 "CH"))
    ZaddAll Added -> Tuple (toNullable Nothing) (toNullable $ Nothing)
    ZaddRestrict XX -> Tuple (toNullable $ Just (serWrite XX)) (toNullable $ Just (toUTF8 "CH"))
    ZaddRestrict NX -> Tuple (toNullable $ Just (serWrite NX)) (toNullable Nothing)
zrange
  :: forall t10
   . Connection
  -> ByteString
  -> Int
  -> Int
  -> Aff (redis :: REDIS | t10) (Array SortedSetItem)
zrange conn key start = fromEffFnAff <<< zrangeImpl conn key start
zincrby
  :: forall t10
   . Connection
  -> ByteString
  -> Number
  -> ByteString
  -> Aff (redis :: REDIS | t10) Number
zincrby conn key increment = fromEffFnAff <<< zincrbyImpl conn key increment
zrank
  :: forall t10
   . Connection
  -> ByteString
  -> ByteString
  -> Aff (redis :: REDIS | t10) (Maybe Int)
zrank conn key member = toMaybe <$> (fromEffFnAff $ zrankImpl conn key member)
