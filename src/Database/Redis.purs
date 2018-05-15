module Database.Redis
  ( REDIS
  , Connection
  , Expire(..)
  , negInf
  , posInf
  , Write(..)
  , Zadd(..)
  , ZaddReturn(..)
  , ZscoreInterval(..)

  , connect
  , del
  , disconnect
  , flushdb
  , hget
  , hgetall
  , hset
  , get
  , incr
  , keys
  , lpop
  , lpush
  , lrange
  , mget
  , set
  , withConnection
  , zadd
  , zcard
  , zrank
  , zincrby
  , zrange
  , zrem
  , zremrangebylex
  , zremrangebyrank
  , zremrangebyscore
  ) where

import Prelude

import Control.Monad.Aff (Aff, bracket)
import Control.Monad.Aff.Compat (EffFnAff, fromEffFnAff)
import Control.Monad.Eff (kind Effect)
import Data.ByteString (ByteString, toUTF8)
import Data.Int53 (class Int53Value, Int53, toInt53, toString)
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

type SortedSetItem i = { member :: ByteString, score :: i }
data ZaddReturn = Changed | Added
data Zadd = ZaddAll ZaddReturn | ZaddRestrict Write
data ZscoreInterval i = Excl i | Incl i | NegInf | PosInf

serZscoreInterval ∷ ∀ i. Int53Value i => ZscoreInterval i → ByteString
serZscoreInterval NegInf = toUTF8 "-inf"
serZscoreInterval PosInf = toUTF8 "+inf"
serZscoreInterval (Excl i) = toUTF8 <<< ("(" <> _) <<< toString <<< toInt53 $ i
serZscoreInterval (Incl i) = toUTF8 <<< toString <<< toInt53 $ i
-- | Use this to avoid type signatures
negInf = NegInf :: ZscoreInterval Int
posInf = PosInf :: ZscoreInterval Int

foreign import delImpl
  :: ∀ eff
   . Connection
  -> Array ByteString
  -> EffFnAff (redis :: REDIS | eff) Unit
foreign import flushdbImpl
  :: ∀ eff
   . Connection
  -> EffFnAff (redis :: REDIS | eff) Unit
foreign import getImpl
  :: ∀ eff
   . Connection
  -> ByteString
  -> EffFnAff (redis :: REDIS | eff) (Maybe ByteString)
foreign import hgetallImpl
  :: ∀ eff
   . Connection
  -> ByteString
  -> EffFnAff
      (redis :: REDIS | eff)
      (Array { key :: ByteString, value :: ByteString })
foreign import hgetImpl
  :: ∀ eff
   . Connection
  -> ByteString
  -> ByteString
  -> EffFnAff (redis :: REDIS | eff) (Nullable ByteString)
foreign import hsetImpl
  :: ∀ eff
   . Connection
  -> ByteString
  -> ByteString
  -> ByteString
  -> EffFnAff (redis :: REDIS | eff) Int
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
foreign import lrangeImpl
  :: ∀ eff
   . Connection
  -> ByteString
  -> Int
  -> Int
  -> EffFnAff (redis :: REDIS | eff) (Array ByteString)
foreign import mgetImpl
  :: ∀ eff
   . Connection
  -> Array ByteString
  -> EffFnAff (redis :: REDIS | eff) (Array ByteString)
foreign import setImpl
  :: ∀ eff
  . Connection
  -> ByteString
  -> ByteString
  -> Nullable { unit :: ByteString, value :: Int }
  -> Nullable ByteString
  -> EffFnAff (redis :: REDIS | eff) Unit
-- | ZADD key [NX|XX] [CH]
-- | INCR mode would be supported by `zaddIncrImpl`
foreign import zaddImpl
  :: ∀ eff
   . Connection
  -> ByteString
  -> Nullable ByteString
  -> Nullable ByteString
  -> Array (SortedSetItem Int53)
  -> EffFnAff (redis :: REDIS | eff) Int
foreign import zcardImpl
  :: ∀ eff
   . Connection
  -> ByteString
  -> EffFnAff (redis :: REDIS | eff) Int
foreign import zrangeImpl
  :: ∀ eff
   . Connection
  -> ByteString
  -> Int
  -> Int
  -> EffFnAff (redis :: REDIS | eff) (Array (SortedSetItem Int53))
foreign import zincrbyImpl
  :: ∀ eff
   . Connection
  -> ByteString
  -> Int53
  -> ByteString
  -> EffFnAff (redis :: REDIS | eff) Int53
foreign import zrankImpl
  :: ∀ eff
   . Connection
  -> ByteString
  -> ByteString
  -> EffFnAff (redis :: REDIS | eff) (Nullable Int)
foreign import zremImpl
  :: ∀ eff
   . Connection
  -> ByteString
  -> Array ByteString
  -> EffFnAff (redis :: REDIS | eff) Int
foreign import zremrangebylexImpl
  :: ∀ eff
   . Connection
  -> ByteString
  -> ByteString
  -> ByteString
  -> EffFnAff (redis :: REDIS | eff) Int
foreign import zremrangebyrankImpl
  :: ∀ eff
   . Connection
  -> ByteString
  -> Int
  -> Int
  -> EffFnAff (redis :: REDIS | eff) Int
foreign import zremrangebyscoreImpl
  :: ∀ eff
   . Connection
  -> ByteString
  -> ByteString
  -> ByteString
  -> EffFnAff (redis :: REDIS | eff) Int

del :: ∀ eff. Connection -> Array ByteString -> Aff (redis :: REDIS | eff) Unit
del conn = fromEffFnAff <<< delImpl conn
flushdb :: ∀ eff. Connection -> Aff (redis :: REDIS | eff) Unit
flushdb = fromEffFnAff <<< flushdbImpl
get :: ∀ eff. Connection -> ByteString -> Aff (redis :: REDIS | eff) (Maybe ByteString)
get conn = fromEffFnAff <<< getImpl conn
hget :: ∀ eff. Connection -> ByteString -> ByteString -> Aff (redis :: REDIS | eff) (Maybe ByteString)
hget conn key field = toMaybe <$> (fromEffFnAff $ hgetImpl conn key field)
hgetall :: ∀ eff. Connection -> ByteString -> Aff (redis :: REDIS | eff) (Array {key :: ByteString, value :: ByteString})
hgetall conn = fromEffFnAff <<< hgetallImpl conn
hset :: ∀ eff. Connection -> ByteString -> ByteString -> ByteString -> Aff (redis :: REDIS | eff) Int
hset conn key field = fromEffFnAff <<< hsetImpl conn key field
incr :: ∀ eff. Connection -> ByteString -> Aff (redis :: REDIS | eff) Int
incr conn = fromEffFnAff <<< incrImpl conn
keys :: ∀ eff. Connection -> ByteString -> Aff (redis :: REDIS | eff) (Array ByteString)
keys conn = fromEffFnAff <<< keysImpl conn
lpop :: ∀ eff . Connection -> ByteString -> Aff (redis :: REDIS | eff) (Maybe ByteString)
lpop conn key = toMaybe <$> (fromEffFnAff $ lpopImpl conn key)
lpush :: ∀ eff . Connection -> ByteString -> ByteString -> Aff (redis :: REDIS | eff) Int
lpush conn key = fromEffFnAff <<< lpushImpl conn key
lrange :: ∀ eff . Connection -> ByteString -> Int -> Int -> Aff (redis :: REDIS | eff) (Array ByteString)
lrange conn key start = fromEffFnAff <<< lrangeImpl conn key start
mget :: ∀ eff. Connection -> Array ByteString -> Aff (redis :: REDIS | eff) (Array ByteString)
mget conn = fromEffFnAff <<< mgetImpl conn
rpop :: ∀ eff . Connection -> ByteString -> Aff (redis :: REDIS | eff) (Maybe ByteString)
rpop conn key = toMaybe <$> (fromEffFnAff $ lpopImpl conn key)
rpush :: ∀ eff . Connection -> ByteString -> ByteString -> Aff (redis :: REDIS | eff) Int
rpush conn key = fromEffFnAff <<< lpushImpl conn key
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
-- | This API allows you to build only these
-- | combination of write modes and return values:
-- | ```
-- | ZADD key CH score member [score member]
-- | ZADD key score member [score member]
-- | ZADD key XX CH score member [score member]
-- | ZADD key NX score member [score member]
zadd
  :: ∀ eff i
   . Int53Value i
  => Connection
  -> ByteString
  -> Zadd
  -> Array (SortedSetItem i)
  -> Aff ( redis :: REDIS | eff) Int
zadd conn key mode = fromEffFnAff <<< zaddImpl conn key write' return' <<< map (\r → r { score = toInt53 r.score })
  where
  Tuple write' return' = case mode of
    ZaddAll Changed -> Tuple (toNullable Nothing) (toNullable $ Just (toUTF8 "CH"))
    ZaddAll Added -> Tuple (toNullable Nothing) (toNullable $ Nothing)
    ZaddRestrict XX -> Tuple (toNullable $ Just (serWrite XX)) (toNullable $ Just (toUTF8 "CH"))
    ZaddRestrict NX -> Tuple (toNullable $ Just (serWrite NX)) (toNullable Nothing)
zcard
  :: ∀ eff
   . Connection
  -> ByteString
  -> Aff ( redis :: REDIS | eff) Int
zcard conn = fromEffFnAff <<< zcardImpl conn
zrange
  :: ∀ eff
   . Connection
  -> ByteString
  -> Int
  -> Int
  -> Aff (redis :: REDIS | eff) (Array (SortedSetItem Int53))
zrange conn key start = fromEffFnAff <<< zrangeImpl conn key start
zincrby
  :: ∀ eff i
   . Int53Value i
  => Connection
  -> ByteString
  -> i
  -> ByteString
  -> Aff (redis :: REDIS | eff) Int53
zincrby conn key increment = fromEffFnAff <<< zincrbyImpl conn key (toInt53 increment)
zrank
  :: ∀ eff
   . Connection
  -> ByteString
  -> ByteString
  -> Aff (redis :: REDIS | eff) (Maybe Int)
zrank conn key member = toMaybe <$> (fromEffFnAff $ zrankImpl conn key member)
zrem
  :: ∀ eff
   . Connection
  -> ByteString
  -> Array ByteString
  -> Aff (redis :: REDIS | eff) Int
zrem conn key = fromEffFnAff <<< zremImpl conn key
zremrangebylex
  :: ∀ eff
   . Connection
  -> ByteString
  -> ByteString
  -> ByteString
  -> Aff (redis :: REDIS | eff) Int
zremrangebylex conn key min = fromEffFnAff <<< zremrangebylexImpl conn key min
zremrangebyrank
  :: ∀ eff
   . Connection
  -> ByteString
  -> Int
  -> Int
  -> Aff (redis :: REDIS | eff) Int
zremrangebyrank conn key min = fromEffFnAff <<< zremrangebyrankImpl conn key min
zremrangebyscore
  :: ∀ eff max min
   . Int53Value min
  => Int53Value max
  => Connection
  -> ByteString
  -> (ZscoreInterval min)
  -> (ZscoreInterval max)
  -> Aff (redis :: REDIS | eff) Int
zremrangebyscore conn key min max = fromEffFnAff $ zremrangebyscoreImpl conn key min' max'
  where
  min' = serZscoreInterval min
  max' = serZscoreInterval max
