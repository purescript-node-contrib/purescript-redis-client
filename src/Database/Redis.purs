module Database.Redis
  ( REDIS
  , Connection
  , Config
  , defaultConfig
  , Expire(..)
  , IPFamily
  , negInf
  , posInf
  , Write(..)
  , Zadd(..)
  , ZaddReturn(..)
  , ZscoreInterval(..)

  , blpop
  , blpopIndef
  , brpop
  , brpopIndef
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
  , ltrim
  , mget
  , rpop
  , rpush
  , set
  , withConnection
  , zadd
  , zcard
  , zrank
  , zincrby
  , zrange
  , zrangebyscore
  , zrem
  , zremrangebylex
  , zremrangebyrank
  , zremrangebyscore
  , zrevrangebyscore
  , zscore
  ) where

import Prelude

import Control.Monad.Aff (Aff, bracket)
import Control.Monad.Aff.Compat (EffFnAff, fromEffFnAff)
import Control.Monad.Eff (kind Effect)
import Data.Array (fromFoldable)
import Data.ByteString (ByteString, toUTF8)
import Data.Int53 (class Int53Value, Int53, toInt53, toString)
import Data.Maybe (Maybe(..))
import Data.NonEmpty (NonEmpty)
import Data.Nullable (Nullable, toMaybe, toNullable)
import Data.Tuple (Tuple(..))
import Unsafe.Coerce (unsafeCoerce)

--------------------------------------------------------------------------------

foreign import data REDIS :: Effect

foreign import data Connection :: Type

data IPFamily = IPv4 | IPv6
derive instance eqIPFamily ∷ Eq IPFamily

type Config =
  { db :: Maybe Int
  , family :: IPFamily
  , host :: String
  , password :: Maybe String
  , port :: Int
  }

foreign import data Null :: Type

defaultConfig ∷ Config
defaultConfig =
  { db: Nothing
  , family: IPv4
  , host: "127.0.0.1"
  , password: Nothing
  , port: 6379
  }

withConnection
  :: ∀ eff a
   . Config
  -> (Connection -> Aff (redis :: REDIS | eff) a)
  -> Aff (redis :: REDIS | eff) a
withConnection c = bracket (connect c) disconnect

type Config' =
  { db :: Nullable Int
  , family :: Int
  , host :: String
  , password :: Nullable String
  , port :: Int
  }
foreign import connectImpl :: ∀ eff. Config' -> EffFnAff (redis :: REDIS | eff) Connection
foreign import disconnectImpl :: ∀ eff. Connection -> EffFnAff (redis :: REDIS | eff) Unit

connect :: ∀ eff. Config -> Aff (redis :: REDIS | eff) Connection
connect cfg = fromEffFnAff <<< connectImpl <<< ser $ cfg
  where
  ser c =
    { db: toNullable c.db
    , family: if c.family == IPv4 then 4 else 6
    , host: c.host
    , password: toNullable c.password
    , port: c.port
    }

disconnect :: ∀ eff. Connection -> Aff (redis :: REDIS | eff) Unit
disconnect = fromEffFnAff <<< disconnectImpl

--------------------------------------------------------------------------------

data Expire = PX Int | EX Int
data Write = NX | XX

serWrite :: Write -> ByteString
serWrite NX = toUTF8 "NX"
serWrite XX = toUTF8 "XX"

data ZaddReturn = Changed | Added
data Zadd = ZaddAll ZaddReturn | ZaddRestrict Write
type Zitem i = { member :: ByteString, score :: i }
type Zlimit = { offset :: Int, count :: Int }
data ZscoreInterval i = Excl i | Incl i | NegInf | PosInf

serZscoreInterval ∷ ∀ i. Int53Value i => ZscoreInterval i → ByteString
serZscoreInterval NegInf = toUTF8 "-inf"
serZscoreInterval PosInf = toUTF8 "+inf"
serZscoreInterval (Excl i) = toUTF8 <<< ("(" <> _) <<< toString <<< toInt53 $ i
serZscoreInterval (Incl i) = toUTF8 <<< toString <<< toInt53 $ i
-- | Use this to avoid type signatures
negInf = NegInf :: ZscoreInterval Int
posInf = PosInf :: ZscoreInterval Int

foreign import blpopImpl
  :: ∀ eff
   . Connection
  -> Array ByteString
  -> Int
  -> EffFnAff
      (redis :: REDIS | eff)
      (Nullable { key ∷ ByteString, value ∷ ByteString })
foreign import brpopImpl
  :: ∀ eff
   . Connection
  -> Array ByteString
  -> Int
  -> EffFnAff
      (redis :: REDIS | eff)
      (Nullable { key ∷ ByteString, value ∷ ByteString })
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
  -> EffFnAff (redis :: REDIS | eff) (Nullable ByteString)
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
foreign import ltrimImpl
  :: ∀ eff
   . Connection
  -> ByteString
  -> Int
  -> Int
  -> EffFnAff (redis :: REDIS | eff) Null
foreign import rpopImpl
  :: ∀ eff
   . Connection
  -> ByteString
  -> EffFnAff (redis :: REDIS | eff) (Nullable ByteString)
foreign import rpushImpl
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
  -> Array (Zitem Int53)
  -> EffFnAff (redis :: REDIS | eff) Int
foreign import zcardImpl
  :: ∀ eff
   . Connection
  -> ByteString
  -> EffFnAff (redis :: REDIS | eff) Int
foreign import zincrbyImpl
  :: ∀ eff
   . Connection
  -> ByteString
  -> Int53
  -> ByteString
  -> EffFnAff (redis :: REDIS | eff) Int53
foreign import zrangeImpl
  :: ∀ eff
   . Connection
  -> ByteString
  -> Int
  -> Int
  -> EffFnAff (redis :: REDIS | eff) (Array (Zitem Int53))
foreign import zrangebyscoreImpl
  :: ∀ eff
   . Connection
  -> ByteString
  -> ByteString
  -> ByteString
  -> Nullable Zlimit
  -> EffFnAff (redis :: REDIS | eff) (Array (Zitem Int53))
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
foreign import zrevrangebyscoreImpl
  :: ∀ eff
   . Connection
  -> ByteString
  -> ByteString
  -> ByteString
  -> Nullable Zlimit
  -> EffFnAff (redis :: REDIS | eff) (Array (Zitem Int53))
foreign import zscoreImpl
  :: ∀ eff
   . Connection
  -> ByteString
  -> ByteString
  -> EffFnAff (redis :: REDIS | eff) (Nullable Int53)
blpop
  :: ∀ eff
   . Connection
  -> NonEmpty Array ByteString
  -> Int
  -> Aff (redis :: REDIS | eff) (Maybe {key ∷ ByteString, value ∷ ByteString})
blpop conn keys timeout = toMaybe <$> (fromEffFnAff $ blpopImpl conn (fromFoldable keys) timeout)
blpopIndef
  :: ∀ eff
   . Connection
  -> NonEmpty Array ByteString
  -> Aff (redis :: REDIS | eff) {key ∷ ByteString, value ∷ ByteString}
blpopIndef conn keys = unsafeCoerce (fromEffFnAff $ blpopImpl conn (fromFoldable keys) 0)
brpop
  :: ∀ eff
   . Connection
  -> NonEmpty Array ByteString
  -> Int
  -> Aff (redis :: REDIS | eff) (Maybe {key ∷ ByteString, value ∷ ByteString})
brpop conn keys timeout = toMaybe <$> (fromEffFnAff $ brpopImpl conn (fromFoldable keys) timeout)
brpopIndef
  :: ∀ eff
   . Connection
  -> NonEmpty Array ByteString
  -> Aff (redis :: REDIS | eff) {key ∷ ByteString, value ∷ ByteString}
brpopIndef conn keys = unsafeCoerce (fromEffFnAff $ brpopImpl conn (fromFoldable keys) 0)
del
  :: ∀ eff
   . Connection
  -> Array ByteString
  -> Aff (redis :: REDIS | eff) Unit
del conn = fromEffFnAff <<< delImpl conn
flushdb
  :: ∀ eff
   . Connection
  -> Aff (redis :: REDIS | eff) Unit
flushdb = fromEffFnAff <<< flushdbImpl
get
  :: ∀ eff
   . Connection
  -> ByteString
  -> Aff (redis :: REDIS | eff) (Maybe ByteString)
get conn key = toMaybe <$> (fromEffFnAff $ getImpl conn key)
hget
  :: ∀ eff
   . Connection
  -> ByteString
  -> ByteString
  -> Aff (redis :: REDIS | eff) (Maybe ByteString)
hget conn key field = toMaybe <$> (fromEffFnAff $ hgetImpl conn key field)
hgetall
  :: ∀ eff
   . Connection
  -> ByteString
  -> Aff (redis :: REDIS | eff) (Array {key :: ByteString, value :: ByteString})
hgetall conn = fromEffFnAff <<< hgetallImpl conn
hset
  :: ∀ eff
   . Connection
  -> ByteString
  -> ByteString
  -> ByteString
  -> Aff (redis :: REDIS | eff) Int
hset conn key field = fromEffFnAff <<< hsetImpl conn key field
incr
  :: ∀ eff
   . Connection
  -> ByteString
  -> Aff (redis :: REDIS | eff) Int
incr conn = fromEffFnAff <<< incrImpl conn
keys
  :: ∀ eff
   . Connection
  -> ByteString
  -> Aff (redis :: REDIS | eff) (Array ByteString)
keys conn = fromEffFnAff <<< keysImpl conn
lpop
  :: ∀ eff
   . Connection
  -> ByteString
  -> Aff (redis :: REDIS | eff) (Maybe ByteString)
lpop conn key = toMaybe <$> (fromEffFnAff $ lpopImpl conn key)
lpush
  :: ∀ eff
   . Connection
  -> ByteString
  -> ByteString
  -> Aff (redis :: REDIS | eff) Int
lpush conn key = fromEffFnAff <<< lpushImpl conn key
lrange
  :: ∀ eff
   . Connection
  -> ByteString
  -> Int
  -> Int
  -> Aff (redis :: REDIS | eff) (Array ByteString)
lrange conn key start = fromEffFnAff <<< lrangeImpl conn key start
ltrim
  :: ∀ eff
   . Connection
  -> ByteString
  -> Int
  -> Int
  -> Aff (redis :: REDIS | eff) Unit
ltrim conn key start end = unit <$ (fromEffFnAff $ ltrimImpl conn key start end)
mget
  :: ∀ eff
   . Connection
  -> Array ByteString
  -> Aff (redis :: REDIS | eff) (Array ByteString)
mget conn = fromEffFnAff <<< mgetImpl conn
rpop
  :: ∀ eff
   . Connection
  -> ByteString
  -> Aff (redis :: REDIS | eff) (Maybe ByteString)
rpop conn key = toMaybe <$> (fromEffFnAff $ rpopImpl conn key)
rpush
  :: ∀ eff
  . Connection
  -> ByteString
  -> ByteString
  -> Aff (redis :: REDIS | eff) Int
rpush conn key = fromEffFnAff <<< rpushImpl conn key
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
  -> Array (Zitem i)
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
zincrby
  :: ∀ eff i
   . Int53Value i
  => Connection
  -> ByteString
  -> i
  -> ByteString
  -> Aff (redis :: REDIS | eff) Int53
zincrby conn key increment = fromEffFnAff <<< zincrbyImpl conn key (toInt53 increment)
zrange
  :: ∀ eff
   . Connection
  -> ByteString
  -> Int
  -> Int
  -> Aff (redis :: REDIS | eff) (Array (Zitem Int53))
zrange conn key start = fromEffFnAff <<< zrangeImpl conn key start
zrangebyscore
  :: ∀ eff max min
   . Int53Value min
  => Int53Value max
  => Connection
  -> ByteString
  -> (ZscoreInterval min)
  -> (ZscoreInterval max)
  -> Maybe Zlimit
  -> Aff (redis :: REDIS | eff) (Array (Zitem Int53))
zrangebyscore conn key min max limit = fromEffFnAff $ zrangebyscoreImpl conn key min' max' limit'
  where
  min' = serZscoreInterval min
  max' = serZscoreInterval max
  limit' = toNullable limit
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
zrevrangebyscore
  :: ∀ eff max min
   . Int53Value min
  => Int53Value max
  => Connection
  -> ByteString
  -> (ZscoreInterval min)
  -> (ZscoreInterval max)
  -> Maybe Zlimit
  -> Aff (redis :: REDIS | eff) (Array (Zitem Int53))
zrevrangebyscore conn key min max limit = fromEffFnAff $ zrevrangebyscoreImpl conn key min' max' limit'
  where
  min' = serZscoreInterval min
  max' = serZscoreInterval max
  limit' = toNullable limit

zscore
  :: ∀ eff
   . Connection
  -> ByteString
  -> ByteString
  -> Aff (redis :: REDIS | eff) (Maybe Int53)
zscore conn key = (toMaybe <$> _) <<< fromEffFnAff <<< zscoreImpl conn key
