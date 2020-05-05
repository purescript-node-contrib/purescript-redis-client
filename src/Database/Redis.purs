module Database.Redis
  ( Connection
  , Config
  , defaultConfig
  , Expire(..)
  , IPFamily
  , negInf
  , posInf
  , ScanStreamOptions
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
  , hscanStream
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
  , scanStream
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
  , zscanStream
  , zscore
  ) where

import Prelude

import Effect.Aff (Aff, bracket)
import Effect.Aff.Compat (EffectFnAff, fromEffectFnAff)
import Data.Array (fromFoldable)
import Data.ByteString (ByteString, toUTF8)
import Data.Int53 (class Int53Value, Int53, toInt53, toString)
import Data.Maybe (Maybe(..))
import Data.NonEmpty (NonEmpty)
import Data.Nullable (Nullable, toMaybe, toNullable)
import Data.Tuple (Tuple(..))
import Prim.Row (class Union)
import Node.Stream 
import Unsafe.Coerce (unsafeCoerce)

--------------------------------------------------------------------------------

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
  :: ∀ a
   . Config
  -> (Connection -> Aff a)
  -> Aff a
withConnection c = bracket (connect c) disconnect

type Config' =
  { db :: Nullable Int
  , family :: Int
  , host :: String
  , password :: Nullable String
  , port :: Int
  }
foreign import connectImpl ::  Config' -> EffectFnAff Connection
foreign import disconnectImpl :: Connection -> EffectFnAff Unit

connect :: Config -> Aff Connection
connect cfg = fromEffectFnAff <<< connectImpl <<< ser $ cfg
  where
  ser c =
    { db: toNullable c.db
    , family: if c.family == IPv4 then 4 else 6
    , host: c.host
    , password: toNullable c.password
    , port: c.port
    }

disconnect :: Connection -> Aff Unit
disconnect = fromEffectFnAff <<< disconnectImpl

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
  :: Connection
  -> Array ByteString
  -> Int
  -> EffectFnAff

      (Nullable { key ∷ ByteString, value ∷ ByteString })
foreign import brpopImpl
  :: Connection
  -> Array ByteString
  -> Int
  -> EffectFnAff

      (Nullable { key ∷ ByteString, value ∷ ByteString })
foreign import delImpl
  :: Connection
  -> Array ByteString
  -> EffectFnAff Unit
foreign import flushdbImpl
  :: Connection
  -> EffectFnAff Unit
foreign import getImpl
  :: Connection
  -> ByteString
  -> EffectFnAff (Nullable ByteString)
foreign import hgetallImpl
  :: Connection
  -> ByteString
  -> EffectFnAff
      (Array { key :: ByteString, value :: ByteString })
foreign import hgetImpl
  :: Connection
  -> ByteString
  -> ByteString
  -> EffectFnAff (Nullable ByteString)
foreign import hsetImpl
  :: Connection
  -> ByteString
  -> ByteString
  -> ByteString
  -> EffectFnAff Int
foreign import incrImpl
  :: Connection
  -> ByteString
  -> EffectFnAff Int
foreign import keysImpl
  :: Connection
  -> ByteString
  -> EffectFnAff (Array ByteString)
foreign import lpopImpl
  :: Connection
  -> ByteString
  -> EffectFnAff (Nullable ByteString)
foreign import lpushImpl
  :: Connection
  -> ByteString
  -> ByteString
  -> EffectFnAff Int
foreign import lrangeImpl
  :: Connection
  -> ByteString
  -> Int
  -> Int
  -> EffectFnAff (Array ByteString)
foreign import ltrimImpl
  :: Connection
  -> ByteString
  -> Int
  -> Int
  -> EffectFnAff Null
foreign import rpopImpl
  :: Connection
  -> ByteString
  -> EffectFnAff (Nullable ByteString)
foreign import rpushImpl
  :: Connection
  -> ByteString
  -> ByteString
  -> EffectFnAff Int
foreign import mgetImpl
  :: Connection
  -> Array ByteString
  -> EffectFnAff (Array ByteString)
foreign import setImpl
  :: Connection
  -> ByteString
  -> ByteString
  -> Nullable { unit :: ByteString, value :: Int }
  -> Nullable ByteString
  -> EffectFnAff Unit
-- | ZADD key [NX|XX] [CH]
-- | INCR mode would be supported by `zaddIncrImpl`
foreign import zaddImpl
  :: Connection
  -> ByteString
  -> Nullable ByteString
  -> Nullable ByteString
  -> Array (Zitem Int53)
  -> EffectFnAff Int
foreign import zcardImpl
  :: Connection
  -> ByteString
  -> EffectFnAff Int
foreign import zincrbyImpl
  :: Connection
  -> ByteString
  -> Int53
  -> ByteString
  -> EffectFnAff Int53
foreign import zrangeImpl
  :: Connection
  -> ByteString
  -> Int
  -> Int
  -> EffectFnAff (Array (Zitem Int53))
foreign import zrangebyscoreImpl
  :: Connection
  -> ByteString
  -> ByteString
  -> ByteString
  -> Nullable Zlimit
  -> EffectFnAff (Array (Zitem Int53))
foreign import zrankImpl
  :: Connection
  -> ByteString
  -> ByteString
  -> EffectFnAff (Nullable Int)
foreign import zremImpl
  :: Connection
  -> ByteString
  -> Array ByteString
  -> EffectFnAff Int
foreign import zremrangebylexImpl
  :: Connection
  -> ByteString
  -> ByteString
  -> ByteString
  -> EffectFnAff Int
foreign import zremrangebyrankImpl
  :: Connection
  -> ByteString
  -> Int
  -> Int
  -> EffectFnAff Int
foreign import zremrangebyscoreImpl
  :: Connection
  -> ByteString
  -> ByteString
  -> ByteString
  -> EffectFnAff Int
foreign import zrevrangebyscoreImpl
  :: Connection
  -> ByteString
  -> ByteString
  -> ByteString
  -> Nullable Zlimit
  -> EffectFnAff (Array (Zitem Int53))
foreign import zscoreImpl
  :: Connection
  -> ByteString
  -> ByteString
  -> EffectFnAff (Nullable Int53)
blpop
  :: Connection
  -> NonEmpty Array ByteString
  -> Int
  -> Aff (Maybe {key ∷ ByteString, value ∷ ByteString})
blpop conn keys' timeout = toMaybe <$> (fromEffectFnAff $ blpopImpl conn (fromFoldable keys') timeout)
blpopIndef
  :: Connection
  -> NonEmpty Array ByteString
  -> Aff {key ∷ ByteString, value ∷ ByteString}
blpopIndef conn keys' = unsafeCoerce (fromEffectFnAff $ blpopImpl conn (fromFoldable keys') 0)
brpop
  :: Connection
  -> NonEmpty Array ByteString
  -> Int
  -> Aff (Maybe {key ∷ ByteString, value ∷ ByteString})
brpop conn keys' timeout = toMaybe <$> (fromEffectFnAff $ brpopImpl conn (fromFoldable keys') timeout)
brpopIndef
  :: Connection
  -> NonEmpty Array ByteString
  -> Aff {key ∷ ByteString, value ∷ ByteString}
brpopIndef conn keys' = unsafeCoerce (fromEffectFnAff $ brpopImpl conn (fromFoldable keys') 0)
del
  :: Connection
  -> NonEmpty Array ByteString
  -> Aff Unit
del conn = fromEffectFnAff <<< delImpl conn <<< fromFoldable
flushdb
  :: Connection
  -> Aff Unit
flushdb = fromEffectFnAff <<< flushdbImpl
get
  :: Connection
  -> ByteString
  -> Aff (Maybe ByteString)
get conn key = toMaybe <$> (fromEffectFnAff $ getImpl conn key)
hget
  :: Connection
  -> ByteString
  -> ByteString
  -> Aff (Maybe ByteString)
hget conn key field = toMaybe <$> (fromEffectFnAff $ hgetImpl conn key field)
hgetall
  :: Connection
  -> ByteString
  -> Aff (Array {key :: ByteString, value :: ByteString})
hgetall conn = fromEffectFnAff <<< hgetallImpl conn
hset
  :: Connection
  -> ByteString
  -> ByteString
  -> ByteString
  -> Aff Int
hset conn key field = fromEffectFnAff <<< hsetImpl conn key field
incr
  :: Connection
  -> ByteString
  -> Aff Int
incr conn = fromEffectFnAff <<< incrImpl conn
keys
  :: Connection
  -> ByteString
  -> Aff (Array ByteString)
keys conn = fromEffectFnAff <<< keysImpl conn
lpop
  :: Connection
  -> ByteString
  -> Aff (Maybe ByteString)
lpop conn key = toMaybe <$> (fromEffectFnAff $ lpopImpl conn key)
lpush
  :: Connection
  -> ByteString
  -> ByteString
  -> Aff Int
lpush conn key = fromEffectFnAff <<< lpushImpl conn key
lrange
  :: Connection
  -> ByteString
  -> Int
  -> Int
  -> Aff (Array ByteString)
lrange conn key start = fromEffectFnAff <<< lrangeImpl conn key start
ltrim
  :: Connection
  -> ByteString
  -> Int
  -> Int
  -> Aff Unit
ltrim conn key start end = unit <$ (fromEffectFnAff $ ltrimImpl conn key start end)
mget
  :: Connection
  -> NonEmpty Array ByteString
  -> Aff (Array ByteString)
mget conn = fromEffectFnAff <<< mgetImpl conn <<< fromFoldable
rpop
  :: Connection
  -> ByteString
  -> Aff (Maybe ByteString)
rpop conn key = toMaybe <$> (fromEffectFnAff $ rpopImpl conn key)
rpush
  :: Connection
  -> ByteString
  -> ByteString
  -> Aff Int
rpush conn key = fromEffectFnAff <<< rpushImpl conn key
set
  :: Connection
  -> ByteString
  -> ByteString
  -> Maybe Expire
  -> Maybe Write
  -> Aff Unit
set conn key value expire write = fromEffectFnAff $ setImpl conn key value expire' write'
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
  :: ∀ i
   . Int53Value i
  => Connection
  -> ByteString
  -> Zadd
  -> NonEmpty Array (Zitem i)
  -> Aff Int
zadd conn key mode =
  fromEffectFnAff <<< zaddImpl conn key write' return' <<< map (\r → r { score = toInt53 r.score }) <<< fromFoldable
  where
  Tuple write' return' = case mode of
    ZaddAll Changed -> Tuple (toNullable Nothing) (toNullable $ Just (toUTF8 "CH"))
    ZaddAll Added -> Tuple (toNullable Nothing) (toNullable $ Nothing)
    ZaddRestrict XX -> Tuple (toNullable $ Just (serWrite XX)) (toNullable $ Just (toUTF8 "CH"))
    ZaddRestrict NX -> Tuple (toNullable $ Just (serWrite NX)) (toNullable Nothing)

zcard
  :: Connection
  -> ByteString
  -> Aff Int
zcard conn = fromEffectFnAff <<< zcardImpl conn

zincrby
  :: ∀ i
   . Int53Value i
  => Connection
  -> ByteString
  -> i
  -> ByteString
  -> Aff Int53
zincrby conn key increment = fromEffectFnAff <<< zincrbyImpl conn key (toInt53 increment)

zrange
  :: Connection
  -> ByteString
  -> Int
  -> Int
  -> Aff (Array (Zitem Int53))
zrange conn key start = fromEffectFnAff <<< zrangeImpl conn key start

zrangebyscore
  :: ∀ max min
   . Int53Value min
  => Int53Value max
  => Connection
  -> ByteString
  -> (ZscoreInterval min)
  -> (ZscoreInterval max)
  -> Maybe Zlimit
  -> Aff (Array (Zitem Int53))
zrangebyscore conn key min max limit = fromEffectFnAff $ zrangebyscoreImpl conn key min' max' limit'
  where
  min' = serZscoreInterval min
  max' = serZscoreInterval max
  limit' = toNullable limit

zrank
  :: Connection
  -> ByteString
  -> ByteString
  -> Aff (Maybe Int)
zrank conn key member = toMaybe <$> (fromEffectFnAff $ zrankImpl conn key member)

zrem
  :: Connection
  -> ByteString
  -> NonEmpty Array ByteString
  -> Aff Int
zrem conn key = fromEffectFnAff <<< zremImpl conn key <<< fromFoldable

zremrangebylex
  :: Connection
  -> ByteString
  -> ByteString
  -> ByteString
  -> Aff Int
zremrangebylex conn key min = fromEffectFnAff <<< zremrangebylexImpl conn key min
zremrangebyrank
  :: Connection
  -> ByteString
  -> Int
  -> Int
  -> Aff Int
zremrangebyrank conn key min = fromEffectFnAff <<< zremrangebyrankImpl conn key min
zremrangebyscore
  :: ∀ max min
   . Int53Value min
  => Int53Value max
  => Connection
  -> ByteString
  -> (ZscoreInterval min)
  -> (ZscoreInterval max)
  -> Aff Int
zremrangebyscore conn key min max = fromEffectFnAff $ zremrangebyscoreImpl conn key min' max'
  where
  min' = serZscoreInterval min
  max' = serZscoreInterval max
zrevrangebyscore
  :: ∀ max min
   . Int53Value min
  => Int53Value max
  => Connection
  -> ByteString
  -> (ZscoreInterval min)
  -> (ZscoreInterval max)
  -> Maybe Zlimit
  -> Aff (Array (Zitem Int53))
zrevrangebyscore conn key min max limit = fromEffectFnAff $ zrevrangebyscoreImpl conn key min' max' limit'
  where
  min' = serZscoreInterval min
  max' = serZscoreInterval max
  limit' = toNullable limit

zscore
  :: Connection
  -> ByteString
  -> ByteString
  -> Aff (Maybe Int53)
zscore conn key = (toMaybe <$> _) <<< fromEffectFnAff <<< zscoreImpl conn key

type ScanStreamOptions = 
  ( count :: Int 
  , match :: String 
  )

foreign import scanStreamImpl :: forall opts. 
  Connection 
  -> Record opts 
  -> (Array String -> Readable () -> Tuple (Array String) (Readable ())) 
  -> EffectFnAff (Tuple (Array String) (Readable ()))

foreign import hscanStreamImpl :: forall opts. 
  Connection 
  -> Record opts 
  -> String 
  -> (Array {key :: String, value :: String} -> Readable () -> Tuple (Array {key :: String, value :: String}) (Readable ())) 
  -> EffectFnAff (Tuple (Array {key :: String, value :: String}) (Readable ())) 

foreign import zscanStreamImpl :: forall opts. 
  Connection 
  -> Record opts 
  -> String 
  -> (Array {member :: String, score :: Int} -> Readable () -> Tuple (Array {member :: String, score :: Int}) (Readable ())) 
  -> EffectFnAff (Tuple (Array {member :: String, score :: Int}) (Readable ())) 

scanStream :: forall options t. 
  Union options t ScanStreamOptions 
  => Connection 
  -> Record options 
  -> Aff (Tuple (Array String) (Readable ()))
scanStream redis options = fromEffectFnAff $ scanStreamImpl redis options Tuple

hscanStream :: forall options t. 
  Union options t ScanStreamOptions 
  => Connection 
  -> Record options 
  -> String
  -> Aff (Tuple (Array {key :: String, value :: String}) (Readable ()))
hscanStream redis options hash = fromEffectFnAff $ hscanStreamImpl redis options hash Tuple

zscanStream :: forall options t. 
  Union options t ScanStreamOptions 
  => Connection 
  -> Record options 
  -> String
  -> Aff (Tuple (Array {member :: String, score :: Int}) (Readable ()))
zscanStream redis options key = fromEffectFnAff $ zscanStreamImpl redis options key Tuple