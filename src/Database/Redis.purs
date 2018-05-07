module Database.Redis
  ( REDIS
  , Connection
  , Expire(..)
  , Write(..)

  , connect
  , disconnect
  , withConnection

  , del
  , set
  , flushdb
  , get
  , incr
  , keys
  , mget
  ) where

import Prelude

import Control.Monad.Aff (Aff, bracket)
import Control.Monad.Aff.Compat (EffFnAff, fromEffFnAff)
import Control.Monad.Eff (kind Effect)
import Data.ByteString (ByteString)
import Data.Maybe (Maybe)
import Data.Nullable (Nullable, toNullable)

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

foreign import delImpl  :: ∀ eff. Connection -> Array ByteString -> EffFnAff (redis :: REDIS | eff) Unit
foreign import flushdbImpl :: ∀ eff. Connection -> EffFnAff (redis :: REDIS | eff) Unit
foreign import setImpl
  :: ∀ eff
  . Connection
  -> ByteString
  -> ByteString
  -> Nullable { unit :: String, value :: Int }
  -> Nullable String
  -> EffFnAff (redis :: REDIS | eff) Unit
foreign import getImpl  :: ∀ eff. Connection -> ByteString -> EffFnAff (redis :: REDIS | eff) (Maybe ByteString)
foreign import incrImpl :: ∀ eff. Connection -> ByteString -> EffFnAff (redis :: REDIS | eff) Int
foreign import keysImpl :: ∀ eff. Connection -> ByteString -> EffFnAff (redis :: REDIS | eff) (Array ByteString)
foreign import mgetImpl :: ∀ eff. Connection -> Array ByteString -> EffFnAff (redis :: REDIS | eff) (Array ByteString)

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
  serExpire (PX v) = { unit: "PX", value: v }
  serExpire (EX v) = { unit: "EX", value: v }
  serWrite NX = "NX"
  serWrite XX = "XX"
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

