module Database.Redis
  ( REDIS
  , Connection

  , connect
  , disconnect
  , withConnection

  , del
  , set
  , get
  , incr
  , keys
  ) where

import Prelude

import Control.Monad.Aff (Aff)
import Control.Monad.Aff.Compat (EffFnAff, fromEffFnAff)
import Control.Monad.Eff (kind Effect)
import Control.Monad.Error.Class (class MonadError, catchError, throwError)
import Data.ByteString (ByteString)
import Data.Either (Either(..), either)
import Data.Maybe (Maybe)

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

foreign import delImpl  :: ∀ eff. Connection -> Array ByteString -> EffFnAff (redis :: REDIS | eff) Unit
foreign import setImpl  :: ∀ eff. Connection -> ByteString -> ByteString -> EffFnAff (redis :: REDIS | eff) Unit
foreign import getImpl  :: ∀ eff. Connection -> ByteString -> EffFnAff (redis :: REDIS | eff) (Maybe ByteString)
foreign import incrImpl :: ∀ eff. Connection -> ByteString -> EffFnAff (redis :: REDIS | eff) Int
foreign import keysImpl :: ∀ eff. Connection -> ByteString -> EffFnAff (redis :: REDIS | eff) (Array ByteString)

del  :: ∀ eff. Connection -> Array ByteString -> Aff (redis :: REDIS | eff) Unit
del conn = fromEffFnAff <<< delImpl conn
set  :: ∀ eff. Connection -> ByteString -> ByteString -> Aff (redis :: REDIS | eff) Unit
set conn key = fromEffFnAff <<< setImpl conn key
get  :: ∀ eff. Connection -> ByteString -> Aff (redis :: REDIS | eff) (Maybe ByteString)
get conn = fromEffFnAff <<< getImpl conn
incr :: ∀ eff. Connection -> ByteString -> Aff (redis :: REDIS | eff) Int
incr conn = fromEffFnAff <<< incrImpl conn
keys :: ∀ eff. Connection -> ByteString -> Aff (redis :: REDIS | eff) (Array ByteString)
keys conn = fromEffFnAff <<< keysImpl conn

--------------------------------------------------------------------------------

bracket
  :: ∀ error monad resource result
   . (MonadError error monad)
  => monad resource
  -> (resource -> monad Unit)
  -> (resource -> monad result)
  -> monad result
bracket acquire release kleisli = do
  resource <- acquire
  result <- (Right <$> kleisli resource) `catchError` (pure <<< Left)
  release resource
  either throwError pure result
