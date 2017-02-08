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
  ) where

import Control.Monad.Aff (Aff)
import Control.Monad.Error.Class (class MonadError, catchError, throwError)
import Data.ByteString (ByteString)
import Data.Either (Either(..), either)
import Data.Maybe (Maybe)
import Prelude

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

foreign import connect :: ∀ eff. String -> Aff (redis :: REDIS | eff) Connection
foreign import disconnect :: ∀ eff. Connection -> Aff (redis :: REDIS | eff) Unit

--------------------------------------------------------------------------------

foreign import del  :: ∀ eff. Connection -> Array ByteString -> Aff (redis :: REDIS | eff) Unit
foreign import set  :: ∀ eff. Connection -> ByteString -> ByteString -> Aff (redis :: REDIS | eff) Unit
foreign import get  :: ∀ eff. Connection -> ByteString -> Aff (redis :: REDIS | eff) (Maybe ByteString)
foreign import incr :: ∀ eff. Connection -> ByteString -> Aff (redis :: REDIS | eff) Int

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
