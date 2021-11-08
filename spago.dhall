{-
Welcome to a Spago project!
You can edit this file as you like.
-}
{ name = "redis-client"
, dependencies =
  [ "aff"
  , "arrays"
  , "bytestrings"
  , "effect"
  , "foldable-traversable"
  , "int-53"
  , "maybe"
  , "nonempty"
  , "nullable"
  , "prelude"
  , "psci-support"
  , "test-unit"
  , "transformers"
  , "tuples"
  , "unsafe-coerce"
  ]
, license = "BSD-3-Clause"
, packages = ./packages.dhall
, repository = "https://github.com/paluh/purescript-redis-client.git"
, sources = [ "src/**/*.purs", "test/**/*.purs" ]
}
