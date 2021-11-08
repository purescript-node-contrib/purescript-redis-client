let mkPackage =
      https://raw.githubusercontent.com/purescript/package-sets/psc-0.13.0-20190626/src/mkPackage.dhall sha256:0b197efa1d397ace6eb46b243ff2d73a3da5638d8d0ac8473e8e4a8fc528cf57

let int-53 = mkPackage
  [ "aff"
  , "effect"
  , "integers"
  , "math"
  , "maybe"
  , "numbers"
  , "prelude"
  , "psci-support"
  , "quickcheck"
  , "quickcheck-laws"
  , "strings"
  , "test-unit"
  ]
  "https://github.com/paluh/purescript-int-53.git"
  "e4ebfe7edbf9f5275efec14e53d643b6cd505720"

let upstream =
      https://github.com/purescript/package-sets/releases/download/psc-0.14.4-20211030/packages.dhall sha256:5cd7c5696feea3d3f84505d311348b9e90a76c4ce3684930a0ff29606d2d816c

in  upstream
  with
    int-53 = int-53
