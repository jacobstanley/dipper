{-# LANGUAGE TemplateHaskell #-}

module Dipper.Jar where

import Data.ByteString (ByteString)
import Data.FileEmbed (embedFile)

------------------------------------------------------------------------

dipperJar :: ByteString
dipperJar = $(embedFile "jar/target/dipper.jar")
