{-# LANGUAGE TemplateHaskell #-}

module Dipper.Jar
    ( dipperJar
    , withDipperJar
    ) where

import           Control.Exception (bracket, catch)
import           Data.ByteString (ByteString)
import qualified Data.ByteString as B
import           Data.FileEmbed (embedFile)
import           System.Directory (getTemporaryDirectory, removeFile)
import           System.IO (openBinaryTempFileWithDefaultPermissions, hClose)

------------------------------------------------------------------------

dipperJar :: ByteString
dipperJar = $(embedFile "jar/target/dipper.jar")

------------------------------------------------------------------------

withDipperJar :: (FilePath -> IO a) -> IO a
withDipperJar = withTempFile "dipper.jar" dipperJar

------------------------------------------------------------------------

withTempFile :: String -> B.ByteString -> (FilePath -> IO a) -> IO a
withTempFile name content action = do
    tmp <- getTemporaryDirectory
    bracket (openBinaryTempFileWithDefaultPermissions tmp name)
            (\(p, h) -> hClose h >> ignoreIOErrors (removeFile p))
            (\(p, h) -> B.hPut h content >> hClose h >> action p)
  where
    ignoreIOErrors ioe = ioe `catch` (\e -> const (return ()) (e :: IOError))

