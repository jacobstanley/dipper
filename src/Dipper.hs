module Dipper (
      HadoopEnv(..)
    , dipperMain
    ) where

import           Control.Exception (bracket, catch)
import qualified Data.ByteString.Char8 as S
import           System.Directory (getTemporaryDirectory, removeFile)
import           System.Environment (getArgs)
import           System.IO (openBinaryTempFileWithDefaultPermissions, hClose)

import           Dipper.Internal
import           Dipper.Jar (dipperJar)

------------------------------------------------------------------------

withTempFile :: String -> S.ByteString -> (FilePath -> IO a) -> IO a
withTempFile name content action = do
    tmp <- getTemporaryDirectory
    bracket (openBinaryTempFileWithDefaultPermissions tmp name)
            (\(p, h) -> hClose h >> ignoreIOErrors (removeFile p))
            (\(p, h) -> S.hPut h content >> hClose h >> action p)
  where
    ignoreIOErrors ioe = ioe `catch` (\e -> const (return ()) (e :: IOError))

------------------------------------------------------------------------

dipperMain :: HadoopEnv -> IO ()
dipperMain env = do
    args <- getArgs
    case args of
      []           -> withTempFile "dipper.jar" dipperJar (runJob env) >>= print
      ["0-mapper"] -> mapper
      _            -> putStrLn "error: Run with no arguments to execute Hadoop job"
