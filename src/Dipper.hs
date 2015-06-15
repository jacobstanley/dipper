module Dipper (
      HadoopEnv(..)
    , dipperMain
    , cloudera
    ) where

import           System.Environment (getArgs)

import           Dipper.Internal
import           Dipper.Jar (withDipperJar)

------------------------------------------------------------------------

dipperMain :: HadoopEnv -> IO ()
dipperMain env = do
    args <- getArgs
    case args of
      []            -> withDipperJar (runJob env) >>= print
      ["0-mapper"]  -> mapper
      ["0-reducer"] -> reducer
      _             -> putStrLn "error: Run with no arguments to execute Hadoop job"
