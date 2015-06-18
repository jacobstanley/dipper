module Dipper.Hadoop.Environment where

import System.FilePath.Posix ((</>))

------------------------------------------------------------------------

data HadoopEnv = HadoopEnv {
    hadoopHome :: FilePath
  , hadoopExec :: FilePath
  } deriving (Eq, Ord, Read, Show)

hadoopStreamingJar :: HadoopEnv -> FilePath
hadoopStreamingJar henv = hadoopHome henv </> "contrib/streaming/hadoop-streaming.jar"

------------------------------------------------------------------------

clouderaEnv :: HadoopEnv
clouderaEnv = HadoopEnv "/usr/lib/hadoop-0.20-mapreduce" "hadoop"
