{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Dipper.Internal where

import           Data.Binary.Get
import           Data.Binary.Put
import qualified Data.ByteString.Char8 as S
import qualified Data.ByteString.Lazy.Char8 as L
import           Data.Monoid ((<>))
import qualified Data.Text as T
import           System.Environment (getExecutablePath)
import           System.Exit (ExitCode)
import           System.FilePath (takeFileName)

import           Pipes
import qualified Pipes.Text.IO as PT
import           System.Process.Streaming hiding (env)

import           Dipper.Binary

------------------------------------------------------------------------

data HadoopEnv = HadoopEnv {
    hadoopHome :: FilePath
  , hadoopExec :: FilePath
  } deriving (Eq, Ord, Read, Show)

------------------------------------------------------------------------

cloudera :: HadoopEnv
cloudera = HadoopEnv "/usr/lib/hadoop-0.20-mapreduce" "hadoop"

streamingJar :: HadoopEnv -> FilePath
streamingJar env = hadoopHome env <> "/contrib/streaming/hadoop-streaming.jar"

------------------------------------------------------------------------

runJob :: HadoopEnv -> FilePath -> IO ExitCode
runJob hadoopEnv dipperJarPath = do
    self <- getExecutablePath

    putStrLn "Arguments:"
    mapM_ putStrLn (mkArgs self)

    (code, _) <- execute (pipeoec stdout stderr (fromConsumer PT.stdout))
                         (program self)
    return code
  where
    linesUtf8 = toLines decodeUtf8 (pure id)
    stdout    = tweakLines (yield "stdout> " *>) linesUtf8
    stderr    = tweakLines (yield "stderr> " *>) linesUtf8

    program self = proc (hadoopExec hadoopEnv)
                        (mkArgs self)

    mkArgs  self =
        [ "jar", streamingJar hadoopEnv
        , "-files", self
        , "-libjars", dipperJarPath

        --, "-D", "mapreduce.job.name=dipper"

        , "-D", "stream.io.identifier.resolver.class=org.dipper.DipperResolver"
        , "-D", "stream.map.input=map"
        , "-D", "stream.map.output=map"
        , "-D", "stream.reduce.input=reduce"
        , "-D", "stream.reduce.output=reduce"

        , "-D", "mapred.output.compress=true"
        , "-D", "mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec"
        , "-D", "mapred.output.compression.type=BLOCK"
        --, "-D", "mapreduce.output.fileoutputformat.compress=true"
        --, "-D", "mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec"
        --, "-D", "mapreduce.output.fileoutputformat.compress.type=BLOCK"

        , "-inputformat",  "org.apache.hadoop.streaming.AutoInputFormat"
        --, "-inputformat", "org.apache.hadoop.mapred.SequenceFileInputFormat"
        , "-input", "/user/root/features"

        --, "-outputformat", "org.apache.hadoop.mapred.TextOutputFormat"
        , "-outputformat", "org.apache.hadoop.mapred.SequenceFileOutputFormat"
        , "-output", "/user/root/test001"

        , "-mapper", takeFileName self <> " " <> "0-mapper"
        , "-reducer", "NONE"
        ]

------------------------------------------------------------------------

mapper :: IO ()
mapper = L.interact (writeKVs . readKVs)

writeKVs :: [(T.Text, S.ByteString)] -> L.ByteString
writeKVs = L.concat . map (runPut . putKV)
  where
    putKV (k, v) = do
      putText k
      putBytesWritable (S.pack (show (S.length v)))

readKVs :: L.ByteString -> [(T.Text, S.ByteString)]
readKVs bs | L.null bs = []
           | otherwise = case runGetOrFail getKV bs of
               Left  (_,   _, err)    -> error ("readKVs: " ++ err)
               Right (bs', _, (k, v)) -> (k, v) : readKVs bs'

getKV :: Get (T.Text, S.ByteString)
getKV = (,) <$> getText <*> getBytesWritable
