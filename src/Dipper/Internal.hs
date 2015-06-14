{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

{-# OPTIONS_GHC -w #-}

module Dipper.Internal where

import           Control.Concurrent.Async (Concurrently (..))
import           Data.Binary.Get
import           Data.Binary.Put
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy.Char8 as L
import           Data.Conduit
import           Data.Conduit.Binary (sourceHandle)
import qualified Data.Conduit.List as CL
import qualified Data.Conduit.Text as CT
import           Data.Monoid ((<>))
import qualified Data.Text as T
import qualified Data.Text.IO as T
import           System.Environment (getExecutablePath)
import           System.Exit (ExitCode)
import           System.FilePath (takeFileName)
import           System.IO (Handle)
import           System.Process (StdStream(..), CreateProcess(..))
import           System.Process (proc, createProcess, waitForProcess)

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

    (Nothing, Just stdout, Just stderr, h) <-
        createProcess (program self) { std_out = CreatePipe
                                     , std_err = CreatePipe }

    runConcurrently
         $ Concurrently (output stdout "stdout")
        *> Concurrently (output stderr "stderr")

    waitForProcess h
  where
    output :: Handle -> T.Text -> IO ()
    output src name = do
        runConduit $ sourceHandle src
                 =$= CT.decodeUtf8
                 =$= CT.lines
                 =$= CL.mapM_ (\xs -> T.putStrLn (name <> "> " <> xs))

    program self = proc (hadoopExec hadoopEnv) (mkArgs self)
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

        , "-D", "dipper.shuffle.0.key=org.apache.io.Text"
        , "-D", "dipper.shuffle.0.value=org.apache.io.BytesWritable"

        , "-D", "dipper.output.0.key=org.apache.io.Text"
        , "-D", "dipper.output.0.value=org.apache.io.BytesWritable"
        , "-D", "dipper.output.0.path=/user/root/output1"

        --, "-D", "mapred.output.compress=true"
        --, "-D", "mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec"
        --, "-D", "mapred.output.compression.type=BLOCK"

        , "-inputformat",  "org.apache.hadoop.streaming.AutoInputFormat"
        --, "-inputformat", "org.apache.hadoop.mapred.SequenceFileInputFormat"
        , "-input", "/user/root/features"

        , "-outputformat", "org.dipper.DipperOutputFormat"
        --, "-outputformat", "org.apache.hadoop.mapred.TextOutputFormat"
        --, "-outputformat", "org.apache.hadoop.mapred.SequenceFileOutputFormat"
        , "-output", "/user/root/test001"

        , "-mapper", takeFileName self <> " " <> "0-mapper"
        , "-reducer", "NONE"
        ]

------------------------------------------------------------------------

mapper :: IO ()
mapper = L.interact (writeKVs . readKVs)

writeKVs :: [(T.Text, B.ByteString)] -> L.ByteString
writeKVs = L.concat . map (runPut . putKV)
  where
    putKV (k, v) = do
      putText k
      putBytesWritable (B.pack (show (B.length v)))

readKVs :: L.ByteString -> [(T.Text, B.ByteString)]
readKVs bs | L.null bs = []
           | otherwise = case runGetOrFail getKV bs of
               Left  (_,   _, err)    -> error ("readKVs: " ++ err)
               Right (bs', _, (k, v)) -> (k, v) : readKVs bs'

getKV :: Get (T.Text, B.ByteString)
getKV = (,) <$> getText <*> getBytesWritable
