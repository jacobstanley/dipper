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
import           Data.Int (Int64)
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
    mapM_ printArg (mkArgs self)

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

    printArg "-D" = return ()
    printArg arg  = putStrLn arg

    program self = proc (hadoopExec hadoopEnv) (mkArgs self)
    mkArgs  self =
        [ "jar", streamingJar hadoopEnv
        , "-files", self
        , "-libjars", dipperJarPath

        --, "-D", "mapreduce.job.name=dipper"

        , "-D", "mapred.output.key.comparator.class=org.dipper.DipperComparator"

        , "-D", "stream.io.identifier.resolver.class=org.dipper.DipperResolver"
        , "-D", "stream.map.input=map"
        , "-D", "stream.map.output=map"
        , "-D", "stream.reduce.input=reduce"
        , "-D", "stream.reduce.output=reduce"

        , "-D", "dipper.shuffle.0.key=org.apache.io.LongWritable"
        , "-D", "dipper.shuffle.0.value=org.apache.io.Text"

        , "-D", "dipper.shuffle.1.key=org.apache.io.Text"
        , "-D", "dipper.shuffle.1.value=org.apache.io.LongWritable"

        , "-D", "dipper.output.0.key=org.apache.io.LongWritable"
        , "-D", "dipper.output.0.value=org.apache.io.LongWritable"
        , "-D", "dipper.output.0.format=org.apache.hadoop.mapred.TextOutputFormat"
        , "-D", "dipper.output.0.path=/user/root/test-output-counts"

        , "-D", "dipper.output.1.key=org.apache.io.Text"
        , "-D", "dipper.output.1.value=org.apache.io.LongWritable"
        , "-D", "dipper.output.1.format=org.apache.hadoop.mapred.SequenceFileOutputFormat"
        , "-D", "dipper.output.1.compress=true"
        , "-D", "dipper.output.1.compression.codec=org.apache.hadoop.io.compress.SnappyCodec"
        , "-D", "dipper.output.1.compression.type=BLOCK"
        , "-D", "dipper.output.1.path=/user/root/test-output-sizes"

        --, "-D", "mapred.output.compress=true"
        --, "-D", "mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec"
        --, "-D", "mapred.output.compression.type=BLOCK"

        , "-inputformat",  "org.apache.hadoop.streaming.AutoInputFormat"
        --, "-inputformat", "org.apache.hadoop.mapred.SequenceFileInputFormat"
        , "-input", "/user/root/features"

        , "-outputformat", "org.dipper.DipperOutputFormat"
        --, "-outputformat", "org.apache.hadoop.mapred.TextOutputFormat"
        --, "-outputformat", "org.apache.hadoop.mapred.SequenceFileOutputFormat"
        --, "-output", "/user/root/test001"

        , "-mapper", takeFileName self <> " " <> "0-mapper"
        , "-reducer", "NONE"
        ]

------------------------------------------------------------------------

mapper :: IO ()
mapper = L.interact (mapperWrite . concatMap go . mapperRead)
  where
    go :: (T.Text, B.ByteString) -> [Either (Int, T.Text) (T.Text, Int)]
    go (txt, bs) = [ Left  (B.length bs, txt)
                   , Right (txt, B.length bs) ]

mapperRead :: L.ByteString -> [(T.Text, B.ByteString)]
mapperRead bs | L.null bs = []
              | otherwise = case runGetOrFail getKV bs of
                  Left  (_,   _, err) -> error ("mapperRead: " ++ err)
                  Right (bs', _, x)   -> x : mapperRead bs'

getKV :: Get (T.Text, B.ByteString)
getKV = (,) <$> getText <*> getBytesWritable

mapperWrite :: [Either (Int, T.Text) (T.Text, Int)] -> L.ByteString
mapperWrite = L.concat . map (runPut . putTagged)
  where
    putTagged t = case t of
        Left (k, v) -> do
            -- shuffle.0
            putVInt 0
            putWord64be (fromIntegral k)
            putText v

        Right (k, v) -> do
            -- shuffle.1
            putVInt 1
            putText k
            putWord64be (fromIntegral v)

------------------------------------------------------------------------

reducer :: IO ()
reducer = L.interact (reducerWrite . map go . reducerRead)
  where
    go :: Either (Int, T.Text) (T.Text, Int) -> Either (Int, Int) (T.Text, Int)
    go t = case t of
        Left  (sz, txt) -> Left  (sz, 1)    -- TODO fold counts
        Right (txt, sz) -> Right (txt, sz)  -- TODO fold sizes

reducerRead :: L.ByteString -> [Either (Int, T.Text) (T.Text, Int)]
reducerRead bs | L.null bs = []
               | otherwise = case runGetOrFail getTagged bs of
                   Left  (_,   _, err) -> error ("mapperRead: " ++ err)
                   Right (bs', _, x)   -> x : reducerRead bs'

getTagged :: Get (Either (Int, T.Text) (T.Text, Int))
getTagged = do
    tag <- getVInt
    case tag of
        0 -> Left  <$> ((,) <$> (fromIntegral <$> getWord32be)
                            <*> getText)

        1 -> Right <$> ((,) <$> getText
                            <*> (fromIntegral <$> getWord32be))

        _ -> error "getTagged: unknown tag"

reducerWrite :: [Either (Int, Int) (T.Text, Int)] -> L.ByteString
reducerWrite = L.concat . map (runPut . putTagged)
  where
    putTagged t = case t of
        Left (k, v) -> do
            -- output.0
            putVInt 0
            putWord64be (fromIntegral k)
            putWord64be (fromIntegral v)

        Right (k, v) -> do
            -- output.1
            putVInt 1
            putText k
            putWord64be (fromIntegral v)
