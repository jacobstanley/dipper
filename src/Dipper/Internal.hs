{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

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
import           Data.List (groupBy, foldl')
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

import           Debug.Trace

------------------------------------------------------------------------

data HadoopEnv = HadoopEnv {
    hadoopHome :: FilePath
  , hadoopExec :: FilePath
  } deriving (Eq, Ord, Read, Show)

------------------------------------------------------------------------

cloudera :: HadoopEnv
cloudera = HadoopEnv "/usr/lib/hadoop-0.20-mapreduce" "hadoop"

streamingJar :: HadoopEnv -> FilePath
streamingJar henv = hadoopHome henv <> "/contrib/streaming/hadoop-streaming.jar"

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
        --, "-D", "mapred.max.tracker.failures=1"

        , "-D", "mapred.output.key.comparator.class=org.dipper.TagKeyComparator"

        , "-D", "stream.io.identifier.resolver.class=org.dipper.DipperResolver"
        , "-D", "stream.map.input=map"
        , "-D", "stream.map.output=map"
        , "-D", "stream.reduce.input=reduce"
        , "-D", "stream.reduce.output=reduce"

        , "-D", "dipper.tag.0.key=org.apache.hadoop.io.LongWritable"
        , "-D", "dipper.tag.0.value=org.apache.hadoop.io.Text"

        , "-D", "dipper.tag.1.key=org.apache.hadoop.io.Text"
        , "-D", "dipper.tag.1.value=org.apache.hadoop.io.LongWritable"

        , "-D", "dipper.tag.2.key=org.apache.hadoop.io.LongWritable"
        , "-D", "dipper.tag.2.value=org.apache.hadoop.io.LongWritable"
        , "-D", "dipper.tag.2.format=org.apache.hadoop.mapred.TextOutputFormat"
        , "-D", "dipper.tag.2.path=/user/root/test-output-counts"

        , "-D", "dipper.tag.3.key=org.apache.hadoop.io.Text"
        , "-D", "dipper.tag.3.value=org.apache.hadoop.io.LongWritable"
        , "-D", "dipper.tag.3.format=org.apache.hadoop.mapred.SequenceFileOutputFormat"
        , "-D", "dipper.tag.3.compress=true"
        , "-D", "dipper.tag.3.compression.codec=org.apache.hadoop.io.compress.SnappyCodec"
        , "-D", "dipper.tag.3.compression.type=BLOCK"
        , "-D", "dipper.tag.3.path=/user/root/test-output-sizes"

        --, "-D", "mapred.output.compress=true"
        --, "-D", "mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec"
        --, "-D", "mapred.output.compression.type=BLOCK"

        , "-inputformat",  "org.apache.hadoop.streaming.AutoInputFormat"
        --, "-inputformat", "org.apache.hadoop.mapred.SequenceFileInputFormat"
        , "-input", "/user/root/features"

        , "-outputformat", "org.dipper.DipperOutputFormat"
        --, "-outputformat", "org.apache.hadoop.mapred.TextOutputFormat"
        --, "-outputformat", "org.apache.hadoop.mapred.SequenceFileOutputFormat"
        , "-output", "/user/root/test-dummy"

        , "-mapper",  takeFileName self <> " " <> "0-mapper"
        , "-reducer", takeFileName self <> " " <> "0-reducer"
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
            -- tag.0
            putVInt 0
            putWord64be (fromIntegral k)
            putText v

        Right (k, v) -> do
            -- tag.1
            putVInt 1
            putText k
            putWord64be (fromIntegral v)

------------------------------------------------------------------------

reducer :: IO ()
reducer = L.interact $ reducerWrite
                     . map combine
                     . groupBy tagKeyEq
                     . reducerRead L.empty L.empty L.empty
                     . showBegin
  where
    tagKeyEq :: Either (Int, T.Text) (T.Text, Int)
             -> Either (Int, T.Text) (T.Text, Int)
             -> Bool
    tagKeyEq (Left  (x, _)) (Left  (y, _)) = x == y
    tagKeyEq (Right (x, _)) (Right (y, _)) = x == y
    tagKeyEq _              _              = False

    combine :: [Either (Int, T.Text) (T.Text, Int)] -> Either (Int, Int) (T.Text, Int)
    combine xs@(Left  (sz,  _) : _) = Left  (sz,  foldCounts xs)
    combine xs@(Right (txt, _) : _) = Right (txt, foldSizes  xs)

    foldCounts :: [Either (Int, T.Text) (T.Text, Int)] -> Int
    foldCounts = foldl' go 0
      where
        go acc (Left _)  = acc + 1
        go _   (Right _) = error "reducer.foldCounts: invalid grouping"

    foldSizes :: [Either (Int, T.Text) (T.Text, Int)] -> Int
    foldSizes = foldl' go 0
      where
        go acc (Right (_, sz)) = acc + sz
        go _   (Left  _)       = error "reducer.foldSizes: invalid grouping"

reducerRead :: L.ByteString -> L.ByteString -> L.ByteString -> L.ByteString -> [Either (Int, T.Text) (T.Text, Int)]
reducerRead prev0 prev1 prev2 bs
    | L.null bs = []
    | otherwise = case runGetOrFail getTagged bs of
        Right (bs', _, x)   -> x : reducerRead prev1 prev2 bs bs'
        Left  (_,   _, err) -> error ("reducerRead: " ++ err ++ "\n"
                                   ++ "previous bytes = " ++ show (L.take 100 prev0))

showBegin :: L.ByteString -> L.ByteString
showBegin bs = trace bs' bs
  where
    bs' = "Start bytes = " ++ show (L.take 100 bs)

getTagged :: Get (Either (Int, T.Text) (T.Text, Int))
getTagged = do
    tag <- getVInt
    case tag of
        0 -> Left  <$> ((,) <$> (fromIntegral <$> getWord64be)
                            <*> getText)

        1 -> Right <$> ((,) <$> getText
                            <*> (fromIntegral <$> getWord64be))

        _ -> do
            xs <- getByteString 100
            fail ("getTagged: unknown tag = " ++ show tag ++ ", next bytes = " ++ show xs)

reducerWrite :: [Either (Int, Int) (T.Text, Int)] -> L.ByteString
reducerWrite = L.concat . map (runPut . putTagged)
  where
    putTagged t = case t of
        Left (k, v) -> do
            -- tag.2
            putVInt 2
            putWord64be (fromIntegral k)
            putWord64be (fromIntegral v)

        Right (k, v) -> do
            -- tag.3
            putVInt 3
            putText k
            putWord64be (fromIntegral v)
