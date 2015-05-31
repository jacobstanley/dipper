{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS_GHC -w #-}

module Dipper where

import           Control.Exception (bracket, catch)
import           Data.Binary.Get
import           Data.Binary.Put
import           Data.Bits ((.|.), shiftL, xor)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as S
import qualified Data.ByteString.Lazy.Char8 as L
import           Data.Int (Int8, Int32, Int64)
import           Data.Map (Map)
import qualified Data.Map as M
import           Data.Monoid ((<>))
import           Data.ReinterpretCast (floatToWord, doubleToWord)
import           Data.ReinterpretCast (wordToFloat, wordToDouble)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Text.Lazy as LT
import qualified Data.Text.Lazy.IO as LT
import           Data.Tuple (swap)
import           Data.Word (Word8)
import           System.Directory (getTemporaryDirectory, removeFile)
import           System.Environment (getArgs, getExecutablePath)
import           System.Exit (ExitCode)
import           System.FilePath (takeFileName)
import           System.IO (openBinaryTempFileWithDefaultPermissions, hClose)

import           Pipes
import qualified Pipes.Group as P
import qualified Pipes.Prelude as P
import qualified Pipes.Text as PT
import qualified Pipes.Text.IO as PT
import           System.Process.Streaming

import           Dipper.Jar (dipperJar)

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

withTempFile :: String -> S.ByteString -> (FilePath -> IO a) -> IO a
withTempFile name content action = do
    tmp <- getTemporaryDirectory
    bracket (openBinaryTempFileWithDefaultPermissions tmp name)
            (\(p, h) -> hClose h >> ignoreIOErrors (removeFile p))
            (\(p, h) -> S.hPut h content >> hClose h >> action p)
  where
    ignoreIOErrors ioe = ioe `catch` (\e -> const (return ()) (e :: IOError))

------------------------------------------------------------------------

runJob :: HadoopEnv -> IO ExitCode
runJob hadoopEnv = withTempFile "dipper.jar" dipperJar $ \jar -> do
    self <- getExecutablePath

    putStrLn "Arguments:"
    mapM_ putStrLn (mkArgs self jar)

    (code, _) <- execute (pipeoec stdout stderr (fromConsumer PT.stdout))
                         (program self jar)
    return code
  where
    linesUtf8 = toLines decodeUtf8 (pure id)
    stdout    = tweakLines (yield "stdout> " *>) linesUtf8
    stderr    = tweakLines (yield "stderr> " *>) linesUtf8

    program self jar = proc (hadoopExec hadoopEnv)
                            (mkArgs self jar)

    mkArgs  self jar =
        [ "jar", streamingJar hadoopEnv
        , "-files", self
        , "-libjars", jar

        , "-D", "mapreduce.job.name=dipper"

        , "-D", "stream.io.identifier.resolver.class=org.dipper.DipperResolver"
        , "-D", "stream.map.input=map"
        , "-D", "stream.map.output=text"
        --, "-D", "stream.reduce.input=reduce"
        --, "-D", "stream.reduce.output=reduce"

        , "-inputformat",  "org.apache.hadoop.streaming.AutoInputFormat"
        , "-input", "/user/root/features"

        --, "-outputformat", "org.apache.hadoop.mapred.SequenceFileOutputFormat"
        , "-outputformat", "org.apache.hadoop.mapred.TextOutputFormat"
        , "-output", "/user/root/foo"

        , "-numReduceTasks", "0"
        , "-mapper", takeFileName self <> " " <> "0-mapper"
        ]

------------------------------------------------------------------------

mapper :: IO ()
mapper = L.interact (writeKVs . readKVs)

writeKVs :: [(T.Text, S.ByteString)] -> L.ByteString
writeKVs = L.unlines . map go
  where
    go (k, v) = L.fromStrict (T.encodeUtf8 k)
             <> "\t"
             <> L.pack (show (S.length v))

readKVs :: L.ByteString -> [(T.Text, S.ByteString)]
readKVs bs | L.null bs = []
           | otherwise = decode
  where
    decode = case runGetOrFail getKV bs of
      Left  (_,   _, err)    -> error ("readKVs: " ++ err)
      Right (bs', _, (k, v)) -> (k, v) : readKVs bs'

getKV :: Get (T.Text, S.ByteString)
getKV = (,) <$> getText <*> getBytesWritable

------------------------------------------------------------------------

getText :: Get T.Text
getText = T.decodeUtf8 <$> (getVInt >>= getByteString)

getBytesWritable :: Get S.ByteString
getBytesWritable = getWord32be >>= getByteString . fromIntegral

putBytesWritable :: S.ByteString -> Put
putBytesWritable bs = do
    putWord32be (fromIntegral (S.length bs))
    putByteString bs

getVInt :: Get Int
getVInt = fromIntegral <$> getVInt64

getVInt64 :: Get Int64
getVInt64 = withFirst . fromIntegral =<< getWord8
  where
    withFirst :: Int8 -> Get Int64
    withFirst x | size == 1 = return (fromIntegral x)
                | otherwise = fixupSign . B.foldl' go 0 <$> getByteString (size - 1)
      where
        go :: Int64 -> Word8 -> Int64
        go i b = (i `shiftL` 8) .|. fromIntegral b

        size | x >= -112 = 1
             | x <  -120 = fromIntegral (-119 - x)
             | otherwise = fromIntegral (-111 - x)

        fixupSign v = if isNegative then v `xor` (-1) else v

        isNegative = x < -120 || (x >= -112 && x < 0)

------------------------------------------------------------------------

dipperMain :: IO ()
dipperMain = do
    args <- getArgs
    case args of
      []           -> runJob cloudera >>= print
      ["0-mapper"] -> mapper
      _            -> putStrLn "error: Run with no arguments to execute Hadoop job"

