{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Dipper.Internal where

import           Data.Binary.Get
import           Data.Binary.Put
import           Data.Bits ((.|.), (.&.), shiftL, shiftR, xor)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as S
import qualified Data.ByteString.Lazy.Char8 as L
import           Data.Int (Int8, Int64)
import           Data.Monoid ((<>))
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import           Data.Word (Word8)
import           System.Environment (getExecutablePath)
import           System.Exit (ExitCode)
import           System.FilePath (takeFileName)

import           Pipes
import qualified Pipes.Text.IO as PT
import           System.Process.Streaming hiding (env)

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

------------------------------------------------------------------------

getText :: Get T.Text
getText = do
    len <- getVInt
    bs  <- getByteString len
    return (T.decodeUtf8With (onError bs) bs)
  where
    onError bs msg _ = error ("getText: could not decode " <> show bs <> " (" <> msg <> ")")

putText :: T.Text -> Put
putText tx = do
    putVInt (T.length tx)
    putByteString (T.encodeUtf8 tx)

getBytesWritable :: Get S.ByteString
getBytesWritable = getWord32be >>= getByteString . fromIntegral

putBytesWritable :: S.ByteString -> Put
putBytesWritable bs = do
    putWord32be (fromIntegral (S.length bs))
    putByteString bs

------------------------------------------------------------------------

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

putVInt :: Int -> Put
putVInt = putVInt64 . fromIntegral

putVInt64 :: Int64 -> Put
putVInt64 i | i >= -112 && i <= 127 = putWord8 (fromIntegral i)
            | otherwise             = putWord8 (fromIntegral encLen) >> putRest len
  where
    isNegative = i < 0

    i' | isNegative = i `xor` (-1)
       | otherwise  = i

    encLen0 | isNegative = -120
                | otherwise  = -112

    encLen = go i' encLen0
      where
        go 0   n = n
        go tmp n = go (tmp `shiftR` 8) (n-1)

    len | encLen < -120 = -(encLen + 120)
        | otherwise     = -(encLen + 112)

    putRest 0   = return ()
    putRest idx = putByte idx >> putRest (idx - 1)

    putByte idx = putWord8 (fromIntegral ((i .&. mask) `shiftR` shift))
      where
        mask :: Int64
        mask  = 0xff `shiftL` shift
        shift = (idx - 1) * 8
