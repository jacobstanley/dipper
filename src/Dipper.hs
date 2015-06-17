{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# OPTIONS_GHC -w #-}

module Dipper (
      HadoopEnv(..)
    , dipperMain
    , cloudera
    ) where

import           Control.Applicative ((<|>))
import           Control.Concurrent.Async (Concurrently(..))
import           Control.Monad (void, zipWithM_, foldM)
import           Data.Conduit ((=$=), runConduit, sequenceConduits)
import           Data.Conduit (Source, Sink, Consumer, Producer, Conduit)
import qualified Data.Conduit.Binary as CB
import qualified Data.Conduit.List as CL
import qualified Data.Conduit.Text as CT
import           Data.List (sort, isPrefixOf)
import           Data.Map (Map)
import qualified Data.Map as M
import           Data.Maybe (fromMaybe)
import           Data.Monoid ((<>))
import qualified Data.Text as T
import qualified Data.Text.IO as T
import           Data.Tuple.Strict (Pair(..))
import           System.Environment (getEnvironment)
import           System.Environment (getExecutablePath, getArgs, lookupEnv)
import           System.Exit (ExitCode(..), exitWith)
import           System.FilePath (takeFileName)
import           System.FilePath.Posix (takeDirectory)
import           System.IO (Handle, stdin, stdout, stderr)
import           System.Process (StdStream(..), CreateProcess(..))
import           System.Process (proc, createProcess, waitForProcess)

import           Dipper.Internal hiding (runJob)
import           Dipper.Interpreter
import           Dipper.Jar (withDipperJar)
import           Dipper.Types

------------------------------------------------------------------------

dipperMain :: (Ord n, Show n) => HadoopEnv -> FilePath -> Term n () -> IO ()
dipperMain henv jobDir term = do
    args <- getArgs
    case args of
      []          -> withDipperJar (\jar -> runJob henv jar jobDir pipeline)
      ["mapper"]  -> runMapper  pipeline
      ["reducer"] -> runReducer pipeline
      _           -> putStrLn "error: Run with no arguments to execute Hadoop job"
  where
    pipeline = mkPipeline jobDir term

------------------------------------------------------------------------

runJob :: HadoopEnv -> FilePath -> FilePath -> Pipeline -> IO ()
runJob henv jar jobDir pipeline = do
    print stages
    zipWithM_ go [1..] stages
  where
    stages = stagesOfPipeline pipeline

    go ix stage = runStage henv jar (jobDir ++ "/stage." ++ show ix) pipeline stage

------------------------------------------------------------------------

runStage :: HadoopEnv -> FilePath -> FilePath -> Pipeline -> Stage -> IO ()
runStage henv jar stageDir pipeline stage = do
    self <- getExecutablePath

    putStrLn "=== Arguments ==="
    mapM_ printArg (mkArgs self)

    (Nothing, Just out, Just err, h) <-
        createProcess (program self) { std_out = CreatePipe
                                     , std_err = CreatePipe }

    runConcurrently
         $ Concurrently (output out "stdout")
        *> Concurrently (output err "stderr")

    code <- waitForProcess h
    case code of
      ExitSuccess   -> return ()
      ExitFailure _ -> exitWith code
  where
    output :: Handle -> T.Text -> IO ()
    output src name = do
        runConduit $ CB.sourceHandle src
                 =$= CT.decodeUtf8
                 =$= CT.lines
                 =$= CL.mapM_ (\xs -> T.putStrLn (name <> "> " <> xs))

    printArg "-D" = return ()
    printArg arg  = putStrLn arg

    program self = proc (hadoopExec henv) (mkArgs self)
    mkArgs  self =
        [ "jar", streamingJar henv
        , "-files", self
        , "-libjars", jar

        -- , "-D", "mapred.reduce.tasks=300"
        , "-D", "mapred.output.key.comparator.class=org.dipper.TagKeyComparator"
        , "-D", "stream.io.identifier.resolver.class=org.dipper.DipperResolver"

        ] ++ stageArgs pipeline stage ++

        [ "-outputformat", "org.dipper.DipperOutputFormat"
        , "-output", stageDir

        , "-mapper",  takeFileName self <> " mapper"
        , "-reducer", takeFileName self <> " reducer"
        ]

stageArgs :: Pipeline -> Stage -> [String]
stageArgs p Stage{..} =
       shuffleArgs
    ++ outputArgs
    ++ ["-inputformat", "org.apache.hadoop.streaming.AutoInputFormat"]
    ++ inputArgs
  where
    inputArgs   = concatMap (\path -> ["-input", path]) (M.keys stageInputs)
    shuffleArgs = concatMap (uncurry kvArgs)            (M.toList stageShuffle)
    outputArgs  = concatMap (\(p,(t,f)) -> sequenceFileArgs p t f)
                . M.toList
                $ M.intersectionWith (,) (pathTags p) stageOutputs


kvArgs :: Tag -> KVFormat -> [String]
kvArgs tag (kFmt :!: vFmt) =
    [ "-D", "dipper.tag." ++ show tag ++ ".key="   ++ T.unpack (fmtType kFmt)
    , "-D", "dipper.tag." ++ show tag ++ ".value=" ++ T.unpack (fmtType vFmt) ]

textFileArgs ::  FilePath -> Tag -> KVFormat -> [String]
textFileArgs = outputFileArgs "org.apache.hadoop.mapred.TextOutputFormat"

sequenceFileArgs ::  FilePath -> Tag -> KVFormat -> [String]
sequenceFileArgs = outputFileArgs "org.apache.hadoop.mapred.SequenceFileOutputFormat"

outputFileArgs :: String -> FilePath -> Tag -> KVFormat -> [String]
outputFileArgs fileFormat path tag kvFormat =
    kvArgs tag kvFormat ++
    [ "-D", "dipper.tag." ++ show tag ++ ".format=" ++ fileFormat
    , "-D", "dipper.tag." ++ show tag ++ ".path="   ++ path ]

------------------------------------------------------------------------

runMapper :: Pipeline -> IO ()
runMapper Pipeline{..} = do
    inputFile <- stripHdfs . fromMaybe errorNoInput <$> foldM lookupEnv' Nothing inputFileVars

    let step     = lookupStep inputFile
        inSchema = fFormat (stepInput step)

    runConduit $ CB.sourceHandle stdin
             =$= decodeUnitRows inSchema
             =$= stepExec step
             =$= encodeTagRows outSchema
             =$= CB.sinkHandle stdout
  where
    lookupEnv' Nothing k = lookupEnv k
    lookupEnv' v       _ = return v

    inputFileVars = ["map_input_file", "mapreduce_map_input_file"]

    lookupStep :: FilePath -> Step FilePath Tag
    lookupStep input = fromMaybe (errorNoMapper input)
                     $ M.lookup (input)               pMappers
                   <|> M.lookup (takeDirectory input) pMappers

    outSchema :: Map Tag KVFormat
    outSchema = M.map (fFormat . stepInput) pReducers

    errorNoInput        = error ("runMapper: could not detect input file, " ++ show inputFileVars ++ " not set.")
    errorNoMapper input = error ("runMapper: could not find mapper for input file: " ++ input)

stripHdfs :: String -> FilePath
stripHdfs uri
    | hdfs `isPrefixOf` uri = dropWhile (/= '/') . drop (length hdfs) $ uri
    | otherwise             = uri
  where
    hdfs = "hdfs://"

------------------------------------------------------------------------

runReducer :: Pipeline -> IO ()
runReducer p@Pipeline{..} =
    runConduit $ CB.sourceHandle stdin
             =$= decodeTagRows inSchema
             =$= reduceAll
             =$= CL.map (mapTag fromPath)
             =$= encodeTagRows outSchema
             =$= CB.sinkHandle stdout
  where
    reduceAll :: Conduit (Row Tag) IO (Row FilePath)
    reduceAll = void
              . sequenceConduits
              . M.elems
              . M.mapWithKey (\tag r -> CL.filter (hasTag tag)
                                    =$= CL.map (withTag ())
                                    =$= stepExec r)
              $ pReducers

    fromPath :: FilePath -> Tag
    fromPath path = fromMaybe (pathError path) (M.lookup path tags)

    inSchema :: Map Tag KVFormat
    inSchema = M.map (fFormat . stepInput) pReducers

    outSchema :: Map Tag KVFormat
    outSchema = M.fromList
              . map (\x -> (fromPath (fName x), fFormat x))
              . concatMap stepOutputs
              . M.elems
              $ pReducers

    tags = pathTags p

    pathError path = error ("runReducer: unexpected output path: " ++ path)

------------------------------------------------------------------------

pathTags :: Pipeline -> Map FilePath Tag
pathTags Pipeline{..} = M.fromList
                      . flip zip [startTag..]
                      . map fName
                      . concatMap stepOutputs
                      . M.elems
                      $ pReducers
  where
    startTag :: Tag
    startTag = fromMaybe 0
             . fmap ((+1) . fst . fst)
             . M.maxViewWithKey
             $ pReducers
