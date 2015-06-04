{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecursiveDo #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeOperators #-}

{-# OPTIONS_GHC -w #-}

module Dipper.Interpreter where

import           Data.Binary.Get
import           Data.Binary.Put
import qualified Data.ByteString as S
import qualified Data.ByteString.Lazy as L
import           Data.ByteString.Builder
import           Data.Int (Int64)
import           Data.Map (Map)
import qualified Data.Map as M
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import           Data.Word (Word8)

import           Dipper.AST
import           Dipper.Binary
import           Dipper.Types

------------------------------------------------------------------------

data File = File FilePath RowFormat RowType
    deriving (Eq, Ord, Show)

data MSCR = MSCR {
    mscrInputs  :: [File]
  , mscrOutputs :: [File]
  , mscrMapper  :: L.ByteString -> L.ByteString
  , mscrReducer :: L.ByteString -> L.ByteString
  }

------------------------------------------------------------------------

interpret :: Term n () -> MSCR
interpret _ = undefined

------------------------------------------------------------------------

readsOfTerm :: Term n a -> [File]
readsOfTerm term = case term of
    Let _ (ReadFile path :: Tail n b) tm ->

        (File path (rowFormat (undefined :: b))
                   (rowType   (undefined :: b))) : readsOfTerm tm

    Let _  _ tm -> readsOfTerm tm
    Run    _ tm -> readsOfTerm tm
    Return _    -> []

writesOfTerm :: Term n a -> [File]
writesOfTerm term = case term of
    Run    tl tm -> writesOfTail tl ++ writesOfTerm tm
    Let _  tl tm -> writesOfTail tl ++ writesOfTerm tm
    Return tl    -> writesOfTail tl

writesOfTail :: Tail n a -> [File]
writesOfTail tl = case tl of
    WriteFile path (xs :: Atom n b) -> [File path (rowFormat (undefined :: b))
                                                  (rowType   (undefined :: b))]
    _                               -> []

------------------------------------------------------------------------

test = decodeTagged schema (encodeTagged schema (take 10 xs))
  where
    xs = cycle [ 67 :*: "abcdefg" :*: S.empty
               , 67 :*: "123"     :*: S.empty
               , 22 :*: "1234"    :*: "Hello World!" ]

    schema = M.fromList [ (67, VarVInt :*: Fixed 0)
                        , (22, Fixed 4 :*: VarWord32be) ]

------------------------------------------------------------------------

decodeTagged :: Map Tag RowFormat -> L.ByteString -> [TaggedRow]
decodeTagged schema bs =
    -- TODO this is unlikely to have good performance
    case runGetOrFail (getTagged schema) bs of
        Left  (_,   _, err)            -> error ("decodeTagged: " ++ err)
        Right (bs', o, x) | L.null bs' -> [x]
                          | otherwise  -> x : decodeTagged schema bs'

encodeTagged :: Map Tag RowFormat -> [TaggedRow] -> L.ByteString
encodeTagged schema = runPut . mapM_ (putTagged schema)

getTagged :: Map Tag RowFormat -> Get TaggedRow
getTagged schema = do
    tag <- getWord8
    case M.lookup tag schema of
      Nothing              -> fail ("getTagged: invalid tag <" ++ show tag ++ ">")
      Just (kFmt :*: vFmt) -> pure tag <&> getFormatted kFmt <&> getFormatted vFmt

putTagged :: Map Tag RowFormat -> TaggedRow -> Put
putTagged schema (tag :*: k :*: v) =
    case M.lookup tag schema of
      Nothing              -> fail ("putTagged: invalid tag <" ++ show tag ++ ">")
      Just (kFmt :*: vFmt) -> putWord8 tag >> putFormatted kFmt k >> putFormatted vFmt v

getFormatted :: ByteFormat -> Get S.ByteString
getFormatted VarVInt     = getByteString =<< getVInt
getFormatted VarWord32be = getByteString . fromIntegral =<< getWord32be
getFormatted (Fixed n)   = getByteString n

putFormatted :: ByteFormat -> S.ByteString -> Put
putFormatted fmt bs = case fmt of
    VarVInt                 -> putVInt     len  >> putByteString bs
    VarWord32be             -> putWord32be lenW >> putByteString bs
    (Fixed n)   | n == len  -> putByteString bs
                | otherwise -> fail ("putTagged: incorrect tag size: "
                                  ++ "expected <" ++ show n ++ " bytes> "
                                  ++ "but was <" ++ show len ++ " bytes>")
  where
    len  = S.length bs
    lenW = fromIntegral (S.length bs)
