{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wno-orphans #-}
{-# OPTIONS_GHC -Wno-incomplete-patterns #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}
{-# HLINT ignore "Replace case with fromMaybe" #-}
{-# HLINT ignore "Use zipWith" #-}

module Lib3
  ( executeSql,
    Execution,
    ExecutionAlgebra(..),
    dataFrameToJson
  )
where

import Data.Char
import Data.List(intercalate)
import Control.Monad.Free (Free (..), liftF)
import DataFrame (DataFrame)
import Data.Time ( UTCTime )
import Data.Aeson hiding (Value)
import Data.Aeson.Types (Parser)
import Data.String (IsString, fromString)
import Control.Applicative ((<$>), (<*>), (<|>))
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.ByteString.Lazy.Char8 as BSL
import DataFrame (DataFrame (..), Column (..), ColumnType (..), Value (..), Row)
import Lib2
    ( ParsedStatement(SelectStatement, ShowTable, ShowTables),
      parseStatement,
      executeStatement,
      checkAll )

type TableName = String
type FileContent = String
type ErrorMessage = String

data ExecutionAlgebra next
  = LoadFile TableName (Either ErrorMessage DataFrame -> next)
  | GetTime (UTCTime -> next)
  | SaveFile (String, DataFrame) ((String, DataFrame) -> next)
  -- feel free to add more constructors here
  deriving Functor

type Execution = Free ExecutionAlgebra

loadFile :: TableName -> Execution (Either ErrorMessage DataFrame)
loadFile name = liftF $ LoadFile name id

getTime :: Execution UTCTime
getTime = liftF $ GetTime id

saveFile :: (String, DataFrame) -> Execution (String, DataFrame)
saveFile df = liftF $ SaveFile df id

executeSql :: String -> Execution (Either ErrorMessage DataFrame)
executeSql sql =
  case parseStatement sql of
    Left err -> return (Left err)
    Right (ShowTable table_name) ->
      case executeStatement (ShowTable table_name) of
        Left err -> return $ Left err
        Right df -> return $ Right df
    Right ShowTables ->
      case executeStatement ShowTables of
        Left err -> return $ Left err
        Right df -> return $ Right df
    Right (SelectStatement cols tables conditions) -> do
      case executeStatement (SelectStatement cols tables conditions) of
        Left err -> return $ Left err
        Right df -> return $ Right df



updateDataFrame :: DataFrame -> [Column] -> [Value] -> String -> DataFrame
updateDataFrame (DataFrame columns rows) updateColumns newValues condition =
  let updateRow row =
        case checkAll condition (DataFrame columns [row]) row of
          Right True  -> updateRowValues row
          _           -> row
          
      updateRowValues row =
        let updatedRow = zipWith updateValue columns row
        in updatedRow
      
      updateValue (Column colName colType) oldValue =
        case lookup colName (zipWithColumnAndValue updateColumns newValues) of
          Just newValue -> newValue
          Nothing       -> oldValue
      
      zipWithColumnAndValue :: [Column] -> [Value] -> [(String, Value)]
      zipWithColumnAndValue cols vals =
        map (\(Column colName _, value) -> (colName, value)) (zip cols vals)
      
      updatedRows = map updateRow rows
  in DataFrame columns updatedRows



columnTypeToJson :: ColumnType -> String
columnTypeToJson IntegerType = "integer"
columnTypeToJson StringType  = "string"
columnTypeToJson BoolType    = "bool"

columnToJson :: Column -> [(String, String)]
columnToJson (Column name colType) =
  [ ("name", name)
  , ("type", columnTypeToJson colType)
  ]

valueToJson :: Value -> String
valueToJson (IntegerValue x) = "{\"IntegerValue\":" ++ show x ++ "}"
valueToJson (StringValue x)  = "{\"StringValue\":\"" ++ x ++ "\"}"
valueToJson (BoolValue x)    = "{\"BoolValue\":" ++ if x then "true" ++ "}" else "false" ++ "}"
valueToJson NullValue        = "{\"NullValue\":" ++ "null" ++ "}"

rowToJson :: Row -> [String]
rowToJson = map valueToJson

dataFrameToJson :: DataFrame -> String
dataFrameToJson (DataFrame columns rows) =
  "{ \"columns\": [" ++ columnsJson ++ "], \"rows\": [" ++ rowsJson ++ "] }"
  where
    columnsJson = intercalate ", " $ map (\col -> "{ " ++ intercalate ", " (map (\(k, v) -> "\"" ++ k ++ "\": \"" ++ v ++ "\"") (columnToJson col)) ++ " }") columns
    rowsJson = intercalate ", " $ map (\row -> "[" ++ intercalate ", " (rowToJson row) ++ "]") rows

--main :: IO ()
--main = do
  --let testDataFrame = DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType][ ]
  --writeFile "output.json" (dataFrameToJson testDataFrame)
  --putStrLn "DataFrame written to output.json"

instance FromJSON ColumnType where
  parseJSON (String "integer") = return IntegerType
  parseJSON (String "string")  = return StringType
  parseJSON (String "bool")    = return BoolType
  parseJSON _                   = fail "Invalid ColumnType"

instance FromJSON Column where
  parseJSON (Object v) = do
    name <- v .: "name"
    colType <- v .: "type"
    return $ Column name colType
  parseJSON _ = fail "Invalid Column"

instance FromJSON Value where
  parseJSON (Object v) = (IntegerValue <$> v .: "IntegerValue")
   <|> (StringValue <$> v .: "StringValue")
   <|> (BoolValue <$> v .: "BoolValue")
   <|> pure NullValue
  parseJSON _ = fail "Failed to parse Value"


instance FromJSON DataFrame where
  parseJSON (Object v) = do
    columns <- v .: "columns"
    rows <- v.: "rows"
    return $ DataFrame columns rows
  parseJSON _ = fail "Invalid DataFrame"

--main :: IO ()
--main = do
  --jsonData <- -- path
  --let parsedData = decode (BSL.fromStrict $ TE.encodeUtf8 $ T.pack jsonData) :: Maybe DataFrame
  --case parsedData of
    --Just df -> print df
    --Nothing -> putStrLn "Failed to parse JSON"
