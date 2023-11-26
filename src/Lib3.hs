{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE OverloadedStrings #-}

module Lib3
  ( executeSql,
    Execution,
    ExecutionAlgebra(..)
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
import DataFrame (ColumnType(..), Column(..), Value(..), Row, DataFrame(..))

type TableName = String
type FileContent = String
type ErrorMessage = String

data ExecutionAlgebra next
  = LoadFile TableName (FileContent -> next)
  | GetTime (UTCTime -> next)
  -- feel free to add more constructors here
  deriving Functor

type Execution = Free ExecutionAlgebra

loadFile :: TableName -> Execution FileContent
loadFile name = liftF $ LoadFile name id

getTime :: Execution UTCTime
getTime = liftF $ GetTime id

executeSql :: String -> Execution (Either ErrorMessage DataFrame)
executeSql sql = do
    return $ Left "implement me"


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
  --jsonData <- readFile "C:\\Users\\Dovix\\Desktop\\GIT\\fp-2023\\output.json"
  --let parsedData = decode (BSL.fromStrict $ TE.encodeUtf8 $ T.pack jsonData) :: Maybe DataFrame
  --case parsedData of
    --Just df -> print df
    --Nothing -> putStrLn "Failed to parse JSON"
