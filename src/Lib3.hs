{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wno-orphans #-}
{-# OPTIONS_GHC -Wno-incomplete-patterns #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}
{-# HLINT ignore "Eta reduce" #-}

module Lib3
  ( executeSql,
    Execution,
    ExecutionAlgebra(..),
    dataFrameToJson
  )
where

import Data.List(intercalate, nub)
import Control.Monad.Free (Free (..), liftF)
import DataFrame (DataFrame)
import Data.Time ( UTCTime )
import Data.Aeson hiding (Value)
import Control.Applicative ((<|>))
import DataFrame (ColumnType(..), Column(..), Value(..), Row, DataFrame(..))
import Lib2
    ( ParsedStatement(SelectStatement, ShowTable, ShowTables),
      parseStatement,
      executeStatement )
import Control.Monad.Trans.Error (Error)
import GHC.Windows (errCodeToIOError)
import Text.Read (readMaybe)

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
    -- Right (InsertData table_name cols values) -> do
    --   result <- loadFile table_name
    --   case result of 
    --     Left err -> return $ Left err
    --     Right (DataFrame cls rws) -> do
    --       case parseRows cls cols values of
    --         Left err -> return $ Left err
    --         Right newRows -> do 
    --           (_, df) <- saveFile (table_name, DataFrame cls (rws ++ newRows))
    --           return $ Right df

parseRows :: [Column] -> [String] -> [[String]] -> Either ErrorMessage [Row]
parseRows table_cols cols values = do
  () <- validateInput table_cols cols values
  return (createRows table_cols cols values)

createRows :: [Column] -> [String] -> [[String]] -> [Row]
createRows _ _ [] = []
createRows table_cols cols (v:vs) =
   createRow table_cols cols v : createRows table_cols cols vs

-- createRow :: [Column] -> [String] -> [String] -> Row
-- createRow ((Column name dtype):tcls) cols values = createValuesForRow name dtype cols values

createRow :: [Column] -> [String] -> [String] -> [Value]
createRow [] _ _ = []
createRow ((Column name dtype):tcls) cols values =
  createValueForRow name dtype cols values : createRow tcls cols values

createValueForRow :: String -> ColumnType -> [String] -> [String] -> Value
createValueForRow _ _ [] [] = NullValue
createValueForRow name dtype (c:cs) (v:vs) =
  if name == c
    then case dtype of
      IntegerType -> IntegerValue (getValueFromMaybe (stringToInt v))
      StringType  -> StringValue v
      BoolType    -> BoolValue (getValueFromMaybe (stringToBool v))
  else
    createValueForRow name dtype cs vs

validateInput :: [Column] -> [String] -> [[String]] -> Either ErrorMessage ()
validateInput table_cols cols values = do
    () <- hasDuplicates (getStringValues table_cols)
    () <- validateColumnNames (getStringValues table_cols) cols
    () <- validateNewValues table_cols cols values
    return ()

validateNewValues :: [Column] -> [String] -> [[String]] -> Either ErrorMessage ()
validateNewValues _ _ [] = Right ()
validateNewValues tcols cols (r:rs) =
  case validateRow cols r of
    Left err -> Left err
    Right () -> validateNewValues tcols cols rs
  where
    validateRow :: [String] -> [String] -> Either ErrorMessage ()
    validateRow [] [] = Right ()
    validateRow [] _  = Left "Too many values"
    validateRow _ []  = Left "Not enough values provided"
    validateRow (n:ns) (rw:rws) =
      case validateValue tcols n rw of
        Left err -> Left err
        Right () -> validateRow ns rws

    validateValue :: [Column] -> String -> String -> Either ErrorMessage ()
    validateValue [] _ _ = Left "Column wasn't found"
    validateValue ((Column name dtype):tcs) n rw =
      if name == n
        then case dtype of
          IntegerType -> do
            case stringToInt rw of
              Just _   -> Right ()
              Nothing  -> Left ("Value " ++ rw ++ " is not integer")
          StringType  -> Right ()
          BoolType    ->
            case stringToBool rw of
              Just _   -> Right ()
              Nothing  -> Left ("Value " ++ rw ++ " is not bool")
      else validateValue tcs n rw

validateColumnNames :: [String] -> [String] -> Either ErrorMessage ()
validateColumnNames _ [] = Right ()
validateColumnNames col_names (c:cs) =
  if c `elem` col_names
    then validateColumnNames col_names cs
  else
    Left "One or multiple columns don't exist inside the table."

stringToInt :: String -> Maybe Integer
stringToInt str = readMaybe str :: Maybe Integer

stringToBool :: String -> Maybe Bool
stringToBool "True"  = Just True
stringToBool "False" = Just False
stringToBool _       = Nothing

getValueFromMaybe :: Maybe a -> a
getValueFromMaybe (Just maybeValue) = maybeValue

getStringValues :: [Column] -> [String]
getStringValues = map extractString
  where
    extractString :: Column -> String
    extractString (Column str _) = str

hasDuplicates :: [String] -> Either ErrorMessage ()
hasDuplicates xs =
  if length xs /= length (nub xs)
    then Left "Column names cannot be the same"
  else
    Right ()

-- json parsinimas
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
