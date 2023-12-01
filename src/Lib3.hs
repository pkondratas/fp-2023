{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wno-orphans #-}
{-# OPTIONS_GHC -Wno-incomplete-patterns #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}
{-# HLINT ignore "Replace case with fromMaybe" #-}
{-# HLINT ignore "Use zipWith" #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}
{-# HLINT ignore "Eta reduce" #-}

module Lib3
  ( executeSql,
    Execution,
    ExecutionAlgebra(..),
    dataFrameToJson
  )
where

import Data.Char
import Data.List ( intercalate, elemIndex, isPrefixOf, nub )
import Control.Monad.Free (Free (..), liftF)
import DataFrame (DataFrame)
import Data.Time ( UTCTime )
import Data.Aeson hiding (Value)
import Control.Applicative ((<|>))
import Text.Read (readMaybe)
import DataFrame (DataFrame (..), Column (..), ColumnType (..), Value (..), Row)
import Lib2
    ( ParsedStatement(SelectStatement, ShowTable, ShowTables, InsertStatement, UpdateStatement),
      parseStatement,
      checkAll,
      applyConditions )

type TableName = String
type FileContent = String
type ErrorMessage = String

data ExecutionAlgebra next
  = LoadFile TableName (Either ErrorMessage DataFrame -> next)
  | GetTime (UTCTime -> next)
  | SaveFile (String, DataFrame) ((String, DataFrame) -> next)
  | GetTableNames ([TableName] -> next)
  -- feel free to add more constructors here
  deriving Functor

type Execution = Free ExecutionAlgebra

loadFile :: TableName -> Execution (Either ErrorMessage DataFrame)
loadFile name = liftF $ LoadFile name id

getTime :: Execution UTCTime
getTime = liftF $ GetTime id

saveFile :: (String, DataFrame) -> Execution (String, DataFrame)
saveFile df = liftF $ SaveFile df id

getTableNames :: Execution [TableName]
getTableNames = liftF $ GetTableNames id

executeSql :: String -> Execution (Either ErrorMessage DataFrame)
executeSql sql =
  case parseStatement sql of
    Left err -> return (Left err)
    Right (ShowTable table_name) ->
      executeStatement (ShowTable table_name)
    Right ShowTables -> executeStatement ShowTables
    Right (SelectStatement cols tables conditions) -> executeStatement (SelectStatement cols tables conditions)
    Right (InsertStatement table_name cols values) -> do
      result <- loadFile table_name
      case result of 
        Left err -> return $ Left err
        Right (DataFrame cls rws) -> do
          case parseRows cls cols values of
            Left err -> return $ Left err
            Right newRows -> do 
              (_, df) <- saveFile (table_name, DataFrame cls (rws ++ newRows))
              return $ Right df
    Right (UpdateStatement table columns values condition) -> executeStatement (UpdateStatement table columns values condition)

parseRows :: [Column] -> [String] -> [[String]] -> Either ErrorMessage [Row]
parseRows table_cols cols values = do
  () <- validateInput table_cols cols values
  return (createRows table_cols cols values)

createRows :: [Column] -> [String] -> [[String]] -> [Row]
createRows _ _ [] = []
createRows table_cols cols (v:vs) =
  createRow table_cols cols v : createRows table_cols cols vs

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
    () <- validateColumnNames (getStringValues table_cols) cols
    () <- validateNewValues table_cols cols values
    () <- hasDuplicates cols
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

-- execute SHOW TABLE table_name;
executeStatement :: ParsedStatement -> Execution (Either ErrorMessage DataFrame)
executeStatement (ShowTable table_name) = do
  result <- loadFile table_name
  case result of
    Left err -> return $ Left err
    Right df -> return $ Right df

-- execute SHOW TABLES;
executeStatement ShowTables = do
  tableNames <- getTableNames
  return $ Right $ DataFrame [Column "Tables" StringType] (map (\name -> [StringValue name]) tableNames)

--execute SELECT cols FROM table WHERE ... AND ... AND ...;
executeStatement (SelectStatement cols tables conditions) = do
  result <- loadTables tables
  case result of
    Left err -> return $ Left err
    Right dataFrames ->
      case applyConditions conditions tables dataFrames of
        Left err -> Pure (Left err)
        Right (DataFrame c r) ->
          if validateColumns (map (\(Column name _) -> name) c) c cols
            then case executeSelect c r cols of
              Left err -> Pure (Left err)
              Right res -> Pure (Right res)
          else if checkIfAll cols
            then Pure (Right (DataFrame c r))
          else
            Pure (Left "(Some of the) Column(s) not found.")
  where
    --Cia gaunami DataFrame is tableName
    loadTables :: [String] -> Execution (Either ErrorMessage [DataFrame])
    loadTables names = loadTables' names []
      where
        loadTables' :: [String] -> [DataFrame] -> Execution (Either ErrorMessage [DataFrame])
        loadTables' [] acc = return $ Right acc
        loadTables' (name:restNames) acc = do
          result <- loadFile name
          case result of
            Left err -> return $ Left err
            Right table -> loadTables' restNames (acc ++ [table])

    checkIfAll :: [String] -> Bool
    checkIfAll [n] =
      n == "*"
    checkIfAll _ = False

    -- validates whether columns exist
    validateColumns :: [String] -> [Column] -> [String] -> Bool
    validateColumns columns c [] = True
    validateColumns columns c (n:ns) =
      (elem n columns || checkIfSumOrMin n c) && validateColumns columns c ns

    checkIfSumOrMin :: String -> [Column] -> Bool
    checkIfSumOrMin str columns
        | ("sum(" `isPrefixOf` (map toLower str) || "min(" `isPrefixOf`map toLower str) && last str == ')' =
            checkAgainColumn (drop 4 (init str)) columns
        | otherwise = False

        -- Define a sample function to check the column
    checkAgainColumn :: String -> [Column] -> Bool
    checkAgainColumn columnName columns =
        case any (\(Column name _) -> name == columnName) columns of
            True -> checkIfInteger (filter (\(Column name _) -> name == columnName) columns)
            False -> False

    checkIfInteger :: [Column] -> Bool
    checkIfInteger [(Column _ colType)] =
        colType == IntegerType

    -- filters the received DataFrame with applied conditions 
    executeSelect :: [Column] -> [Row] -> [String] -> Either ErrorMessage DataFrame
    -- visus columus is db, rows, column names
    executeSelect columns rows selectedColumnNames =
      let
        -- Find the index of a column by name in a list of columns
        findColumnIndex :: String -> [Column] -> Maybe Int
        findColumnIndex columnName columns = elemIndex columnName (map (\(Column colName _) -> colName) columns)

        -- Extract values from a specified column in a list of rows
        getValuesInColumn :: Int -> [Row] -> [Integer]
        getValuesInColumn columnIndex rows = [value | (IntegerValue value) <- map (!! columnIndex) rows]

        -- Calculate the minimum value in a specified column of a DataFrame
        minim :: DataFrame -> String -> Either ErrorMessage Integer
        minim (DataFrame columns rows) columnName =
            case findColumnIndex columnName columns of
                Just columnIndex ->
                    case getValuesInColumn columnIndex rows of
                        [] -> Left "No values in the specified column."
                        values -> Right (foldr1 min values)
                Nothing -> Left "Column not found in the DataFrame."

        -- Calculate the sum of values in a specified column of a DataFrame
        sum_n :: DataFrame -> String -> Either ErrorMessage Integer
        sum_n (DataFrame columns rows) columnName =
            case findColumnIndex columnName columns of
                Just columnIndex ->
                    case getValuesInColumn columnIndex rows of
                        values -> Right (sum values)
                Nothing -> Left "Column not found in the DataFrame."

        filterRows :: [Column] -> [Row] -> [String] -> [Row]
        filterRows _ [] _ = []
        filterRows cols (row:restRows) selectedColumnNames =
          let
            extractValues :: [Column] -> Row -> Row
            extractValues selectedCols values = [val | (col, val) <- zip cols values, col `elem` selectedCols]

            filteredRow = extractValues selectedColumns row

            filteredRestRows = filterRows cols restRows selectedColumnNames
          in
            filteredRow : filteredRestRows

        splitByBracket :: String -> (String, String)
        splitByBracket s =
          let
            tuple = splitAt 3 (init s)
          in
            (fst tuple, tail (snd tuple))

        extractFunctions :: [String] -> ([String], [(String, String)]) -> ([String], [(String, String)])
        extractFunctions [] cols = cols
        extractFunctions (c:cs) (cols, functions) =
          case take 4 (map toLower c) of
            "min(" -> extractFunctions cs (cols, functions ++ [splitByBracket c])
            "sum(" -> extractFunctions cs (cols, functions ++ [splitByBracket c])
            _ ->  extractFunctions cs (cols ++ [c], functions)

        -- -- cia baigta
        combineRowsAndFunctions :: [Row] -> [(String, String)] -> [Column] -> [Row] -> [Row]
        combineRowsAndFunctions rez [] _ _ = rez
        combineRowsAndFunctions r (t:ts) columns rows = combineRowsAndFunctions (map (addValueToRow t columns rows) r) ts columns rows
          where
            addValueToRow :: (String, String) -> [Column] -> [Row] -> Row -> Row
            addValueToRow t columns rows row =
              if map toLower (fst t) == "min"
                then
                  case minim (DataFrame columns rows) (snd t) of
                    Left err -> row
                    Right rez -> row ++ [IntegerValue rez]
              else
                case sum_n (DataFrame columns rows) (snd t) of
                  Left err -> row
                  Right rez -> row ++ [IntegerValue rez]


        extractColumns :: [String] -> [Column] -> [Column]
        extractColumns names cols = filter (\(Column name _) -> name `elem` names) cols

        combineColsAndFunctions :: [Column] -> [(String, String)] -> [Column]
        combineColsAndFunctions rez [] = rez
        combineColsAndFunctions rez (t:ts) = combineColsAndFunctions (addValueToCols rez t) ts
          where
            addValueToCols :: [Column] -> (String, String) -> [Column]
            addValueToCols c tup =
              if map toLower (fst tup) == "min"
                then
                  c ++ [Column ("MIN(" ++ (snd tup) ++ ")") IntegerType]
              else
                  c ++ [Column ("SUM(" ++ (snd tup) ++ ")") IntegerType]

        -- reikia atskirt kur yra column name ir kur yra min ir sum
        fun = extractFunctions selectedColumnNames ([], [])
        filteredRows = filterRows columns rows (fst fun)
        allRows = combineRowsAndFunctions filteredRows (snd fun) columns rows
        selectedColumns = extractColumns selectedColumnNames columns
        allCols = combineColsAndFunctions selectedColumns (snd fun)
      in
        if null (snd fun)
          then Right (DataFrame allCols allRows)
          else Right (DataFrame allCols [head allRows])

executeStatement (UpdateStatement table columns values condition) = do
  result <- loadFile table
  case result of
    Left err -> return $ Left err
    Right (DataFrame dfCol dfRows) -> do
      case parseColumns columns dfCol of
        Left err -> return $ Left err
        Right colsToUpdate -> do
          case parseValues values colsToUpdate of
            Left err -> return $ Left err
            Right parsedValues -> do
              let updatedDF = updateDataFrame (DataFrame dfCol dfRows) colsToUpdate parsedValues condition
              (_, saveResult) <- saveFile (table, updatedDF)
              return $ Right saveResult
  where
    parseColumns :: [String] -> [Column] -> Either ErrorMessage [Column]
    parseColumns colStrings dfColumns =
      if allIn colStrings (map (\(Column name _) -> name) dfColumns)
        then Right (getColumns colStrings dfColumns)
        else Left "Incorrect columns"
      where
        allIn :: Eq a => [a] -> [a] -> Bool
        allIn xs ys = all (`elem` ys) xs

        getColumns :: [String] -> [Column] -> [Column]
        getColumns cs dc =
          filter (\(Column name _) -> name `elem` cs) dc

    parseValues :: [String] -> [Column] -> Either ErrorMessage [Value]
    parseValues valueStrings cols
      | length valueStrings /= length cols = Left "Incorrect number of values"
      | otherwise = sequenceA $ zipWith parseValue valueStrings cols
      where
        parseValue :: String -> Column -> Either ErrorMessage Value
        parseValue valueString (Column _ colType) =
          case colType of
            IntegerType -> case reads valueString of
              [(intValue, "")] -> Right (IntegerValue intValue)
              _ -> Left  "Failed to parse Integer value"
            StringType -> Right (StringValue valueString)
            BoolType -> case reads (map toLower valueString) of
              [(boolValue, "")] -> Right (BoolValue boolValue)
              _ -> Left "Failed to parse Bool value"


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