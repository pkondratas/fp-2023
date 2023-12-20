{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wno-orphans #-}
{-# OPTIONS_GHC -Wno-incomplete-patterns #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}
{-# HLINT ignore "Replace case with fromMaybe" #-}
{-# HLINT ignore "Use zipWith" #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}
{-# HLINT ignore "Eta reduce" #-}
{-# HLINT ignore "Use if" #-}

module Lib3
  ( executeSql,
    Execution,
    ExecutionAlgebra(..),
    dataFrameToJson,
    jsonParser
  )
where

import Data.Char
import Data.List ( intercalate, elemIndex, isPrefixOf, nub, nubBy )
import Control.Monad.Free (Free (..), liftF)
import DataFrame (DataFrame)
import Data.Time ( UTCTime, formatTime, defaultTimeLocale )
import Data.Aeson hiding (Value)
import InMemoryTables ( database )
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.ByteString.Lazy.Char8 as BSL
import Control.Applicative ((<|>))
import Text.Read (readMaybe)
import Control.Monad.Trans.Class (lift)
import Data.Either (partitionEithers)
import DataFrame (DataFrame (..), Column (..), ColumnType (..), Value (..), Row)
import Lib2
    ( ParsedStatement(SelectStatement, ShowTable, ShowTables, InsertStatement, UpdateStatement, DeleteStatement),
      parseStatement, sortDataFrame)
import Control.Monad.Trans.Except (runExceptT, ExceptT (ExceptT), throwE)

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
    Right (SelectStatement cols tables conditions orderCols sortMode) -> executeStatement (SelectStatement cols tables conditions orderCols sortMode)
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
    Right (DeleteStatement tableName conditions) -> do
      loadResult <- loadFile tableName
      case loadResult of
        Left errorMsg -> return $ Left errorMsg
        Right df -> do
          if null conditions
            then deleteWithoutWhere df tableName
            else do
              let result = deleteWithWhere df conditions
              case result of
                Left errorMsg -> return $ Left errorMsg
                Right deletedData -> saveDeletedDataFrame deletedData tableName

deleteWithoutWhere :: DataFrame -> String -> Execution (Either ErrorMessage DataFrame)
deleteWithoutWhere originalDataFrame tableName = do
    let deletedDataFrame = deleteAllRows originalDataFrame
    saveFile (tableName, deletedDataFrame)
    return $ Right deletedDataFrame

deleteAllRows :: DataFrame -> DataFrame
deleteAllRows (DataFrame columns _) = DataFrame columns []

deleteWithWhere :: DataFrame -> String -> Either String DataFrame
deleteWithWhere (DataFrame columns rows) conditions =
  let keepRow row =
        case checkAll conditions (DataFrame columns [row]) row of
          Right True  -> False
          Right False -> True
          Left err    -> error err 
      updatedRows = filter keepRow rows
  in case sequence (map (checkAll conditions (DataFrame columns rows)) rows) of
    Right _ -> Right (DataFrame columns updatedRows)
    Left err -> Left ("deletion failed: " ++ err)

saveDeletedDataFrame :: DataFrame -> String -> Execution (Either ErrorMessage DataFrame)
saveDeletedDataFrame df tableName = do
  saveFile (tableName, df)
  return $ Right df

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
  parseJSON _                  = fail "Invalid ColumnType"

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

jsonParser :: String -> Maybe DataFrame
jsonParser jsonData = decode (BSL.fromStrict $ TE.encodeUtf8 $ T.pack jsonData)

-- execute SHOW TABLE table_name;
executeStatement :: ParsedStatement -> Execution (Either ErrorMessage DataFrame)
executeStatement (ShowTable table_name) = do
  result <- loadFile table_name
  case result of
    Left err -> return $ Left err
    Right (DataFrame cols _) -> return $ Right (DataFrame [Column "Columns" StringType] (map (\(Column name _) -> [StringValue name]) cols))

-- execute SHOW TABLES;
executeStatement ShowTables = do
  tableNames <- getTableNames
  return $ Right $ DataFrame [Column "Tables" StringType] (map (\name -> [StringValue name]) tableNames)

--execute SELECT cols FROM table WHERE ... AND ... AND ...;
executeStatement (SelectStatement cols tables conditions orderCols sortMode) = do
  result <- loadTables tables
  case result of 
    Left err -> return $ Left err
    Right dataFrames -> runExceptT $ do
      (DataFrame c r) <- ExceptT . return $ applyConditions conditions tables dataFrames--CIA PRIDETI ORDERINIMA
      if validateColumns (map (\(Column name _) -> name) c) c cols
        then do
          res <- ExceptT . return $ executeSelect c r cols
          case checkIfNowExists cols of
            False -> return $ sortDataFrame res orderCols sortMode
            True -> lift $ addNowColsToDataFrame res
      else if checkIfAll cols
        then return $ sortDataFrame (DataFrame c r) orderCols sortMode
      else
         throwE "(Some of the) Column(s) not found."

  where
    addNowColsToDataFrame :: DataFrame -> Execution DataFrame
    addNowColsToDataFrame (DataFrame cs rows) = do
      time <- getTime
      return $ (DataFrame cs (addToDataFrame cs rows (replicate (length rows) []) time))

    addToDataFrame :: [Column] -> [Row] -> [Row] -> UTCTime -> [Row]
    addToDataFrame [] _ newRows _ = newRows
    addToDataFrame ((Column name _):cs) rows newRows time =
          if name == "NOW()"
            then addToDataFrame cs rows (map (++ [timeToValue time] ) newRows) time
          else
            addToDataFrame cs (map tail rows) (addValues newRows (map head rows)) time

    addValues :: [[Value]] -> [Value] -> [[Value]]
    addValues rows vals =
      zipWith (\value list -> list ++ [value]) vals rows

    timeToValue :: UTCTime -> Value
    timeToValue currentTime =
      StringValue $ formatTime defaultTimeLocale "%Y-%m-%d %H:%M:%S" currentTime

    checkIfNowExists :: [String] -> Bool
    checkIfNowExists [] = False
    checkIfNowExists (c:cs) =
      if (map toLower c) == "now()"
        then True
      else checkIfNowExists cs

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
        | (map toLower str) == "now()" = True
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
            "now(" -> extractFunctions cs (cols, functions ++ [("now()", "")])
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
              case map toLower (fst tup) of
                "min"   -> c ++ [Column ("MIN(" ++ (snd tup) ++ ")") IntegerType]
                "sum"   -> c ++ [Column ("SUM(" ++ (snd tup) ++ ")") IntegerType]
                "now()" -> c ++ [Column "NOW()" StringType]

        -- reikia atskirt kur yra column name ir kur yra min ir sum
        fun = extractFunctions selectedColumnNames ([], [])
        filteredRows = filterRows columns rows (fst fun)
        allRows = combineRowsAndFunctions filteredRows (snd fun) columns rows
        selectedColumns = extractColumns selectedColumnNames columns
        allCols = combineColsAndFunctions selectedColumns (snd fun)
      in
        Right (DataFrame allCols allRows)

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
              case updatedDF of
                Left err -> return $ Left err
                Right df -> do
                  (_, saveResult) <- saveFile (table, df)
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


updateDataFrame :: DataFrame -> [Column] -> [Value] -> String -> Either ErrorMessage DataFrame
updateDataFrame (DataFrame columns rows) updateColumns newValues condition = do
  let result = map updateRow rows
  let (errors, updatedRows) = partitionEithers result
  if null errors
    then Right $ DataFrame columns updatedRows
    else Left "Incorrect condition"
  where
    updateRow :: Row -> Either ErrorMessage Row
    updateRow row =
      case checkAll condition (DataFrame columns [row]) row of
        Right True  -> Right $ updateRowValues row
        Right False -> Right row
        Left err -> Left err

    updateRowValues :: Row -> Row
    updateRowValues row =
      let updatedRow = zipWith updateValue columns row
      in updatedRow

    updateValue :: Column -> Value -> Value
    updateValue (Column colName colType) oldValue =
      case lookup colName (zipWithColumnAndValue updateColumns newValues) of
        Just newValue -> newValue
        Nothing       -> oldValue

    zipWithColumnAndValue :: [Column] -> [Value] -> [(String, Value)]
    zipWithColumnAndValue cols vals =
      map (\(Column colName _, value) -> (colName, value)) (zip cols vals)

applyConditions :: String -> [String] -> [DataFrame] -> Either ErrorMessage DataFrame
--Jeigu yra tik vienas tableName
applyConditions conditions [tableName] [table] = do
    case table of
      (DataFrame columns rows) ->
        if null conditions
          then return (DataFrame (renameColumns tableName columns) rows)
          else return (DataFrame (renameColumns tableName columns) (filterRows conditions (DataFrame (renameColumns tableName columns) rows) rows))

--Jeigu yra daugiau
applyConditions conditions tableNames tables = do
  let joinedTable = joinTables tableNames tables
  case joinedTable of
        (DataFrame columns rows) ->
          if null conditions
            then return (DataFrame columns rows)
            else return (DataFrame columns (filterRows conditions joinedTable rows))

----------------------
--For joining tables--
----------------------
joinTables :: [String] -> [DataFrame] -> DataFrame
joinTables tableNames tables =
  foldl (\acc (tableName, df) -> joinTwoTables (head tableNames) acc tableName df) baseTable restTables
  where
    baseTable = head tables
    restTables =  zip (tail tableNames) (tail tables)

joinTwoTables :: TableName -> DataFrame -> TableName -> DataFrame -> DataFrame
joinTwoTables tableName1 df1 tableName2 df2 =
  let commonColumns = findCommonColumns (renameColumns tableName1 (getColumns df1)) (renameColumns tableName2 (getColumns df2))
      df1Renamed = DataFrame (renameColumns tableName1 (getColumns df1)) (getRows df1)
      df2Renamed = DataFrame (renameColumns tableName2 (getColumns df2)) (getRows df2)
      combinedDF = rowMultiplication df1Renamed df2Renamed
  in removeDuplicateColumns commonColumns combinedDF

findCommonColumns :: [Column] -> [Column] -> [String]
findCommonColumns cols1 cols2 =
  [colName1 | Column colName1 _ <- cols1, any (\(Column colName2 _) -> colName1 == colName2) cols2]

renameColumns :: TableName -> [Column] -> [Column]
renameColumns tableName cols =
  [Column (tableName ++ "." ++ colName) colType | Column colName colType <- cols]

rowMultiplication :: DataFrame -> DataFrame -> DataFrame
rowMultiplication (DataFrame cols1 rows1) (DataFrame cols2 rows2) =
  DataFrame (cols1 ++ cols2) [row1 ++ row2 | row1 <- rows1, row2 <- rows2]

removeDuplicateColumns :: [String] -> DataFrame -> DataFrame
removeDuplicateColumns commonColumns (DataFrame cols rows) =
  DataFrame (nubBy (\(Column col1 _) (Column col2 _) -> col1 == col2) cols) rows

getColumns :: DataFrame -> [Column]
getColumns dataFrame = case dataFrame of
  (DataFrame columns rows) -> columns

getRows :: DataFrame -> [Row]
getRows dataFrame = case dataFrame of
  (DataFrame columns rows) -> rows

-- Helper function to find a table by name in the database
findTableByName :: String -> (TableName, DataFrame)
findTableByName tableName =
  case lookup tableName database of
    Just df -> (tableName, df)
    Nothing -> error ("Table not found: " ++ tableName)
----------------------

-- Filters rows based on the given conditions
filterRows :: String -> DataFrame -> [Row] -> [Row]
filterRows conditions table rows =
    [row | row <- rows, checkAll conditions table row == Right True]

checkCondition :: String -> DataFrame -> Row -> Either ErrorMessage Bool
checkCondition condition table row = executeCondition $ getFirstThreeWords condition
  where
    getFirstThreeWords :: String -> Either ErrorMessage (String, String, String)
    getFirstThreeWords input =
      case words input of
        [operand1, operator, operand2] -> Right (operand1, operator, operand2)
        _ -> Left "Incorrect condition syntax"

    -- Returns the operation value based on the operator provided in the condition string
    executeCondition :: Either ErrorMessage (String, String, String) -> Either ErrorMessage Bool
    executeCondition result = do
      case result of
        Left err -> Left err
        Right (op1, operator, op2) -> do
          case getOperandValue op1 of
            Left err -> Left err
            Right NullValue -> Right False
            Right (IntegerValue operand1) -> do
              case getOperandValue op2 of
                Left err -> Left err
                Right NullValue -> Right False
                Right (IntegerValue operand2) -> do
                  case operator of
                    "=" -> Right (operand1 == operand2)
                    "<>" -> Right (operand1 /= operand2)
                    "!=" -> Right (operand1 /= operand2)
                    "<" -> Right (operand1 < operand2)
                    ">" -> Right (operand1 > operand2)
                    "<=" -> Right (operand1 <= operand2)
                    ">=" -> Right (operand1 >= operand2)
                    _ -> Left "Incorrect condition syntax"

    -- Returns an operand value based on the operand string (either a regular integer or a column value)
    getOperandValue :: String -> Either ErrorMessage Value
    getOperandValue opName =
      if isInteger opName then
        case reads opName of
          [(intValue, _)] -> Right (IntegerValue intValue)
      else
        case getValueFromTable opName row of
          Right (IntegerValue intValue) -> Right (IntegerValue intValue)
          Right NullValue -> Right NullValue
          _ -> Left "Column does not exist in this row"

    -- Check if operand is an integer      
    isInteger :: String -> Bool
    isInteger str = case reads str :: [(Integer, String)] of
      [(_, "")] -> True
      _ -> False

    -- Get the value from the DataFrame row by column name
    getValueFromTable :: String -> Row -> Either ErrorMessage Value
    getValueFromTable columnName currentRow =
      let
        toMaybeDataFrame :: DataFrame -> Maybe DataFrame
        toMaybeDataFrame df = case df of
          (DataFrame [] []) -> Nothing
          _ -> Just df

        -- Find the column index by columnName
        maybeColumnIndex = toMaybeDataFrame table >>= \(DataFrame columns _) ->
          elemIndex columnName (map (\(Column name _) -> name) columns)

        maybeValue index =
          case index of
            Just i -> do
              case currentRow `atMay` i of 
                Just val -> Right val
                Nothing -> Left "Column does not exist"
            Nothing -> Left "Column does not exist"

        -- Get value from the row
        value = maybeValue maybeColumnIndex
      in
        value

    -- Get an element at a specific index in a list
    atMay :: [a] -> Int -> Maybe a
    atMay [] _ = Nothing
    atMay (x:_) 0 = Just x
    atMay (_:xs) n = atMay xs (n - 1)

checkAll :: String -> DataFrame -> Row -> Either ErrorMessage Bool
checkAll conditions tableFrame row
    | null conditions = Right True
    | otherwise = checkAllConditions (splitByAnd conditions) tableFrame row

checkAllConditions :: [String] -> DataFrame -> Row -> Either ErrorMessage Bool
checkAllConditions [] _ _ = Right True -- Base case: all conditions have been checked and are true
checkAllConditions (condition:rest) tableFrame row =
    case checkCondition condition tableFrame row of
        Left errMsg -> Left errMsg -- If any condition fails, return the error message
        Right True -> checkAllConditions rest tableFrame row -- If condition is true, check the rest of the conditions
        Right False -> Right False -- If condition is false, return false immediately

splitByAnd :: String -> [String]
splitByAnd input = splitByWord "and" input

splitByWord :: String -> String -> [String]
splitByWord _ [] = [""]
splitByWord word input@(x:xs)
    | word `isPrefixOf` map toLower input = "" : splitByWord word (drop (length word) input)
    | otherwise = (x : head rest) : tail rest
    where
        rest = splitByWord word xs