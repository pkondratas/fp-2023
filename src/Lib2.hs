{-# OPTIONS_GHC -Wno-unused-top-binds #-}
{-# OPTIONS_GHC -Wno-incomplete-patterns #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}
{-# HLINT ignore "Use isJust" #-}
{-# OPTIONS_GHC -Wno-unused-matches #-}

module Lib2
  ( parseStatement,
    executeStatement,
    ParsedStatement(..)
  )
where

import Data.List ( elemIndex, isPrefixOf )
import DataFrame (DataFrame (DataFrame), Column (Column), ColumnType (StringType, IntegerType), Value (StringValue, IntegerValue, NullValue), Row)
import InMemoryTables ( TableName, database )
import Data.Char (toLower)
import Data.Maybe (fromMaybe)
import Data.List (isPrefixOf)

type ErrorMessage = String
type Database = [(TableName, DataFrame.DataFrame)]

-- Keep the type, modify constructors
data ParsedStatement
  = ShowTable String
  | ShowTables
  | SelectStatement [String] String String
  deriving (Show, Eq)

-- Parses user input into an entity representing a parsed
-- statement
parseStatement :: String -> Either ErrorMessage ParsedStatement
-- parseStatement _ = Left "Not implemented: yooooo"
parseStatement query =
  if elemIndex ';' query /= Nothing && fromMaybe (-1) (elemIndex ';' query) == length query - 1
    then identifyCommand (words (init query))
  else
    Left "Invalid syntax."

  where
    identifyCommand :: [String] -> Either ErrorMessage ParsedStatement
    identifyCommand [] = Left "Empty query."
    identifyCommand (w:ws) =
      case validCommand w ws of
        Left err -> Left err
        Right rest -> Right rest

    -- function that reads the the keywords
    validCommand :: String -> [String] -> Either ErrorMessage ParsedStatement
    validCommand _ [] = Left "Too few arguments."
    validCommand command q
      | map toLower command == "show" = case map toLower (head q) of
          "table" ->
            case identifyShowTableName (tail q) of
              Left err -> Left err
              Right result -> Right result
          "tables" ->
            if null (tail q)
              then Right ShowTables
            else
              Left "Wrong query syntax."
          _ -> Left "Unsuported command/wrong syntax."
      | map toLower command == "select" = case splitSelectStatement q of
        Left err -> Left err
        Right result -> Right result
      | otherwise = Left "Wrong query syntax"

    splitSelectStatement :: [String] -> Either ErrorMessage ParsedStatement
    splitSelectStatement q = do
        (a, b) <- splitColumns [] q
        (name, conditions) <- identifySelectTableName b
        parseSelectStatement a name conditions

    -- splits columns until finds from (if where or select - error)
    splitColumns :: [String] -> [String] -> Either ErrorMessage ([String], [String])
    splitColumns cols [] = Left "Wrong query syntax"
    splitColumns cols w
      | map toLower (head w) == "where" || map toLower (head w) == "select" = Left "Wrong SELECT/WHERE placement/count."
      | map toLower (head w) == "from" && null cols = Left "No columns specified"
      | map toLower (head w) == "from" = Right (cols, tail w)
      | otherwise = splitColumns (cols ++ [head w]) (tail w)

    -- identifies the name (can't be select where from or any other)
    -- if after from contains more than one word (which are not name and WHERE clause) throws error
    identifySelectTableName :: [String] -> Either ErrorMessage (String, [String])
    identifySelectTableName [] = Left "No table name after FROM."
    identifySelectTableName (w:ws)
      | map toLower w == "where" || map toLower w == "from" || map toLower w == "select" = Left "Table name can not be a SQL statement."
      | null ws = Right (w, ws)
      | map toLower (head ws) == "where" = Right (w, ws)
      | otherwise = Left "Table name should contain only one word, followed with optional conditions(WHERE)"

    -- makes a parsed select depending if there is where clause or not
    parseSelectStatement :: [String] -> String -> [String] -> Either ErrorMessage ParsedStatement
    parseSelectStatement cols name [_] = Left "Conditions needed after WHERE."
    parseSelectStatement cols name [] = Right (SelectStatement cols name "")
    parseSelectStatement cols name conditions = Right (SelectStatement cols name (unwords (tail conditions)))

    -- SHOW TABLE table_name identification 
    identifyShowTableName :: [String] -> Either ErrorMessage ParsedStatement
    identifyShowTableName [] = Left "Specify table name."
    identifyShowTableName (_:_:_) = Left "Table name should contain one word."
    identifyShowTableName (w:ws) = Right (ShowTable w)

-- Executes a parsed statemet. Produces a DataFrame. Uses
-- InMemoryTables.databases a source of data.
-- execute SHOW TABLE table_name;
executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement (ShowTable table_name) =
  case lookup table_name database of
    Just result -> Right $ DataFrame [Column "Columns" StringType] (map (\(Column name _) -> [StringValue name]) (columns result))
    Nothing -> Left ("Table " ++ table_name ++ " doesn't exist")
  where
    columns :: DataFrame -> [Column]
    columns (DataFrame cols _) = cols

-- execute SHOW TABLES;
executeStatement ShowTables =
  Right $ DataFrame [Column "Tables" StringType] (map (\(name, _) -> [StringValue name]) database)


--execute SELECT cols FROM table WHERE ... AND ... AND ...;
executeStatement (SelectStatement cols table conditions) =
  case applyConditions conditions table of
    Left err -> Left err
    Right (DataFrame c r) ->
      if validateColumns (map (\(Column name _) -> name) c) c cols
        then case executeSelect c r cols of
          Left err -> Left err
          Right result -> Right result
      else if checkIfAll cols
        then Right (DataFrame c r)
      else
        Left "(Some of the) Column(s) not found."
  where
    checkIfAll :: [String] -> Bool
    checkIfAll [n] =
      n == "*"

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


-- Filters a DataFrame table by statement conditions
applyConditions :: String -> String -> Either ErrorMessage DataFrame
applyConditions conditions tableName = do
    let maybeDataFrame = lookup tableName database
    case maybeDataFrame of
        Just (DataFrame columns rows) -> do
          if null conditions
            then return (DataFrame columns rows)
          else
            return (DataFrame columns (filterRows conditions tableName rows))
        Nothing -> Left "Table not found in the database"

-- Filters rows based on the given conditions
filterRows :: String -> String -> [Row] -> [Row]
filterRows conditions table rows =
    [row | row <- rows, checkAll conditions table row == Right True]

checkCondition :: String -> String -> Row -> Either ErrorMessage Bool
checkCondition condition table row = executeCondition (getFirstThreeWords condition)
  where
    getFirstThreeWords :: String -> (String, String, String)
    getFirstThreeWords input =
      case words input of
        [operand1, operator, operand2] -> (operand1, operator, operand2)
        _ -> error "Incorrect condition syntax"

    -- Returns the operation value based on the operator provided in the condition string
    executeCondition :: (String, String, String) -> Either ErrorMessage Bool
    executeCondition (operand1,  operator,  operand2 ) =
      case operator of
        "=" -> Right (getOperandValue operand1 == getOperandValue operand2)
        "<>" -> Right (getOperandValue operand1 /= getOperandValue operand2)
        "!=" -> Right (getOperandValue operand1 /= getOperandValue operand2)
        "<" -> Right (getOperandValue operand1 < getOperandValue operand2)
        ">" -> Right (getOperandValue operand1 > getOperandValue operand2)
        "<=" -> Right (getOperandValue operand1 <= getOperandValue operand2)
        ">=" -> Right (getOperandValue operand1 >= getOperandValue operand2)
        _ -> error "Incorrect condition syntax"

    -- Returns an operand value based on the operand string (either a regular integer or a column value)
    getOperandValue :: String -> Integer
    getOperandValue opName =
      if isInteger opName then
        case reads opName of
          [(intValue, _)] -> intValue
      else
        case getValueFromTable opName row of
          IntegerValue intValue -> intValue
          _ -> error "Column does not exist in this row"

    -- Check if operand is an integer      
    isInteger :: String -> Bool
    isInteger str = case reads str :: [(Integer, String)] of
      [(_, "")] -> True
      _ -> False

    -- Get the value from the DataFrame row by column name
    getValueFromTable :: String -> Row -> Value
    getValueFromTable columnName currentRow =
      let
        -- Find the DataFrame by the tableName in the database
        maybeDataFrame = lookup table database

        -- Find the column index by columnName
        maybeColumnIndex = maybeDataFrame >>= \(DataFrame columns _) ->
          elemIndex columnName (map (\(Column name _) -> name) columns)

        -- Get value from the row
        value = maybe NullValue (\index -> fromMaybe NullValue (currentRow `atMay` index)) maybeColumnIndex
      in
        value

    -- Get an element at a specific index in a list
    atMay :: [a] -> Int -> Maybe a
    atMay [] _ = Nothing
    atMay (x:_) 0 = Just x
    atMay (_:xs) n = atMay xs (n - 1)

checkAll :: String -> String -> Row -> Either ErrorMessage Bool
checkAll conditions tableName row
    | null conditions = Left "Conditions string is empty"
    | otherwise = checkAllConditions (splitByAnd conditions) tableName row

checkAllConditions :: [String] -> String -> Row -> Either ErrorMessage Bool
checkAllConditions [] _ _ = Right True -- Base case: all conditions have been checked and are true
checkAllConditions (condition:rest) tableName row =
    case checkCondition condition tableName row of
        Left errMsg -> Left errMsg -- If any condition fails, return the error message
        Right True -> checkAllConditions rest tableName row -- If condition is true, check the rest of the conditions
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
