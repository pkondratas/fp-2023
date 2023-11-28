{-# OPTIONS_GHC -Wno-unused-top-binds #-}
{-# OPTIONS_GHC -Wno-incomplete-patterns #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}
{-# HLINT ignore "Use isJust" #-}
{-# OPTIONS_GHC -Wno-unused-matches #-}
{-# HLINT ignore "Use if" #-}

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
import Data.List

type ErrorMessage = String
type Database = [(TableName, DataFrame.DataFrame)]

-- Keep the type, modify constructors
data ParsedStatement
  = ShowTable String
  | ShowTables
  | SelectStatement [String] [String] String
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
        (a, b) <- distinguishColumnNames q
        (names, conditions) <- distinguishTableNames b
        parseSelectStatement a names conditions

    -- makes a parsed select depending if there is where clause or not
    parseSelectStatement :: [String] -> [String] -> [String] -> Either ErrorMessage ParsedStatement
    parseSelectStatement cols names [_] = Left "Conditions needed after WHERE."
    parseSelectStatement cols names [] = Right (SelectStatement cols names "")
    parseSelectStatement cols names conditions = Right (SelectStatement cols names (unwords (tail conditions)))

    -- SHOW TABLE table_name identification 
    identifyShowTableName :: [String] -> Either ErrorMessage ParsedStatement
    identifyShowTableName [] = Left "Specify table name."
    identifyShowTableName (_:_:_) = Left "Table name should contain one word."
    identifyShowTableName (w:ws) = Right (ShowTable w)

distinguishColumnNames :: [String] -> Either ErrorMessage ([String], [String])
distinguishColumnNames rest = 
    case findFrom [] rest of
      Left err -> Left err
      Right (cols, tablesAndConditions) -> case words (changeCommasIntoSpaces (concat cols)) == words (changeCommasIntoSpaces (unwords cols)) of
                False -> Left "Wrong syntax: incorrect commas between columns."
                True -> case concat cols of
                    [] -> Left "Wrong syntax"
                    (c:cs) -> if c == ',' then Left "No columns specified before first comma" else 
                        case validateCommaSyntax cs of
                            Left err -> Left err
                            Right () -> case validateTableNames (splitItemsByComma (concat cols)) of
                                Left err -> Left err
                                Right () -> Right (splitItemsByComma (concat cols), tablesAndConditions)


findFrom :: [String] -> [String] -> Either ErrorMessage ([String], [String])
findFrom cols [] = Left "Wrong syntax: missing 'FROM' statement"
findFrom cols words =
    if map toLower (head words) == "from" then Right (cols, (tail words)) else findFrom (cols ++ [head words]) (tail words)

distinguishTableNames :: [String] -> Either ErrorMessage ([String], [String])
distinguishTableNames rest = 
    let (tables, conditions) = findWhere [] rest
    in case words (changeCommasIntoSpaces (concat tables)) == words (changeCommasIntoSpaces (unwords tables)) of
        False -> Left ((changeCommasIntoSpaces (concat tables)) ++ " != " ++ (changeCommasIntoSpaces (unwords tables)))
        True -> case concat tables of
            [] -> Left "Wrong syntax: no tables found"
            (c:cs) -> if c == ',' then Left "No columns specified before first comma" else 
                case validateCommaSyntax cs of
                    Left err -> Left err
                    Right () -> case validateTableNames (splitItemsByComma (concat tables)) of
                        Left err -> Left err
                        Right () -> Right (splitItemsByComma (concat tables), conditions)

validateTableNames :: [String] -> Either ErrorMessage ()
validateTableNames [] = Right ()
validateTableNames (n:ns)
  | map toLower n == "where" || map toLower n == "select" || map toLower n == "from" = Left "Wrong syntax(keywords cannot be table names)"
  | otherwise = validateTableNames ns

splitItemsByComma :: String -> [String]
splitItemsByComma str = words $ map (\c -> if c == ',' then ' ' else c) str

validateCommaSyntax :: String -> Either ErrorMessage ()
validateCommaSyntax [] = Right ()
validateCommaSyntax (c:cs)
    | c == ',' = if null cs 
        then 
            Left "Wrong syntax: comma can't be last symbol after table name"
        else
            case head cs == ',' of
                False -> validateCommaSyntax (tail cs)
                True -> Left "Wrong syntax(no column name between commas)"
    | otherwise = validateCommaSyntax cs

findWhere :: [String] -> [String] -> ([String], [String])
findWhere tables [] = (tables, [])
findWhere tables words =
    if map toLower (head words) == "where" then (tables, words) else findWhere (tables ++ [head words]) (tail words)

changeCommasIntoSpaces :: String -> String
changeCommasIntoSpaces =  map (\c -> if c == ',' then ' ' else c)

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

--VEIKIMO PRINCIPAS:
--Paduoda conditionus ir tableName'us
--Jeigu table tik vienas, tada gauni DataFrame pagal tableName ir iskart paduodi funkcijai
--Jeigu table daugiau, juos sujungi, tada sujungtus paduodi i funkcija atfiltruoti

--TODO:
-- Kai yra vienas table, DataFrame nera prefikso prie column'o (pvz.: nera, kad butu table1.col1, o yra tiesiog col1)

-- Filters a DataFrame table by statement conditions
applyConditions :: String -> [String] -> Either ErrorMessage DataFrame
--Jeigu yra tik vienas tableName
applyConditions conditions [tableName] = do
    let maybeDataFrame = lookup tableName database
    let table = maybeToDataFrame maybeDataFrame
    case maybeDataFrame of
        Just (DataFrame columns rows) ->
          if null conditions
            then return (DataFrame (renameColumns tableName columns) rows)
            else return (DataFrame (renameColumns tableName columns) (filterRows conditions table rows))
        Nothing -> Left "Table not found in the database"
    where
      maybeToDataFrame :: Maybe DataFrame -> DataFrame
      maybeToDataFrame maybeDf =
        case maybeDf of
          Just df -> df
          Nothing -> DataFrame [] []

applyConditions conditions tableNames = do
  let joinedTable = joinTables tableNames
  case joinedTable of
        (DataFrame columns rows) ->
          if null conditions
            then return (DataFrame columns rows)
            else return (DataFrame columns (filterRows conditions joinedTable rows))

----------------------
--For joining tables--
----------------------
joinTables :: [String] -> DataFrame
joinTables tableNames =
  foldl (\acc (tableName, df) -> joinTwoTables (head tableNames) acc tableName df) baseTable restTables
  where
    baseTable = snd (findTableByName (head tableNames))
    restTables = map (\tableName -> findTableByName tableName) (tail tableNames)

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
        toMaybeDataFrame :: DataFrame -> Maybe DataFrame
        toMaybeDataFrame df = case df of
          (DataFrame [] []) -> Nothing
          _ -> Just df

        -- Find the column index by columnName
        maybeColumnIndex = toMaybeDataFrame table >>= \(DataFrame columns _) ->
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

checkAll :: String -> DataFrame -> Row -> Either ErrorMessage Bool
checkAll conditions tableFrame row
    | null conditions = Left "Conditions string is empty"
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
