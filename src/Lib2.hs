{-# OPTIONS_GHC -Wno-unused-top-binds #-}
{-# OPTIONS_GHC -Wno-incomplete-patterns #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}
{-# HLINT ignore "Use isJust" #-}
{-# OPTIONS_GHC -Wno-unused-matches #-}
{-# HLINT ignore "Use if" #-}
{-# HLINT ignore "Use guards" #-}

module Lib2
  ( parseStatement,
    executeStatement,
    checkAll,
    applyConditions,
    sortDataFrame,
    ParsedStatement(..),
    SortMode(..)
  )
where

import DataFrame (DataFrame (DataFrame), Column (Column), ColumnType (StringType, IntegerType), Value (..), Row)
import InMemoryTables ( TableName, database )
import Data.Char (toLower)
import Data.Maybe (fromMaybe)
import Debug.Trace
import Data.Char (isSpace)
import Data.List

type ErrorMessage = String
type Database = [(TableName, DataFrame.DataFrame)]

-- Keep the type, modify constructors
data ParsedStatement
  = ShowTable String
  | ShowTables
  | SelectStatement [String] [String] String [String] SortMode
  | InsertStatement String [String] [[String]]
  | DeleteStatement String String
  | UpdateStatement String [String] [String] String
  | DropTableStatement String
  | CreateStatement String [(String, String)]
  deriving (Show, Eq)

data SortMode = Ascending | Descending
  deriving (Show, Eq)

-- Parses user input into an entity representing a parsed
-- statement
parseStatement :: String -> Either ErrorMessage ParsedStatement
-- parseStatement _ = Left "Not implemented: yooooo"
parseStatement query =
  if map toLower (takeWhile (not . isSpace) query) == "insert" && elemIndex ';' query /= Nothing && fromMaybe (-1) (elemIndex ';' query) == length query - 1
    then do
      identifyCommand (splitOnWhitespaceInQuotes query) -- Print cols and valuesBlock identifyCommand (splitOnWhitespaceInQuotes query)
  else if map toLower (takeWhile (not . isSpace) query) == "update" && elemIndex ';' query /= Nothing && fromMaybe (-1) (elemIndex ';' query) == length query - 1
    then do
      identifyCommand (splitOnWhitespaceInQuotes2 query)
  else if map toLower (takeWhile (not . isSpace) query) == "create" && elemIndex ';' query /= Nothing && fromMaybe (-1) (elemIndex ';' query) == length query - 1
    then do
      identifyCommand (removeParensAndSemicolons (addWhitespaceBeforeSemicolon query))    
  else if Data.List.elemIndex ';' query /= Nothing && fromMaybe (-1) (Data.List.elemIndex ';' query) == Data.List.length query - 1
    then identifyCommand (Data.List.words (Data.List.init query))
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
      | Data.List.map toLower command == "show" = case Data.List.map toLower (Data.List.head q) of
          "table" ->
            case identifyShowTableName (Data.List.tail q) of
              Left err -> Left err
              Right result -> Right result
          "tables" ->
            if Data.List.null (Data.List.tail q)
              then Right ShowTables
            else
              Left "Wrong query syntax."
          _ -> Left "Unsuported command/wrong syntax."
      | Data.List.map toLower command == "select" = case splitSelectStatement q of
        Left err -> Left err
        Right result -> Right result
      | Data.List.map toLower command == "delete" = case splitDeleteStatement q of
        Left err -> Left err
        Right result -> Right result
      | Data.List.map toLower command == "update" = case splitUpdateStatement q of
        Left err -> Left err
        Right result -> Right result
      | Data.List.map toLower command == "insert" = case Data.List.map toLower (Data.List.head q) of
          "into" ->
            case identifyInsertValues (Data.List.tail q) of
              Left err -> Left err
              Right result -> Right result
          _ -> Left "Missing 'INTO' keyword after 'INSERT'."
      | map toLower command == "drop" && map toLower (head q) == "table" = identifyDropTable (tail q)
      |map toLower command == "create" = case map toLower (head q) of
          "table" ->
            case identifyCreateTable (tail q) of
              Left err -> Left err
              Right result -> Right result
          _ -> Left "Missing 'INTO' keyword after 'INSERT'."          
      | otherwise = Left "Wrong query syntax"

    identifyCreateTable :: [String] -> Either String ParsedStatement
    identifyCreateTable input = case input of
        (tableName : rest) -> Right $ CreateStatement tableName (parseColumns rest)
        _ -> Left "Failed to parse CREATE TABLE statement"

    -- Remove leading and trailing parentheses and semicolons from a string
    removeParensAndSemicolons :: String -> [String]
    removeParensAndSemicolons = words . filter (`notElem` "(),")

    addWhitespaceBeforeSemicolon :: String -> String
    addWhitespaceBeforeSemicolon [] = []
    addWhitespaceBeforeSemicolon (x:xs)
      | x == ';'  = ' ' : ';' : addWhitespaceBeforeSemicolon xs
      | otherwise = x : addWhitespaceBeforeSemicolon xs
      
    -- Parse the column list
    parseColumns :: [String] -> [(String, String)]
    parseColumns [] = error "Invalid CREATE TABLE statement: Missing closing parenthesis or semicolon"
    parseColumns (";" : _) = []  -- End of column list
    parseColumns (columnName : dataType : rest) = (columnName, removeTrailingComma dataType) : parseColumns rest
    parseColumns _ = error "Invalid CREATE TABLE statement: Mismatched parenthesis or semicolon"

    -- Remove trailing commas from data type specifications and add a whitespace before removing the closing parenthesis
    removeTrailingComma :: String -> String
    removeTrailingComma str
        | last str == ')' = init str ++ " "
        | otherwise = str

    identifyDropTable :: [String] -> Either ErrorMessage ParsedStatement
    identifyDropTable [] = Left "Table name isn't specified"
    identifyDropTable [q] = Right (DropTableStatement q)
    identifyDropTable (q:_) = Left "Table should contain only one word"

                -- Function to split the query at whitespaces outside quotes
    splitOnWhitespaceInQuotes :: String -> [String]
    splitOnWhitespaceInQuotes s = Data.List.filter (not . Data.List.null) $ case Data.List.dropWhile isSpace s of
      "" -> []  -- If the string is empty, return an empty list.
      ('\'':cs) ->
        if checkConditionB cs
          then splitOnWhitespaceInQuotes (Data.List.dropWhile isSpace cs)
          else
            let (word, rest) = Data.List.break (== '\'') cs
            in word : splitOnWhitespaceInQuotes (Data.List.dropWhile isSpace rest)
      (c:cs) ->
        let (word, rest) = Data.List.break isSpace s
        in if word `Data.List.elem` [", ", "),"] then splitOnWhitespaceInQuotes (Data.List.dropWhile isSpace rest)
          else word : splitOnWhitespaceInQuotes (Data.List.dropWhile isSpace rest)

    checkConditionB :: String -> Bool
    checkConditionB cs = case Data.List.dropWhile (/= '\'') cs of
      [] -> False
      (_:')':_) -> True
      _ -> False
    
    splitOnWhitespaceInQuotes2 :: String -> [String]
    splitOnWhitespaceInQuotes2 s =
      Data.List.filter (not . Data.List.null) $ case Data.List.dropWhile isSpace s of
        "" -> []  -- If the string is empty, return an empty list.
        ('\'':cs) ->
          let (word, rest) = Data.List.break (== '\'') cs
          in ('\'':word Data.List.++ "'") : splitOnWhitespaceInQuotes2 (Data.List.dropWhile isSpace (Data.List.tail rest))
        (c:cs) ->
          let (word, rest) = Data.List.break (\x -> isSpace x) s
          in word : splitOnWhitespaceInQuotes2 (Data.List.dropWhile isSpace rest)

    splitSelectStatement :: [String] -> Either ErrorMessage ParsedStatement
    splitSelectStatement q = do
        (a, b) <- splitColumns [] q
        (names, rest) <- distinguishTableNames b
        case splitByWord "order by" (Data.List.unwords rest) of
          [""] -> parseSelectStatement a names [] [] Ascending
          [conditions] -> parseSelectStatement a names (Data.List.words conditions) [] Ascending
          [conditions, afterConditions] -> do
            (cols, mode) <- parseOrderColumns $ Data.List.unwords $ Data.List.words afterConditions
            parseSelectStatement a names (Data.List.words conditions) cols mode
          where
            parseOrderColumns :: String -> Either ErrorMessage ([String], SortMode)
            parseOrderColumns [] = Left "We should not get here 🤡🤡🤡🤡🤡🤡"
            parseOrderColumns str = do
              let cols = Data.List.words $ removeCommas str
              case Data.List.map toLower $ Data.List.last cols of
                "asc" -> Right (Data.List.init cols, Ascending)
                "desc" -> Right (Data.List.init cols, Descending)
                [] -> Right ([], Ascending)
                _ -> Right (cols, Ascending)

            removeCommas :: String -> String
            removeCommas = Data.List.filter (`Data.List.notElem` [','])


    splitUpdateStatement :: [String] -> Either ErrorMessage ParsedStatement
    splitUpdateStatement queryTokens =
      -- Check if the query contains the necessary components
      if Data.List.elemIndex "set" (Data.List.map (Data.List.map toLower) queryTokens) /= Nothing
        then do
          -- Extract and process the relevant parts of the query
          let lowershit = splitByWord "set" $ Data.List.unwords queryTokens
          let tableName = Data.List.head $ Data.List.words $ Data.List.head lowershit
          let restAfterUpdate = Data.List.unwords $ Data.List.words $ lowershit Data.List.!! 1
          if Data.List.null tableName
            then Left "Invalid Table Name." 
            else do
              let whereSplit = splitByWord "where" restAfterUpdate
              case whereSplit of
                [pairs] -> do
                  let updatePairs = Data.List.words pairs
                  let (valuesFinal, updatedColumns) = parsePairs updatePairs
                  let cols = Data.List.filter (not . Data.List.null) (Data.List.map cleanName valuesFinal)
                  let values = Data.List.filter (not . Data.List.null) (Data.List.map cleanName updatedColumns)
                  if Data.List.length values /= Data.List.length updatedColumns
                    then Left "Invalid Syntax. Value count doesnt match column count" 
                    else do
                      Right (UpdateStatement tableName cols values "")
                _ -> do
                  let updatePairs = Data.List.words (Data.List.head whereSplit)
                  let conditions = Data.List.unwords $ Data.List.words $ whereSplit Data.List.!! 1
                  let (valuesFinal, updatedColumns) = parsePairs updatePairs
                  let cols = Data.List.filter (not . Data.List.null) (Data.List.map cleanName valuesFinal)
                  let values = Data.List.filter (not . Data.List.null) (Data.List.map cleanName updatedColumns)
                  if Data.List.length values /= Data.List.length updatedColumns 
                    then Left "Invalid Syntax. Value count doesnt match column count" 
                    else do
                      let combinedConditions = conditions
                      Right (UpdateStatement tableName cols values (Data.List.init combinedConditions))
          else do
            traceShow (Data.List.map (Data.List.map toLower) queryTokens) $ return ()
            Left "Invalid UPDATE query syntax."

    parsePairs :: [String] -> ([String], [String])
    parsePairs [] = ([], [])  -- Base case for an empty list
    parsePairs (x:xs)
        | "=" `Data.List.isInfixOf` x =  -- Check if the string contains "="
            let (columns, values) = parsePairs (Data.List.tail xs)
            in (columns, (Data.List.head xs): values)  -- Using tail to skip the "="
        | otherwise =         
            let (columns, values) = parsePairs xs
            in (x : columns, values)

    -- Parse delete statement
    splitDeleteStatement :: [String] -> Either ErrorMessage ParsedStatement
    splitDeleteStatement query = do
      if Data.List.null query || Data.List.map toLower (Data.List.head query) /= "from"
        then Left "Invalid DELETE syntax: Missing 'FROM' keyword."
        else do
          let restWithoutFrom = Data.List.tail query
          if Data.List.null (Data.List.tail restWithoutFrom)
            then do
              Right (DeleteStatement (Data.List.head restWithoutFrom) "")
            else do
              if Data.List.map toLower (Data.List.head (Data.List.tail restWithoutFrom)) /= "where" || Data.List.length (Data.List.tail restWithoutFrom) < 2
                then Left "Invalid DELETE syntax: Either Table Name Is Wrong Or Missing 'WHERE' keyword Or Conditions After It"
                else do
                  let (table, restWithoutTable) = Data.List.span (\s -> Data.List.map toLower s /= Data.List.map toLower "where") restWithoutFrom
                  let (conditions, _) = Data.List.span (\s -> Data.List.map toLower s /= Data.List.map toLower "where") (Data.List.tail restWithoutTable)
                  if Data.List.null table
                    then Left "Invalid DELETE syntax."
                    else do
                      Right (DeleteStatement (Data.List.unwords table) (Data.List.unwords conditions))

    -- splits columns until finds from (if where or select - error)
    splitColumns :: [String] -> [String] -> Either ErrorMessage ([String], [String])
    splitColumns cols [] = Left "Wrong query syntax"
    splitColumns cols w
      | Data.List.map toLower (Data.List.head w) == "where" || Data.List.map toLower (Data.List.head w) == "select" = Left "Wrong SELECT/WHERE placement/count."
      | Data.List.map toLower (Data.List.head w) == "from" && Data.List.null cols = Left "No columns specified"
      | Data.List.map toLower (Data.List.head w) == "from" = Right (cols, Data.List.tail w)
      | otherwise = splitColumns (cols Data.List.++ splitByComma (Data.List.head w)) (Data.List.tail w)
      where
        -- Helper function to split a string by commas
        splitByComma :: String -> [String]
        splitByComma str = Data.List.filter (not . Data.List.null) $ Data.List.words $ Data.List.map (\c -> if c == ',' then ' ' else c) str

    -- INSERT INTO table (cols) VALUES (vals), (vals), ...
    identifyInsertValues :: [String] -> Either String ParsedStatement
    identifyInsertValues [] = Left "Specify table name and values."
    identifyInsertValues (table:rest) = do
      let (cols, restWithoutValues) = Data.List.span (\s -> Data.List.map toLower s /= Data.List.map toLower "values") rest
      let valuesBlock = Data.List.dropWhile (\s -> Data.List.map toLower s == Data.List.map toLower "values") restWithoutValues
      if Data.List.null cols || Data.List.null valuesBlock
        then do
          Left "Invalid INSERT syntax."
      else do
        ()   <- validateSpaces cols      
        str  <- removeBrackets (Data.List.concat cols)
        ()   <- checkFirstChar (Data.List.head str)
        ()   <- validateCommaSyntax str
        if Data.List.all (Data.List.all (not . Data.List.null)) (removeSpaces (Data.List.map removeBracketsFromList (groupStrings (Data.List.map cleanName valuesBlock)))) && Data.List.all (not . Data.List.null) (splitItemsByComma str)
          then do
            Right (InsertStatement table (splitItemsByComma str) (removeSpaces (Data.List.map removeBracketsFromList (groupStrings (Data.List.map cleanName valuesBlock)))))
        else
            Left "Invalid INSERT syntax: Empty strings in valuesLines or columns."

    validateSpaces :: [String] -> Either String ()
    validateSpaces cols = 
        if Data.List.words (changeCommasIntoSpaces (Data.List.concat cols)) == Data.List.words (changeCommasIntoSpaces (Data.List.unwords cols)) 
            then Right ()
        else Left "Wrong syntax: comma missing in-between columns."

    checkFirstChar :: Char -> Either String ()
    checkFirstChar c = if c == ',' then Left "No columns specified before first comma" else Right ()

    removeSpaces :: [[String]] -> [[String]]
    removeSpaces = Data.List.map (Data.List.filter (/= " "))

    removeBrackets :: String -> Either String String
    removeBrackets cols =
        case (Data.List.head cols, Data.List.last cols) of
            ('(', ')') -> Right (Data.List.init (Data.List.tail cols))
            _ -> Left "Wrong syntax: missing ( or ) surrounding columns."

    removeBracketsFromList :: [String] -> [String]
    removeBracketsFromList = Data.List.map (Data.List.filter (`Data.List.notElem` ['(', ')', ',', '[', ']', '{', '}']))

    -- Function to check if a string starts with '('
    startsWithBracket :: String -> Bool
    startsWithBracket str = Data.List.head str == '('

    -- Function to check if a string ends with ')' or '),'
    endsWithBracket :: String -> Bool
    endsWithBracket str = Data.List.last str == ')' || Data.List.last str == ','

    -- Function to group strings based on the bracket conditions
    groupStrings :: [String] -> [[String]]
    groupStrings = Data.List.groupBy (\x y -> (not (startsWithBracket y)) && (not (endsWithBracket x)))

    -- Clean the column or table name from potential extra characters
    cleanName :: String -> String
    cleanName = Data.List.filter (`Data.List.notElem` [',', ';', '\''])

    -- makes a parsed select depending if there is where clause or not
    parseSelectStatement :: [String] -> [String] -> [String] -> [String] -> SortMode -> Either ErrorMessage ParsedStatement
    parseSelectStatement cols names [_] orderColumns sortMode = Left "Conditions needed after WHERE."
    parseSelectStatement cols names [] orderColumns sortMode = Right (SelectStatement cols names "" orderColumns sortMode)
    parseSelectStatement cols names conditions orderColumns sortMode = Right (SelectStatement cols names (Data.List.unwords (Data.List.tail conditions)) orderColumns sortMode)

    -- SHOW TABLE table_name identification 
    identifyShowTableName :: [String] -> Either ErrorMessage ParsedStatement
    identifyShowTableName [] = Left "Specify table name."
    identifyShowTableName (_:_:_) = Left "Table name should contain one word."
    identifyShowTableName (w:ws) = Right (ShowTable w)

distinguishColumnNames :: [String] -> Either ErrorMessage ([String], [String])
distinguishColumnNames rest = 
    case findFrom [] rest of
      Left err -> Left err
      Right (cols, tablesAndConditions) -> case Data.List.words (changeCommasIntoSpaces (Data.List.concat cols)) == Data.List.words (changeCommasIntoSpaces (Data.List.unwords cols)) of
                False -> Left "Wrong syntax: incorrect commas between columns."
                True -> case Data.List.concat cols of
                    [] -> Left "Wrong syntax"
                    (c:cs) -> if c == ',' then Left "No columns specified before first comma" else 
                        case validateCommaSyntax cs of
                            Left err -> Left err
                            Right () -> case validateTableNames (splitItemsByComma (Data.List.concat cols)) of
                                Left err -> Left err
                                Right () -> Right (splitItemsByComma (Data.List.concat cols), tablesAndConditions)


findFrom :: [String] -> [String] -> Either ErrorMessage ([String], [String])
findFrom cols [] = Left "Wrong syntax: missing 'FROM' statement"
findFrom cols words =
    if Data.List.map toLower (Data.List.head words) == "from" then Right (cols, (Data.List.tail words)) else findFrom (cols Data.List.++ [Data.List.head words]) (Data.List.tail words)

distinguishTableNames :: [String] -> Either ErrorMessage ([String], [String])
distinguishTableNames rest = 
    let (tables, conditions) = findWhere [] rest
    in case Data.List.words (changeCommasIntoSpaces (Data.List.concat tables)) == Data.List.words (changeCommasIntoSpaces (Data.List.unwords tables)) of
        False -> Left ((changeCommasIntoSpaces (Data.List.concat tables)) Data.List.++ " != " Data.List.++ (changeCommasIntoSpaces (Data.List.unwords tables)))
        True -> case Data.List.concat tables of
            [] -> Left "Wrong syntax: no tables found"
            (c:cs) -> if c == ',' then Left "No columns specified before first comma" else 
                case validateCommaSyntax cs of
                    Left err -> Left err
                    Right () -> case validateTableNames (splitItemsByComma (Data.List.concat tables)) of
                        Left err -> Left err
                        Right () -> Right (splitItemsByComma (Data.List.concat tables), conditions)

validateTableNames :: [String] -> Either ErrorMessage ()
validateTableNames [] = Right ()
validateTableNames (n:ns)
  | Data.List.map toLower n == "where" || Data.List.map toLower n == "select" || Data.List.map toLower n == "from" = Left "Wrong syntax(keywords cannot be table names)"
  | otherwise = validateTableNames ns

splitItemsByComma :: String -> [String]
splitItemsByComma str = Data.List.words $ Data.List.map (\c -> if c == ',' then ' ' else c) str

validateCommaSyntax :: String -> Either ErrorMessage ()
validateCommaSyntax [] = Right ()
validateCommaSyntax (c:cs)
    | c == ',' = if Data.List.null cs 
        then 
            Left "Wrong syntax: comma can't be last symbol after table name"
        else
            case Data.List.head cs == ',' of
                False -> validateCommaSyntax (Data.List.tail cs)
                True -> Left "Wrong syntax(no column name between commas)"
    | otherwise = validateCommaSyntax cs

findWhere :: [String] -> [String] -> ([String], [String])
findWhere tables [] = (tables, [])
findWhere tables words =
    if Data.List.map toLower (Data.List.head words) == "where" || Data.List.map toLower (Data.List.head words) == "order" then (tables, words) else findWhere (tables Data.List.++ [Data.List.head words]) (Data.List.tail words)

changeCommasIntoSpaces :: String -> String
changeCommasIntoSpaces =  Data.List.map (\c -> if c == ',' then ' ' else c)

-- Executes a parsed statemet. Produces a DataFrame. Uses
-- InMemoryTables.databases a source of data.
-- execute SHOW TABLE table_name;
executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement (ShowTable table_name) =
  case Data.List.lookup table_name database of
    Just result -> Right $ DataFrame [Column "Columns" StringType] (Data.List.map (\(Column name _) -> [StringValue name]) (columns result))
    Nothing -> Left ("Table " Data.List.++ table_name Data.List.++ " doesn't exist")
  where
    columns :: DataFrame -> [Column]
    columns (DataFrame cols _) = cols

-- execute SHOW TABLES;
executeStatement ShowTables =
  Right $ DataFrame [Column "Tables" StringType] (Data.List.map (\(name, _) -> [StringValue name]) database)

--execute SELECT cols FROM table WHERE ... AND ... AND ...;
executeStatement (SelectStatement cols tables conditions orderCols sortMode) =
  case applyConditions conditions tables dataFrames of
    Left err -> Left err
    Right df ->
      case sortDataFrame df orderCols sortMode of
        (DataFrame c r) ->
          if validateColumns (Data.List.map (\(Column name _) -> name) c) c cols
            then case executeSelect c r cols of
              Left err -> Left err
              Right result -> Right result
          else if checkIfAll cols
            then Right (DataFrame c r)
          else
            Left "(Some of the) Column(s) not found."
  where
    --Cia gaunami DataFrame is tableName
    dataFrames = toDataFrames tables

    toDataFrames :: [String] -> [DataFrame]
    toDataFrames tables = Data.List.map getDataFrame tables
      where
        getDataFrame :: String -> DataFrame
        getDataFrame name = case Data.List.lookup name database of
          Just df -> df
          Nothing -> DataFrame [] []
    --

    checkIfAll :: [String] -> Bool
    checkIfAll [n] =
      n == "*"
    checkIfAll _ = False

    -- validates whether columns exist
    validateColumns :: [String] -> [Column] -> [String] -> Bool
    validateColumns columns c [] = True
    validateColumns columns c (n:ns) =
      (Data.List.elem n columns || checkIfSumOrMin n c) && validateColumns columns c ns

    checkIfSumOrMin :: String -> [Column] -> Bool
    checkIfSumOrMin str columns
        | ("sum(" `Data.List.isPrefixOf` (Data.List.map toLower str) || "min(" `Data.List.isPrefixOf`Data.List.map toLower str) && Data.List.last str == ')' =
            checkAgainColumn (Data.List.drop 4 (Data.List.init str)) columns
        | otherwise = False

        -- Define a sample function to check the column
    checkAgainColumn :: String -> [Column] -> Bool
    checkAgainColumn columnName columns =
        case Data.List.any (\(Column name _) -> name == columnName) columns of
            True -> checkIfInteger (Data.List.filter (\(Column name _) -> name == columnName) columns)
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
        findColumnIndex columnName columns = Data.List.elemIndex columnName (Data.List.map (\(Column colName _) -> colName) columns)

        -- Extract values from a specified column in a list of rows
        getValuesInColumn :: Int -> [Row] -> [Integer]
        getValuesInColumn columnIndex rows = [value | (IntegerValue value) <- Data.List.map (Data.List.!! columnIndex) rows]

        -- Calculate the minimum value in a specified column of a DataFrame
        minim :: DataFrame -> String -> Either ErrorMessage Integer
        minim (DataFrame columns rows) columnName =
            case findColumnIndex columnName columns of
                Just columnIndex ->
                    case getValuesInColumn columnIndex rows of
                        [] -> Left "No values in the specified column."
                        values -> Right (Data.List.foldr1 min values)
                Nothing -> Left "Column not found in the DataFrame."

        -- Calculate the sum of values in a specified column of a DataFrame
        sum_n :: DataFrame -> String -> Either ErrorMessage Integer
        sum_n (DataFrame columns rows) columnName =
            case findColumnIndex columnName columns of
                Just columnIndex ->
                    case getValuesInColumn columnIndex rows of
                        values -> Right (Data.List.sum values)
                Nothing -> Left "Column not found in the DataFrame."

        filterRows :: [Column] -> [Row] -> [String] -> [Row]
        filterRows _ [] _ = []
        filterRows cols (row:restRows) selectedColumnNames =
          let
            extractValues :: [Column] -> Row -> Row
            extractValues selectedCols values = [val | (col, val) <- Data.List.zip cols values, col `Data.List.elem` selectedCols]

            filteredRow = extractValues selectedColumns row

            filteredRestRows = filterRows cols restRows selectedColumnNames
          in
            filteredRow : filteredRestRows

        splitByBracket :: String -> (String, String)
        splitByBracket s =
          let
            tuple = Data.List.splitAt 3 (Data.List.init s)
          in
            (fst tuple, Data.List.tail (snd tuple))

        extractFunctions :: [String] -> ([String], [(String, String)]) -> ([String], [(String, String)])
        extractFunctions [] cols = cols
        extractFunctions (c:cs) (cols, functions) =
          case Data.List.take 4 (Data.List.map toLower c) of
            "min(" -> extractFunctions cs (cols, functions Data.List.++ [splitByBracket c])
            "sum(" -> extractFunctions cs (cols, functions Data.List.++ [splitByBracket c])
            _ ->  extractFunctions cs (cols Data.List.++ [c], functions)

        -- -- cia baigta
        combineRowsAndFunctions :: [Row] -> [(String, String)] -> [Column] -> [Row] -> [Row]
        combineRowsAndFunctions rez [] _ _ = rez
        combineRowsAndFunctions r (t:ts) columns rows = combineRowsAndFunctions (Data.List.map (addValueToRow t columns rows) r) ts columns rows
          where
            addValueToRow :: (String, String) -> [Column] -> [Row] -> Row -> Row
            addValueToRow t columns rows row =
              if Data.List.map toLower (fst t) == "min"
                then
                  case minim (DataFrame columns rows) (snd t) of
                    Left err -> row
                    Right rez -> row Data.List.++ [IntegerValue rez]
              else
                case sum_n (DataFrame columns rows) (snd t) of
                  Left err -> row
                  Right rez -> row Data.List.++ [IntegerValue rez]


        extractColumns :: [String] -> [Column] -> [Column]
        extractColumns names cols = Data.List.filter (\(Column name _) -> name `Data.List.elem` names) cols

        combineColsAndFunctions :: [Column] -> [(String, String)] -> [Column]
        combineColsAndFunctions rez [] = rez
        combineColsAndFunctions rez (t:ts) = combineColsAndFunctions (addValueToCols rez t) ts
          where
            addValueToCols :: [Column] -> (String, String) -> [Column]
            addValueToCols c tup =
              if Data.List.map toLower (fst tup) == "min"
                then
                  c Data.List.++ [Column ("MIN(" Data.List.++ (snd tup) Data.List.++ ")") IntegerType]
              else
                  c Data.List.++ [Column ("SUM(" Data.List.++ (snd tup) Data.List.++ ")") IntegerType]

        -- reikia atskirt kur yra column name ir kur yra min ir sum
        fun = extractFunctions selectedColumnNames ([], [])
        filteredRows = filterRows columns rows (fst fun)
        allRows = combineRowsAndFunctions filteredRows (snd fun) columns rows
        selectedColumns = extractColumns selectedColumnNames columns
        allCols = combineColsAndFunctions selectedColumns (snd fun)
      in
        if Data.List.null (snd fun)
          then Right (DataFrame allCols allRows)
          else Right (DataFrame allCols [Data.List.head allRows])
--------------------------------------
sortDataFrame :: DataFrame -> [String] -> SortMode -> DataFrame
sortDataFrame (DataFrame columns rows) colNames mode =
  DataFrame columns $ Data.List.sortBy (compareRows colIndices) rows
  where
    colIndices = Data.List.map (getColumnIndex columns) colNames

    compareRows :: [Int] -> Row -> Row -> Ordering
    compareRows [] _ _ = EQ
    compareRows (idx:rest) row1 row2 =
      case compareValues (row1 Data.List.!! idx) (row2 Data.List.!! idx) of
        EQ -> compareRows rest row1 row2
        result -> if mode == Ascending then result else flipOrder result

    compareValues :: Value -> Value -> Ordering
    compareValues (IntegerValue x) (IntegerValue y) = compare x y
    compareValues (StringValue x) (StringValue y) = compare x y
    compareValues (BoolValue x) (BoolValue y) = compare x y
    compareValues NullValue NullValue = EQ
    compareValues NullValue _ = LT
    compareValues _ NullValue = GT

    getColumnIndex :: [Column] -> String -> Int
    getColumnIndex columns' name =
      case Data.List.lookup name $ Data.List.zip (Data.List.map extractColumnName columns') [0..] of
        Just idx -> idx
        Nothing -> error $ "Column " Data.List.++ name Data.List.++ " not found in DataFrame."

    extractColumnName :: Column -> String
    extractColumnName (Column name _) = name

    flipOrder :: Ordering -> Ordering
    flipOrder LT = GT
    flipOrder EQ = EQ
    flipOrder GT = LT
----------------------------------------------------------------

-- Filters a DataFrame table by statement conditions
applyConditions :: String -> [String] -> [DataFrame] -> Either ErrorMessage DataFrame
--Jeigu yra tik vienas tableName
applyConditions conditions [tableName] [table] = do
    case table of
      (DataFrame columns rows) ->
        if Data.List.null conditions
          then return (DataFrame (renameColumns tableName columns) rows)
          else return (DataFrame (renameColumns tableName columns) (filterRows conditions (DataFrame (renameColumns tableName columns) rows) rows))

--Jeigu yra daugiau
applyConditions conditions tableNames tables = do
  let joinedTable = joinTables tableNames tables
  case joinedTable of
        (DataFrame columns rows) ->
          if Data.List.null conditions
            then return (DataFrame columns rows)
            else return (DataFrame columns (filterRows conditions joinedTable rows))

----------------------
--For joining tables--
----------------------
joinTables :: [String] -> [DataFrame] -> DataFrame
joinTables tableNames tables =
  Data.List.foldl (\acc (tableName, df) -> joinTwoTables (Data.List.head tableNames) acc tableName df) baseTable restTables
  where
    baseTable = Data.List.head tables
    restTables =  Data.List.zip (Data.List.tail tableNames) (Data.List.tail tables)

joinTwoTables :: TableName -> DataFrame -> TableName -> DataFrame -> DataFrame
joinTwoTables tableName1 df1 tableName2 df2 =
  let commonColumns = findCommonColumns (renameColumns tableName1 (getColumns df1)) (renameColumns tableName2 (getColumns df2))
      df1Renamed = DataFrame (renameColumns tableName1 (getColumns df1)) (getRows df1)
      df2Renamed = DataFrame (renameColumns tableName2 (getColumns df2)) (getRows df2)
      combinedDF = rowMultiplication df1Renamed df2Renamed
  in removeDuplicateColumns commonColumns combinedDF

findCommonColumns :: [Column] -> [Column] -> [String]
findCommonColumns cols1 cols2 =
  [colName1 | Column colName1 _ <- cols1, Data.List.any (\(Column colName2 _) -> colName1 == colName2) cols2]

renameColumns :: TableName -> [Column] -> [Column]
renameColumns tableName cols =
  [Column (tableName Data.List.++ "." Data.List.++ colName) colType | Column colName colType <- cols]

rowMultiplication :: DataFrame -> DataFrame -> DataFrame
rowMultiplication (DataFrame cols1 rows1) (DataFrame cols2 rows2) =
  DataFrame (cols1 Data.List.++ cols2) [row1 Data.List.++ row2 | row1 <- rows1, row2 <- rows2]

removeDuplicateColumns :: [String] -> DataFrame -> DataFrame
removeDuplicateColumns commonColumns (DataFrame cols rows) =
  DataFrame (Data.List.nubBy (\(Column col1 _) (Column col2 _) -> col1 == col2) cols) rows

getColumns :: DataFrame -> [Column]
getColumns dataFrame = case dataFrame of
  (DataFrame columns rows) -> columns

getRows :: DataFrame -> [Row]
getRows dataFrame = case dataFrame of
  (DataFrame columns rows) -> rows

-- Helper function to find a table by name in the database
findTableByName :: String -> (TableName, DataFrame)
findTableByName tableName =
  case Data.List.lookup tableName database of
    Just df -> (tableName, df)
    Nothing -> error ("Table not found: " Data.List.++ tableName)
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
      case Data.List.words input of
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
          Data.List.elemIndex columnName (Data.List.map (\(Column name _) -> name) columns)

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
    | Data.List.null conditions = Right True
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
    | word `Data.List.isPrefixOf` Data.List.map toLower input = "" : splitByWord word (Data.List.drop (Data.List.length word) input)
    | otherwise = (x : Data.List.head rest) : Data.List.tail rest
    where
        rest = splitByWord word xs
