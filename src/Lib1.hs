{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib1
  ( parseSelectAllStatement,
    findTableByName,
    validateDataFrame,
    renderDataFrameAsTable,
  )
where

import DataFrame (DataFrame (..), Row, Column (..), ColumnType (..), Value (..))
import InMemoryTables (TableName)

import Data.Maybe (listToMaybe)
import Data.Char (toLower)


type ErrorMessage = String

type Database = [(TableName, DataFrame)]

-- Your code modifications go below this comment

-- 1) implement the function which returns a data frame by its name
-- in provided Database list
findTableByName :: Database -> String -> Maybe DataFrame
findTableByName database targetedTableName = listToMaybe [dataFrame | (name, dataFrame) <- database, map toLower name == map toLower targetedTableName]

-- 2) implement the function which parses a "select * from ..."
-- sql statement and extracts a table name from the statement
parseSelectAllStatement :: String -> Either ErrorMessage TableName
parseSelectAllStatement statement = if null statement then Left "Input is empty"
else
  case words (map toLower statement) of
    ["select", "*", "from", tableName] -> Right (removeSemicolon tableName)
    _ -> Left "Invalid SQL statement"

removeSemicolon :: String -> String
removeSemicolon s = if not (null s) && last s == ';' then init s else s

-- 3) implement the function which validates tables: checks if
-- columns match value types, if rows sizes match columns,..
validateDataFrame :: DataFrame -> Either ErrorMessage ()
validateDataFrame (DataFrame columns rows) = validateRows rows
  where
    validateRows :: [Row] -> Either ErrorMessage ()
    validateRows [] = Right()
    validateRows (r:rs) = 
      case validateValueCount columns r of
        Left error -> Left error
        Right _ -> validateRows rs

    validateValueCount :: [Column] -> [Value] -> Either ErrorMessage ()
    validateValueCount [] [] = Right()
    validateValueCount [] (_:_) = Left "Too many values"
    validateValueCount (_:_) [] = Left "Not enough values"
    validateValueCount (c:cs) (v:vs) 
      | validateValueType c v = validateValueCount cs vs
      | otherwise = Left "Type of value is incorrect"

    
    validateValueType :: Column -> Value -> Bool
    validateValueType _ NullValue = True
    validateValueType (Column _ colType) v =
      case getValueType v of
        Left _ -> False
        Right valueType -> valueType == colType
        
    getValueType :: Value -> Either () ColumnType
    getValueType (IntegerValue _) = Right IntegerType
    getValueType (StringValue _) = Right StringType
    getValueType (BoolValue _) = Right BoolType
    getValueType _ = Left ()

-- 4) implement the function which renders a given data frame
-- as ascii-art table (use your imagination, there is no "correct"
-- answer for this task!), it should respect terminal
-- width (in chars, provided as the first argument)
renderDataFrameAsTable :: Integer -> DataFrame -> String
renderDataFrameAsTable n (DataFrame columns rows) = main ++ "\n" ++ add ++"\n" ++ aRows
    where
        cWidths = calculateCWidths n columns
        aRows = makeRows rows cWidths
        main = makeMain columns cWidths
        add = makeAdd n columns

        calculateCWidths :: Integer -> [Column] -> [Int]
        calculateCWidths maxWidth columnArray = 
            let columnNum = length columnArray
                cellWidth = (maxWidth - 2) `div` fromIntegral columnNum
            in replicate columnNum (fromIntegral cellWidth)

        makeAdd :: Integer -> [Column] -> String
        makeAdd maxWidth columnArray = 
            let columnNum = length columnArray
                cellWidth = (maxWidth - 2) `div` fromIntegral columnNum
            in replicate (columnNum * fromIntegral cellWidth + 3) '-'

        makeCell :: Value -> Int -> String
        makeCell value width = 
            let cellContent = case value of
                    StringValue s -> s 
                    IntegerValue i -> show i
                    BoolValue b -> if b then "True" else "False"
                    NullValue-> ""
                strLen = length cellContent
                emptySpace = width - strLen - 3
                emptySpace2 = if emptySpace `mod` 2 == 0
                      then emptySpace `div` 2
                      else emptySpace `div` 2 + 1
                in replicate (emptySpace `div` 2) ' ' ++ cellContent ++ replicate (emptySpace2) ' ' ++ "<#>"

        makeMain :: [Column] -> [Int] -> String
        makeMain allColumns cWidthArray =
            let mainCells = zipWith makeMCell allColumns cWidthArray
            in "<#>" ++ concat mainCells

        makeMCell :: Column -> Int -> String
        makeMCell (Column name _) w = makeCell (StringValue name) w

        makeRows :: [Row] -> [Int] -> String
        makeRows rowArray cWidth = 
            let readyRows = map (makeSingleRow cWidth) rowArray
            in unlines readyRows

        makeSingleRow :: [Int] -> Row -> String
        makeSingleRow widths row = 
            let cells = zipWith makeCell row widths
            in "<#>" ++ concat cells
