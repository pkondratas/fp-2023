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

type ErrorMessage = String

type Database = [(TableName, DataFrame)]

-- Your code modifications go below this comment

-- 1) implement the function which returns a data frame by its name
-- in provided Database list
findTableByName :: Database -> String -> Maybe DataFrame
findTableByName _ _ = error "findTableByName not implemented"

-- 2) implement the function which parses a "select * from ..."
-- sql statement and extracts a table name from the statement
parseSelectAllStatement :: String -> Either ErrorMessage TableName
parseSelectAllStatement _ = error "parseSelectAllStatement not implemented"

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
renderDataFrameAsTable _ _ = error "renderDataFrameAsTable not implemented"
