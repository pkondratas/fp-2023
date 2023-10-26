{-# OPTIONS_GHC -Wno-unused-top-binds #-}
{-# OPTIONS_GHC -Wno-incomplete-patterns #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}
{-# HLINT ignore "Use isJust" #-}
{-# OPTIONS_GHC -Wno-unused-matches #-}

module Lib2
  ( parseStatement,
    executeStatement,
    ParsedStatement
  )
where

import Data.List ( elemIndex )
import DataFrame (DataFrame (DataFrame), Column (Column), ColumnType (StringType), Value (StringValue, IntegerValue, NullValue), Row)
import InMemoryTables ( TableName, database )
import Data.Char (toLower)
import Data.Maybe (fromMaybe)

type ErrorMessage = String
type Database = [(TableName, DataFrame.DataFrame)]

-- Keep the type, modify constructors
data ParsedStatement
  = ShowTable String
  | ShowTables
  | Select {table :: String, conditions :: String}
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
    validCommand command (w:ws) =
      if map toLower command == "show"
        then case map toLower w of
          "table" ->
            case identifyTableName ws of
              Left err -> Left err
              Right result -> Right result
          "tables" ->
            if null ws
              then Right ShowTables
            else
              Left "Wrong query syntax."
          _ -> Left "Unsuported command/wrong syntax."
      else
        Left "Wrong query syntax"

    -- SHOW TABLE table_name identification 
    identifyTableName :: [String] -> Either ErrorMessage ParsedStatement
    identifyTableName [] = Left "Specify table name"
    identifyTableName (_:_:_) = Left "Table name should contain one word"
    identifyTableName (w:ws) = Right (ShowTable w)


-- Executes a parsed statemet. Produces a DataFrame. Uses
-- InMemoryTables.databases a source of data.
executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
-- executeStatement _ = Left "Not implemented: executeStatement"
executeStatement (ShowTable table_name) = 
  case lookup table_name database of
    Just result -> Right $ DataFrame [Column "Columns" StringType] (map (\(Column name _) -> [StringValue name]) (columns result))
    Nothing -> Left ("Table " ++ table_name ++ " doesn't exist")
  where
    columns :: DataFrame -> [Column]
    columns (DataFrame cols _) = cols

executeStatement ShowTables =
  Right $ DataFrame [Column "Tables" StringType] (map (\(name, _) -> [StringValue name]) database)

-- Sita atfiltruoja lentele
-- applyConditions :: String -> String -> Either ErrorMessage DataFrame

-- TODO for applyConditions:
-- Get conditions and table from parameters
-- Function to get all table rows
-- Iterate through the rows (x:xs) style
-- For each row check if conditions are true with checkAndCondition function
-- If yes, add the row to the result
-- Return result in the form of DataFrame

checkCondition :: String -> String -> Row -> Either ErrorMessage Bool
checkCondition condition table row = executeCondition (words condition)
  where
    -- Returns the operation value based on the operator provided in the condition string
    executeCondition :: [String] -> Either ErrorMessage Bool
    executeCondition (operand1 :  operator :  operand2 : _) =
      case operator of
        "=" -> Right (getOperandValue operand1 == getOperandValue operand2)
        "<>" -> Right (getOperandValue operand1 /= getOperandValue operand2)
        "!=" -> Right (getOperandValue operand1 /= getOperandValue operand2)
        "<" -> Right (getOperandValue operand1 < getOperandValue operand2)
        ">" -> Right (getOperandValue operand1 > getOperandValue operand2)
        "<=" -> Right (getOperandValue operand1 <= getOperandValue operand2)
        ">=" -> Right (getOperandValue operand1 >= getOperandValue operand2)
        _ -> Left "Incorrect operator"

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
