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

import Data.List ( elemIndex )
import DataFrame (DataFrame (DataFrame), Column (Column), ColumnType (StringType), Value (StringValue))
import InMemoryTables ( TableName, database )
import Data.Char (toLower)
import Data.Maybe (fromMaybe)

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

-- executeStatement select = 
--   case applyConditions table conditions of
--     Left err -> Left err
--     Right [c] [r] -> Right executeSelect c r select
--   where 
--     executeSelect :: [Column] -> [Row] -> ParsedStatement -> Either ErrorMessage DataFrame
--     executeSelect c r (SelectStatement cols table conditions) =
      

