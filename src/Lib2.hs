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
import DataFrame (DataFrame)
import InMemoryTables (TableName)
import Data.Char (toLower)
import Data.Maybe (fromMaybe)

type ErrorMessage = String
type Database = [(TableName, DataFrame.DataFrame)]

-- Keep the type, modify constructors
data ParsedStatement
  = ShowTable {command :: String, table_name :: String}
  | ShowTables {command :: String }
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
    identifyCommand [] = Left "Empty query"
    identifyCommand (w:ws) =
      case validCommand w ws of
        Left err -> Left err
        Right rest -> Right rest

    -- function that reads the the keywords
    validCommand :: String -> [String] -> Either ErrorMessage ParsedStatement
    validCommand _ [] = Left "Too few arguments"
    validCommand command (w:ws) =
      if map toLower command == "show"
        then case w of
          "table" ->
            case identifyTableName ws of
              Left err -> Left err
              Right result -> Right result
          "tables" ->
            if null ws
              then Right (ShowTables "tables")
            else
              Left "Wrong query syntax"
      else
        Left "Wrong query syntax"

    identifyTableName :: [String] -> Either ErrorMessage ParsedStatement
    identifyTableName [] = Left "Specify table name"
    identifyTableName (_:_:_) = Left "Table name should contain one word"
    identifyTableName (w:ws) = Right (ShowTable "table" w)

-- Executes a parsed statemet. Produces a DataFrame. Uses
-- InMemoryTables.databases a source of data.
executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement _ = Left "Not implemented: executeStatement"
