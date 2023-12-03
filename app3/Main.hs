module Main (main) where

import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Free (Free (..))

import Data.Functor((<&>))
import Data.Time ( UTCTime, getCurrentTime )
import Data.List qualified as L
import Lib1 qualified
import Lib2 qualified
import Lib3 qualified
import Data.Aeson (decode)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.ByteString.Lazy.Char8 as BSL
import DataFrame (ColumnType(..), Column(..), Value(..), Row, DataFrame(..))
import System.Console.Repline
  ( CompleterStyle (Word),
    ExitDecision (Exit),
    HaskelineT,
    WordCompleter,
    evalRepl,
  )
import System.Console.Terminal.Size (Window, size, width)
import System.Directory (listDirectory, doesFileExist)
import System.FilePath (dropExtension, pathSeparator, takeExtension)

type Repl a = HaskelineT IO a

final :: Repl ExitDecision
final = do
  liftIO $ putStrLn "Goodbye!"
  return Exit

ini :: Repl ()
ini = liftIO $ putStrLn "Welcome to select-manipulate database! Press [TAB] for auto completion."

completer :: (Monad m) => WordCompleter m
completer n = do
  let names = [
              "select", "*", "from", "show", "table",
              "tables", "insert", "into", "values",
              "set", "update", "delete"
              ]
  return $ Prelude.filter (L.isPrefixOf n) names

-- Evaluation : handle each line user inputs
cmd :: String -> Repl ()
cmd c = do
  s <- terminalWidth <$> liftIO size
  result <- liftIO $ cmd' s
  case result of
    Left err -> liftIO $ putStrLn $ "Error: " ++ err
    Right table -> liftIO $ putStrLn table
  where
    terminalWidth :: (Integral n) => Maybe (Window n) -> n
    terminalWidth = maybe 80 width
    cmd' :: Integer -> IO (Either String String)
    cmd' s = do
      df <- runExecuteIO $ Lib3.executeSql c 
      return $ Lib1.renderDataFrameAsTable s <$> df

main :: IO ()
main =
  evalRepl (const $ pure ">>> ") cmd [] Nothing Nothing (Word completer) ini final

runExecuteIO :: Lib3.Execution r -> IO r
runExecuteIO (Pure r) = return r
runExecuteIO (Free step) = do
    next <- runStep step
    runExecuteIO next
    where
        -- probably you will want to extend the interpreter
        runStep :: Lib3.ExecutionAlgebra a -> IO a
        runStep (Lib3.GetTime next) = do
          time <- getCurrentTime
          return $ next time
        runStep (Lib3.LoadFile table_name next) = do
          existingFile <- doesFileExist ("db" ++ [pathSeparator] ++ table_name ++ ".json")
          if existingFile 
            then do
              jsonData <- readFile ("db" ++ [pathSeparator] ++ table_name ++ ".json")
              case decode (BSL.fromStrict $ TE.encodeUtf8 $ T.pack jsonData) :: Maybe DataFrame of
                Just df -> return $ next $ Right df
                Nothing -> return $ next $ Left "Wrong data format"
          else return $ next $ Left ("Table " ++ table_name ++ " does not exist.")
        runStep (Lib3.SaveFile (table_name, df) next) = 
          do
            writeFile ("db" ++ [pathSeparator] ++ table_name ++ ".json") (Lib3.dataFrameToJson df)
            return $ next (table_name, df)
        runStep (Lib3.GetTableNames next) = do
            files <- listDirectory "db"
            let tableNames = foldl (\acc fileName -> if takeExtension fileName == ".json" then dropExtension fileName : acc else acc) [] files
            return $ next tableNames
          


