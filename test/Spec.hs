import Data.Either
import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Free (Free (..))
import Data.Functor((<&>))
import Data.List qualified as L
import Data.Maybe ()
import InMemoryTables qualified as D
import Lib1
import Lib2
import Lib3
import Test.Hspec
import Data.Time ( UTCTime, getCurrentTime )
import DataFrame (DataFrame (DataFrame), Column (Column), ColumnType (StringType, IntegerType), Value (StringValue, IntegerValue, NullValue), Row)
import InMemoryTables ( TableName, database )

runExecuteIO :: Lib3.Execution r -> IO r
runExecuteIO (Pure r) = return r
runExecuteIO (Free step) = do
    next <- runStep step
    runExecuteIO next
  where
    runStep :: Lib3.ExecutionAlgebra a -> IO a
    runStep (Lib3.LoadFile table_name next) =
      case lookup table_name database of
        Just df -> return $ next $ Right df
        Nothing -> return (next $ Left $ "Table " ++ table_name ++ " doesn't exist.")
    runStep (Lib3.SaveFile tuple next) = 
      return $ next tuple
    runStep (Lib3.GetTableNames next) = 
      return $ next ([tableName | (tableName, _) <- database])
    runStep (Lib3.GetTime next ) =
      return $ next testTime

testTime :: UTCTime
testTime = read "2023-12-24 19:00:00 UTC"


main :: IO ()
main = hspec $ do
  describe "Lib1.findTableByName" $ do
    it "handles empty lists" $ do
      Lib1.findTableByName [] "" `shouldBe` Nothing
    it "handles empty names" $ do
      Lib1.findTableByName D.database "" `shouldBe` Nothing
    it "can find by name" $ do
      Lib1.findTableByName D.database "employees" `shouldBe` Just (snd D.tableEmployees)
    it "can find by case-insensitive name" $ do
      Lib1.findTableByName D.database "employEEs" `shouldBe` Just (snd D.tableEmployees)
  describe "Lib1.parseSelectAllStatement" $ do
    it "handles empty input" $ do
      Lib1.parseSelectAllStatement "" `shouldSatisfy` isLeft
    it "handles invalid queries" $ do
      Lib1.parseSelectAllStatement "select from dual" `shouldSatisfy` isLeft
    it "returns table name from correct queries" $ do
      Lib1.parseSelectAllStatement "selecT * from dual;" `shouldBe` Right "dual"
  describe "Lib1.validateDataFrame" $ do
    it "finds types mismatch" $ do
      Lib1.validateDataFrame (snd D.tableInvalid1) `shouldSatisfy` isLeft
    it "finds column size mismatch" $ do
      Lib1.validateDataFrame (snd D.tableInvalid2) `shouldSatisfy` isLeft
    it "reports different error messages" $ do
      Lib1.validateDataFrame (snd D.tableInvalid1) `shouldNotBe` Lib1.validateDataFrame (snd D.tableInvalid2)
    it "passes valid tables" $ do
      Lib1.validateDataFrame (snd D.tableWithNulls) `shouldBe` Right ()
  describe "Lib1.renderDataFrameAsTable" $ do
    it "renders a table" $ do
      Lib1.renderDataFrameAsTable 100 (snd D.tableEmployees) `shouldSatisfy` not . null
  describe "Lib2.parseStatement" $ do
    it "parse SHOW TABLES;" $ do
      Lib2.parseStatement "SHOW TABLES;" `shouldBe` Right ShowTables
    it "parse Show      tabLeS;" $ do
      Lib2.parseStatement "Show      tabLeS;" `shouldBe` Right ShowTables
    it "parse SHOW TABLE aaa;" $ do
      Lib2.parseStatement "SHOW TABLE aaa;" `shouldBe` Right (ShowTable "aaa")
    it "parse sHoW TaBLe aaa;" $ do
      Lib2.parseStatement "sHoW TaBLe aaa;" `shouldBe` Right (ShowTable "aaa")
    it "parse SELECT col1 col2 FROM table1;" $ do
      Lib2.parseStatement "SELECT col1 col2 FROM table1;" `shouldBe` Right (SelectStatement ["col1", "col2"] ["table1"] "")
    it "parse SELECT col1 col2 FROM table1 WHERE a > b;" $ do
      Lib2.parseStatement "SELECT col1 col2 FROM table1 WHERE a > b;" `shouldBe` Right (SelectStatement ["col1", "col2"] ["table1"] "a > b")
    it "parse SELECT col1 col2 FROM table1 WHERE a > b AND a > c AND b > c;" $ do
      Lib2.parseStatement "SELECT col1 col2 FROM table1 WHERE a > b AND a > c AND b > c;" `shouldBe` Right (SelectStatement ["col1", "col2"] ["table1"] "a > b AND a > c AND b > c")
    it "parse Show tabLeS asdsadsad;" $ do
      Lib2.parseStatement "Show tabLeS asdsadsad;" `shouldSatisfy` isLeft
    it "parse Show tabLe;" $ do
      Lib2.parseStatement "Show tabLe;" `shouldSatisfy` isLeft
    it "parse Show tabLe asdsad asdsad;" $ do
      Lib2.parseStatement "Show tabLe asdsad asdsad;" `shouldSatisfy` isLeft
    it "parse SELECT col1 col2 FROM table1 WHERE;" $ do
      Lib2.parseStatement "SELECT col1 col2 FROM table1 WHERE;" `shouldSatisfy` isLeft
    it "parse SELECT col1 col2 FROM table1 aaaaa;" $ do
      Lib2.parseStatement "SELECT col1 col2 FROM table1 aaaaa;" `shouldSatisfy` isLeft
    it "parse SELECT col1 col2 FROM table1 aaaaa;" $ do
      Lib2.parseStatement "SELECT col1 col2 FROM table1 aaaaa;" `shouldSatisfy` isLeft
    it "parse SELECT col1 col2 FROM;" $ do
      Lib2.parseStatement "SELECT col1 col2 FROM;" `shouldSatisfy` isLeft
    it "parse SELECT col1 col2 FROM;" $ do
      Lib2.parseStatement "SELECT col1 col2;" `shouldSatisfy` isLeft
    it "parse SELECT FROM;" $ do
      Lib2.parseStatement "SELECT col1 col2;" `shouldSatisfy` isLeft

  describe "Lib2.executeStatement" $ do
    it "returns a DataFrame for an existing table" $ do
      Lib2.executeStatement (ShowTable "employees") `shouldBe` Right (DataFrame [Column "Columns" StringType] [[StringValue "id"], [StringValue "name"], [StringValue "surname"]])
    
    it "returns an error message for a non-existing table" $ do
      Lib2.executeStatement (ShowTable "non_existing_table") `shouldSatisfy`  isLeft
    
    it "returns a DataFrame for ShowTables" $ do
      Lib2.executeStatement ShowTables `shouldBe` Right (DataFrame [Column "Tables" StringType] [[StringValue "employees"], [StringValue "invalid1"], [StringValue "invalid2"], [StringValue "long_strings"], [StringValue "flags"], [StringValue "people"]])
    
    it "returns a Dataframe from: SELECT id, name surname FROM employees" $ do
      Lib2.executeStatement (SelectStatement ["employees.id", "employees.name", "employees.surname"] ["employees"] "") 
      `shouldBe` Right (DataFrame [Column "employees.id" IntegerType, Column "employees.name" StringType, Column "employees.surname" StringType] [[IntegerValue 1, StringValue "Vi", StringValue "Po"], [IntegerValue 2, StringValue "Ed", StringValue "Dl"]])
    
    it "returns a DataFrame with MIN function" $ do
      Lib2.executeStatement (SelectStatement ["min(employees.id)"] ["employees"] "") `shouldBe` Right (DataFrame [Column "MIN(employees.id)" IntegerType] [[IntegerValue 1]])
   
    it "returns a DataFrame with SUM function" $ do
      Lib2.executeStatement (SelectStatement ["sum(employees.id)"] ["employees"] "") `shouldBe` Right (DataFrame [Column "SUM(employees.id)" IntegerType] [[IntegerValue 3]])
    
    it "returns LEFT when column is not found" $ do
      Lib2.executeStatement (SelectStatement ["sum(employees.iadqd)"] ["employees"] "") `shouldBe` Left "(Some of the) Column(s) not found."
    
    it "returns Left when table is not found" $ do
      Lib2.executeStatement (SelectStatement ["sum(emyees.id)"] ["emyees"] "") `shouldBe` Left "(Some of the) Column(s) not found."

    it "returns a Dataframe from: SELECT id, name FROM employees WHERE id > 1" $ do
      Lib2.executeStatement (SelectStatement ["employees.id", "employees.name"] ["employees"] "employees.id > 1") `shouldBe` 
       Right (DataFrame [Column "employees.id" IntegerType, Column "employees.name" StringType] [[IntegerValue 2, StringValue "Ed"]])
    
    it "returns a Dataframe from: SELECT id, name FROM employees WHERE id >= 1 AND id != 2" $ do
      Lib2.executeStatement (SelectStatement ["employees.id", "employees.name"] ["employees"] "employees.id >= 1 AND employees.id != 2") `shouldBe` 
       Right (DataFrame [Column "employees.id" IntegerType, Column "employees.name" StringType] [[IntegerValue 1, StringValue "Vi"]])
    
    it "returns a Dataframe from: SELECT id, name FROM employees WHERE 1 < 2" $ do
      Lib2.executeStatement (SelectStatement ["employees.id", "employees.name"] ["employees"] "1 < 2") `shouldBe` 
       Right (DataFrame [Column "employees.id" IntegerType, Column "employees.name" StringType] [[IntegerValue 1, StringValue "Vi"], [IntegerValue 2, StringValue "Ed"]])
    
    it "returns a Dataframe from: SELECT id, name FROM employees WHERE id > 1 AND employees.id < 5 AND employees.id != 3 AND 5 != 10" $ do
      Lib2.executeStatement (SelectStatement ["employees.id", "employees.name"] ["employees"] "employees.id > 1 AND employees.id < 5 AND employees.id != 3 AND 5 != 10") `shouldBe` 
       Right (DataFrame [Column "employees.id" IntegerType, Column "employees.name" StringType] [ [IntegerValue 2, StringValue "Ed"]])

    it "returns a Dataframe from: SELECT min(id) from employees WHERE id != 1 AND 1 < 2" $ do
      Lib2.executeStatement (SelectStatement ["min(employees.id)"] ["employees"] "employees.id != 1 AND 1 < 2") `shouldBe` 
       Right (DataFrame [Column "MIN(employees.id)" IntegerType] [[IntegerValue 2]])

    it "returns a DataFrame from: Select employees.name, people.name from employees, people where employees.id = people.id" $ do 
      Lib2.executeStatement (SelectStatement["employees.name","people.name"] ["employees", "people"] "employees.id = people.id") `shouldBe`
       Right (DataFrame [Column "employees.name" StringType,Column "people.name" StringType] [[StringValue "Vi",StringValue "domas"],
                                                                                              [StringValue "Vi",StringValue "emilja"],
                                                                                              [StringValue "Vi",StringValue "greta"],
                                                                                              [StringValue "Ed",StringValue "petras"]])
    it "returns a DataFrame from: Select employees.name, people.name from employees, people where employees.id = people.id" $ do 
      Lib2.executeStatement (SelectStatement["employees.name","people.name"] ["employees", "people"] "employees.id = people.id and people.id != 2") `shouldBe`
       Right (DataFrame [Column "employees.name" StringType,Column "people.name" StringType] [[StringValue "Vi",StringValue "domas"],
                                                                                              [StringValue "Vi",StringValue "emilja"],
                                                                                              [StringValue "Vi",StringValue "greta"]])
    it "returns left error when column is not existent" $ do
      Lib2.executeStatement (SelectStatement["employees.name","people.NonExistetColumn"] ["employees", "people"] "employees.id = people.id ") `shouldBe`
        Left "(Some of the) Column(s) not found."

  describe "Lib3" $ do
    it "returns a DataFrame with inserted line" $ do
      result <- runExecuteIO $ Lib3.executeSql "INSERT INTO employees (id,name,surname) VALUES (123, 'Namerino', 'Surnamerino');"
      result `shouldBe` Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType][[IntegerValue 1, StringValue "Vi", StringValue "Po"],
                                                                                                                          [IntegerValue 2, StringValue "Ed", StringValue "Dl"],
                                                                                                                          [IntegerValue 123, StringValue "Namerino", StringValue "Surnamerino"]])
    it "returns a DataFrame with inserted line" $ do
      result <- runExecuteIO $ Lib3.executeSql "INSERT INTO people (id,name) VALUES (3, 'remigijus');"
      result `shouldBe` Right (DataFrame[Column "id" IntegerType, Column "name" StringType][[IntegerValue 1, StringValue "domas"],
                                                                                            [IntegerValue 2, StringValue "petras"],
                                                                                            [IntegerValue 1, StringValue "emilja"],
                                                                                            [IntegerValue 1, StringValue "greta"],
                                                                                            [IntegerValue 3, StringValue "remigijus"]])
    it "returns a DataFrame with inserted line" $ do
      result <- runExecuteIO $ Lib3.executeSql "INSERT INTO noTable (id,name) VALUES (3, 'remigijus');"
      result `shouldBe` Left "Table noTable doesn't exist."

    it "returns a DataFrame from: Select NOW(), employees.id from employees;" $ do
      result <- runExecuteIO $ Lib3.executeSql "Select NOW(), employees.id from employees;"
      result `shouldBe` Right (DataFrame [Column "employees.id" IntegerType, Column "NOW()" StringType] [[IntegerValue 1, StringValue "2023-12-24 19:00:00"], 
                                                                                                         [IntegerValue 2, StringValue "2023-12-24 19:00:00"]])
    it "returns a DataFrame from: Select NOW(), people.name from people;" $ do
      result <- runExecuteIO $ Lib3.executeSql "Select NOW(), people.name from people;"
      result `shouldBe` Right (DataFrame [Column "people.name" StringType, Column "NOW()" StringType] [[StringValue "domas", StringValue "2023-12-24 19:00:00"],
                                                                                                       [StringValue "petras", StringValue "2023-12-24 19:00:00"],
                                                                                                       [StringValue "emilja", StringValue "2023-12-24 19:00:00"], 
                                                                                                       [StringValue "greta", StringValue "2023-12-24 19:00:00"]])                                                                                                 
    it "returns left if table does not exist" $ do
      result <- runExecuteIO $ Lib3.executeSql "Select NOW(), employees.id from yees;" 
      result `shouldSatisfy` isLeft

    it "returns a DataFrame with deleted rows" $ do 
      result <- runExecuteIO $ Lib3.executeSql "Delete from people;"
      result `shouldBe` Right (DataFrame[Column "id" IntegerType, Column "name" StringType][])

    it "returns a DataFrame with deleted rows" $ do 
      result <- runExecuteIO $ Lib3.executeSql "Delete from people where id = 1;"
      result `shouldBe` Right (DataFrame[Column "id" IntegerType, Column "name" StringType][[IntegerValue 2, StringValue "petras"]])

    it "returns a DataFrame with deleted rows" $ do 
      result <- runExecuteIO $ Lib3.executeSql "Delete from people where id != 1;"
      result `shouldBe` Right (DataFrame[Column "id" IntegerType, Column "name" StringType][[IntegerValue 1, StringValue "domas"],
                                                                                            [IntegerValue 1, StringValue "emilja"],
                                                                                            [IntegerValue 1, StringValue "greta"]])
    it "returns a Left is column is not present" $ do 
      result <- runExecuteIO $ Lib3.executeSql "Delete from people where notHereColumn != 1;"
      result `shouldBe` Left "deletion failed: Column does not exist in this row"
    it "Serializes a DataFrame to json" $ do
      Lib3.dataFrameToJson  (DataFrame[Column "id" IntegerType, Column "name" StringType][[IntegerValue 1, StringValue "domas"],
                                                                                            [IntegerValue 1, StringValue "emilja"],
                                                                                            [IntegerValue 1, StringValue "greta"]]) 
      `shouldBe` "{ \"columns\": [{ \"name\": \"id\", \"type\": \"integer\" }, { \"name\": \"name\", \"type\": \"string\" }], \"rows\": [[{\"IntegerValue\":1}, {\"StringValue\":\"domas\"}], [{\"IntegerValue\":1}, {\"StringValue\":\"emilja\"}], [{\"IntegerValue\":1}, {\"StringValue\":\"greta\"}]] }" 

    it "returns a DataFrame with updated values" $ do
      result <- runExecuteIO $ Lib3.executeSql "UPDATE employees SET name = 'testName', surname = 'testSurname' WHERE id = 1;"
      result `shouldBe` Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType][[IntegerValue 1, StringValue "testName", StringValue "testSurname"],
                                                                                                                          [IntegerValue 2, StringValue "Ed", StringValue "Dl"]])
    it "returns a DataFrame with all rows updated" $ do
      result <- runExecuteIO $ Lib3.executeSql "UPDATE employees SET name = 'testName', surname = 'testSurname';"
      result `shouldBe` Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType][[IntegerValue 1, StringValue "testName", StringValue "testSurname"],
                                                                                                                          [IntegerValue 2, StringValue "testName", StringValue "testSurname"]])
    it "returns an error message" $ do
      result <- runExecuteIO $ Lib3.executeSql "UPDATE employees SET name = 'testName' WHERE age = 20;"
      result `shouldSatisfy` isLeft
    
    it "Serializes a DataFrame to json" $ do
      Lib3.dataFrameToJson (DataFrame[Column "id" IntegerType, Column "name" StringType, Column "surname" StringType][ [IntegerValue 1, StringValue "Vi", StringValue "Po"],
                                                                                                                       [IntegerValue 2, StringValue "Ed", StringValue "Dl"]]) 
      `shouldBe` "{ \"columns\": [{ \"name\": \"id\", \"type\": \"integer\" }, { \"name\": \"name\", \"type\": \"string\" }, { \"name\": \"surname\", \"type\": \"string\" }], \"rows\": [[{\"IntegerValue\":1}, {\"StringValue\":\"Vi\"}, {\"StringValue\":\"Po\"}], [{\"IntegerValue\":2}, {\"StringValue\":\"Ed\"}, {\"StringValue\":\"Dl\"}]] }"

    it "Serializes a DataFrame to json that has no rows" $ do
      Lib3.dataFrameToJson (DataFrame[Column "id" IntegerType, Column "name" StringType, Column "surname" StringType][ ]) 
      `shouldBe` "{ \"columns\": [{ \"name\": \"id\", \"type\": \"integer\" }, { \"name\": \"name\", \"type\": \"string\" }, { \"name\": \"surname\", \"type\": \"string\" }], \"rows\": [] }"

    it "Serializes a DataFrame to json that has no rows and no columns" $ do
      Lib3.dataFrameToJson (DataFrame[][]) 
      `shouldBe` "{ \"columns\": [], \"rows\": [] }"

    it "Parses Json into a DataFrame" $ do
      Lib3.jsonParser ("{ \"columns\": [], \"rows\": [] }") `shouldBe` Just(DataFrame[][])  

    it "Parses Json into a DataFrame" $ do
      Lib3.jsonParser ("{ \"columns\": [{ \"name\": \"id\", \"type\": \"integer\" }, { \"name\": \"name\", \"type\": \"string\" }, { \"name\": \"surname\", \"type\": \"string\" }], \"rows\": [] }")
      `shouldBe` Just(DataFrame[Column "id" IntegerType, Column "name" StringType, Column "surname" StringType][]) 
    it "Parses Json into a DataFrame" $ do
      Lib3.jsonParser  ("{ \"columns\": [{ \"name\": \"id\", \"type\": \"integer\" }, { \"name\": \"name\", \"type\": \"string\" }, { \"name\": \"surname\", \"type\": \"string\" }], \"rows\": [[{\"IntegerValue\":1}, {\"StringValue\":\"Vi\"}, {\"StringValue\":\"Po\"}], [{\"IntegerValue\":2}, {\"StringValue\":\"Ed\"}, {\"StringValue\":\"Dl\"}]] }")
      `shouldBe` Just (DataFrame[Column "id" IntegerType, Column "name" StringType, Column "surname" StringType][ [IntegerValue 1, StringValue "Vi", StringValue "Po"],
                                                                                                                       [IntegerValue 2, StringValue "Ed", StringValue "Dl"]])                                                                                       
