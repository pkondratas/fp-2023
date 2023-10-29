import Data.Either
import Data.Maybe ()
import InMemoryTables qualified as D
import Lib1
import Lib2
import Test.Hspec
import DataFrame (DataFrame (DataFrame), Column (Column), ColumnType (StringType, IntegerType), Value (StringValue, IntegerValue, NullValue), Row)

-- data ParsedStatement
--   = ShowTable String
--   | ShowTables
--   | SelectStatement [String] String String
--   deriving (Show, Eq)


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
      Lib2.parseStatement "SELECT col1 col2 FROM table1;" `shouldBe` Right (SelectStatement ["col1", "col2"] "table1" "")
    it "parse SELECT col1 col2 FROM table1 WHERE a > b;" $ do
      Lib2.parseStatement "SELECT col1 col2 FROM table1 WHERE a > b;" `shouldBe` Right (SelectStatement ["col1", "col2"] "table1" "a > b")
    it "parse SELECT col1 col2 FROM table1 WHERE a > b AND a > c AND b > c;" $ do
      Lib2.parseStatement "SELECT col1 col2 FROM table1 WHERE a > b AND a > c AND b > c;" `shouldBe` Right (SelectStatement ["col1", "col2"] "table1" "a > b AND a > c AND b > c")
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
      Lib2.executeStatement ShowTables `shouldBe` Right (DataFrame [Column "Tables" StringType] [[StringValue "employees"], [StringValue "invalid1"], [StringValue "invalid2"], [StringValue "long_strings"], [StringValue "flags"]])
    
    it "returns a Dataframe from: SELECT id name surname FROM employees" $ do
      Lib2.executeStatement (SelectStatement ["id", "name", "surname"] "employees" "") 
      `shouldBe` Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType] [[IntegerValue 1, StringValue "Vi", StringValue "Po"], [IntegerValue 2, StringValue "Ed", StringValue "Dl"]])
    
    it "returns a DataFrame with MIN function" $ do
      Lib2.executeStatement (SelectStatement ["min(id)"] "employees" "") `shouldBe` Right (DataFrame [Column "MIN(id)" IntegerType] [[IntegerValue 1]])
   
    it "returns a DataFrame with SUM function" $ do
      Lib2.executeStatement (SelectStatement ["sum(id)"] "employees" "") `shouldBe` Right (DataFrame [Column "SUM(id)" IntegerType] [[IntegerValue 3]])
    
    it "returns LEFT when column is not found" $ do
      Lib2.executeStatement (SelectStatement ["sum(iadqd)"] "employees" "") `shouldBe` Left "(Some of the) Column(s) not found."
    
    it "returns Left when table is not found" $ do
      Lib2.executeStatement (SelectStatement ["sum(id)"] "emyees" "") `shouldBe` Left "Table not found in the database"

    it "returns a Dataframe from: SELECT id name FROM employees WHERE id > 1" $ do
      Lib2.executeStatement (SelectStatement ["id", "name"] "employees" "id > 1") `shouldBe` 
       Right (DataFrame [Column "id" IntegerType, Column "name" StringType] [[IntegerValue 2, StringValue "Ed"]])
    
    it "returns a Dataframe from: SELECT id name FROM employees WHERE id >= 1 AND id != 2" $ do
      Lib2.executeStatement (SelectStatement ["id", "name"] "employees" "id >= 1 AND id != 2") `shouldBe` 
       Right (DataFrame [Column "id" IntegerType, Column "name" StringType] [[IntegerValue 1, StringValue "Vi"]])
    
    it "returns a Dataframe from: SELECT id name FROM employees WHERE 1 < 2" $ do
      Lib2.executeStatement (SelectStatement ["id", "name"] "employees" "1 < 2") `shouldBe` 
       Right (DataFrame [Column "id" IntegerType, Column "name" StringType] [[IntegerValue 1, StringValue "Vi"], [IntegerValue 2, StringValue "Ed"]])
    
    it "returns a Dataframe from: SELECT id name FROM employees WHERE id > 1 AND id < 5 AND id != 3 AND 5 != 10" $ do
      Lib2.executeStatement (SelectStatement ["id", "name"] "employees" "id > 1 AND id < 5 AND id != 3 AND 5 != 10") `shouldBe` 
       Right (DataFrame [Column "id" IntegerType, Column "name" StringType] [ [IntegerValue 2, StringValue "Ed"]])

    it "returns a Dataframe from: SELECT min(id) from employees WHERE id != 1 AND 1 < 2" $ do
      Lib2.executeStatement (SelectStatement ["min(id)"] "employees" "id != 1 AND 1 < 2") `shouldBe` 
       Right (DataFrame [Column "MIN(id)" IntegerType] [[IntegerValue 2]])
