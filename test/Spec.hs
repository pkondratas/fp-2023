import Data.Either
import Data.Maybe ()
import InMemoryTables qualified as D
import Lib1
import Lib2
import Test.Hspec

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
  