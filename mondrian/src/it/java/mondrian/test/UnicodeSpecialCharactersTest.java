package mondrian.test;

import junit.framework.TestCase;
import mondrian.olap.Connection;
import mondrian.spi.Dialect;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.sql.Statement;

public class UnicodeSpecialCharactersTest extends TestCase {

  // Test to check fix for MONDRIAN-990, ensuring that when Unicode characters
  // (tested with characters that don't belong to the QL_Latin1_General_CP1_CI_AS collation) are used in identifiers of
  // a MDX query, then Mondrian is able to process the query and return a result with the characters present
  public void test_specialCharacters() throws SQLException {
    TestContext context = TestContext.instance().withSchema(
      "<?xml version=\"1.0\"?>\n"
        + "<Schema name=\"FoodMart\">\n"
        + "  <Cube name=\"Cube1\">"
        + "    <Table name=\"unicode_test\" schema=\"dbo\"/>"
        + "    <Dimension highCardinality=\"false\" name=\"Dimension1\" type=\"StandardDimension\" visible=\"true\">"
        + "      <Hierarchy hasAll=\"true\" name=\"Hierarchy1\" visible=\"true\">"
        + "        <Level column=\"name\" hideMemberIf=\"Never\" levelType=\"Regular\" name=\"Level1\"/>"
        + "      </Hierarchy>"
        + "    </Dimension>"
        + "    <Measure aggregator=\"sum\" column=\"value\" name=\"sum\" visible=\"true\"/>"
        + "  </Cube>"
        + "</Schema>\n" );

    Dialect dialect = context.getDialect();
    // We only want to test the Unicode characters processing in SQL Server and Oracle
    if ( dialect.getDatabaseProduct() != Dialect.DatabaseProduct.MSSQL
      && dialect.getDatabaseProduct() != Dialect.DatabaseProduct.ORACLE ) {
      return;
    }

    Connection olapConnection = context.getConnection();
    DataSource ds = olapConnection.getDataSource();
    java.sql.Connection connection = ds.getConnection();
    Statement createStatement = connection.createStatement();
    createStatement.execute( "CREATE TABLE foodmart.dbo.unicode_test\n"
      + "(\n"
      + "    name  varchar(100) COLLATE Korean_Wansung_CI_AS,\n"
      + "    value int,\n"
      + ");\n" );
    createStatement.close();

    Statement insertStatement = connection.createStatement();
    insertStatement.execute( "INSERT INTO foodmart.dbo.unicode_test\n"
      + "VALUES ( N'박', 1),\n"
      + "       ( N'유', 2),\n"
      + "       (N'김', 3);" );
    insertStatement.close();

    String queryFromAnalyzer = ""
      + "select "
      + "  [Measures].[sum] on columns "
      + "from Cube1 "
      + "where ([Dimension1.Hierarchy1].[Level1].[김])";

    String expectedResult = "Axis #0:\n"
      + "{[Dimension1.Hierarchy1].[김]}\n"
      + "Axis #1:\n"
      + "{[Measures].[sum]}\n"
      + "Row #0: 3\n";

    context.assertQueryReturns( queryFromAnalyzer, expectedResult );

    Statement deleteStatement = connection.createStatement();
    deleteStatement.execute( "DROP TABLE foodmart.dbo.unicode_test" );
    deleteStatement.close();
  }
}
