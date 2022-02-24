package mondrian.test;

import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.regex.Pattern;

import org.olap4j.CellSet;
import org.olap4j.OlapConnection;

import junit.framework.ComparisonFailure;
import mondrian.olap.Axis;
import mondrian.olap.CacheControl;
import mondrian.olap.Cell;
import mondrian.olap.Connection;
import mondrian.olap.Member;
import mondrian.olap.Result;
import mondrian.olap.Util;
import mondrian.spi.Dialect;

/**
 * <code>TestContext</code> is a class which contains the information
 * necessary to run mondrian tests.
 */
public interface TestContext {

    /**
       * Returns the connect string by which the unit tests can talk to the database.
       *
       * <p>In the base class, the result is the same as the static method
       * {@link #getDefaultConnectString}. If a derived class overrides {@link #getConnectionProperties()}, the result of
       * this method will change also.
       */
    String getConnectString();

    void flushSchemaCache();

    /**
       * Returns the connection to run queries.
       *
       * <p>When invoked on the default TestContext instance, returns a connection
       * to the database.
       */
    Connection getConnection();

    Util.PropertyList getConnectionProperties();

    /**
       * Returns the definition of the schema.
       *
       * @return XML definition of the given schema
       */
    String getRawSchema();

    /**
       * Executes a query.
       *
       * @param queryString Query string
       */
    Result executeQuery(String queryString);

    ResultSet executeStatement(String queryString) throws SQLException;

    /**
       * Executes a query using olap4j.
       */
    CellSet executeOlap4jQuery(String queryString) throws SQLException;

    CellSet executeOlap4jXmlaQuery(String queryString) throws SQLException;

    /**
       * Executes a query, and asserts that it throws an exception which contains the given pattern.
       *
       * @param queryString Query string
       * @param pattern     Pattern which exception must match
       */
    void assertQueryThrows(String queryString, String pattern);

    /**
       * Executes an expression, and asserts that it gives an error which contains a particular pattern. The error might
       * occur during parsing, or might be contained within the cell value.
       */
    void assertExprThrows(String expression, String pattern);

    /**
       * Returns the name of the default cube.
       *
       * <p>Tests which evaluate scalar expressions, such as
       * {@link #assertExprReturns(String, String)}, generate queries against this cube.
       *
       * @return the name of the default cube
       */
    String getDefaultCubeName();

    /**
       * Executes the expression in the context of the cube indicated by
       * <code>cubeName</code>, and returns the result as a Cell.
       *
       * @param expression The expression to evaluate
       * @return Cell which is the result of the expression
       */
    Cell executeExprRaw(String expression);

    /**
       * Executes an expression and asserts that it returns a given result.
       */
    void assertExprReturns(String expression, String expected);

    /**
       * Asserts that an expression, with a given set of parameter bindings, returns a given result.
       *
       * @param expr        Scalar MDX expression
       * @param expected    Expected result
       * @param paramValues Array of parameter names and values
       */
    void assertParameterizedExprReturns(String expr, String expected, Object... paramValues);

    /**
       * Executes a query with a given expression on an axis, and asserts that it returns the expected string.
       */
    void assertAxisReturns(String expression, String expected);

    /**
       * Massages the actual result of executing a query to handle differences in unique names betweeen old and new
       * behavior.
       *
       * <p>Even though the new naming is not enabled by default, reference logs
       * should be in terms of the new naming.
       *
       * @param actual Actual result
       * @return Expected result massaged for backwards compatibility
       * @see mondrian.olap.MondrianProperties#SsasCompatibleNaming
       */
    String upgradeActual(String actual);

    /**
       * Massages an MDX query to handle differences in unique names betweeen old and new behavior.
       *
       * <p>The main difference addressed is with level naming. The problem
       * arises when dimension, hierarchy and level have the same name:<ul>
       *
       * <li>In old behavior, the [Gender].[Gender] represents the Gender level,
       * and [Gender].[Gender].[Gender] is invalid.
       *
       * <li>In new behavior, [Gender].[Gender] represents the Gender hierarchy,
       * and [Gender].[Gender].[Gender].members represents the Gender level.
       * </ul></p>
       *
       * <p>So, {@code upgradeQuery("[Gender]")} returns
       * "[Gender].[Gender]" for old behavior, "[Gender].[Gender].[Gender]" for new behavior.</p>
       *
       * @param queryString Original query
       * @return Massaged query for backwards compatibility
       * @see mondrian.olap.MondrianProperties#SsasCompatibleNaming
       */
    String upgradeQuery(String queryString);

    /**
       * Compiles a scalar expression in the context of the default cube.
       *
       * @param expression The expression to evaluate
       * @param scalar     Whether the expression is scalar
       * @return String form of the program
       */
    String compileExpression(String expression, boolean scalar);

    /**
       * Executes a set expression which is expected to return 0 or 1 members. It is an error if the expression returns
       * tuples (as opposed to members), or if it returns two or more members.
       *
       * @param expression Expression string
       * @return Null if axis returns the empty set, member if axis returns one member. Throws otherwise.
       */
    Member executeSingletonAxis(String expression);

    /**
       * Executes a query with a given expression on an axis, and returns the whole axis.
       */
    Axis executeAxis(String expression);

    /**
       * Executes a query with a given expression on an axis, and asserts that it throws an error which matches a particular
       * pattern. The expression is evaulated against the default cube.
       */
    void assertAxisThrows(String expression, String pattern);

    /**
       * Returns the output writer.
       */
    PrintWriter getWriter();

    /**
       * Executes a query and checks that the result is a given string.
       */
    void assertQueryReturns(String query, String desiredResult);

    /**
       * Executes a query and checks that the result is a given string, displaying a message if result does not match
       * desiredResult.
       */
    void assertQueryReturns(String message, String query, String desiredResult);

    /**
       * Executes a very simple query.
       *
       * <p>This forces the schema to be loaded and performs a basic sanity check.
       * If this is a negative schema test, causes schema validation errors to be thrown.
       */
    void assertSimpleQuery();

    /**
       * Checks that an actual string matches an expected pattern. If they do not, throws a {@link ComparisonFailure} and
       * prints the difference, including the actual string as an easily pasted Java string literal.
       */
    void assertMatchesVerbose(Pattern expected, String actual);

    void close();

    /**
       * Returns a {@link CacheControl}.
       */
    CacheControl getCacheControl();

    Dialect getDialect();

    /**
       * Checks that expected SQL equals actual SQL. Performs some normalization on the actual SQL to compensate for
       * differences between dialects.
       */
    void assertSqlEquals(String expectedSql, String actualSql, int expectedRows);

    /**
       * Asserts that an MDX set-valued expression depends upon a given list of dimensions.
       */
    void assertSetExprDependsOn(String expr, String dimList);

    /**
       * Asserts that an MDX member-valued depends upon a given list of dimensions.
       */
    void assertMemberExprDependsOn(String expr, String dimList);

    /**
       * Asserts that an MDX expression depends upon a given list of dimensions.
       */
    void assertExprDependsOn(String expr, String hierList);

    /**
       * Creates a connection with "Ignore=true" and returns the list of warnings in the schema.
       *
       * @return Warnings encountered while loading schema
       */
    List<Exception> getSchemaWarnings();

    OlapConnection getOlap4jConnection() throws SQLException;

    /**
       * Tests whether the database is valid. Allows tests that depend on optional databases to figure out whether to
       * proceed.
       *
       * @return whether a database is present and correct
       */
    boolean databaseIsValid();

}