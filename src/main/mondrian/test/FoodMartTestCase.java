/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2003 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 29 March, 2002
*/
package mondrian.test;

import junit.framework.TestCase;
import mondrian.olap.*;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;

/**
 * <code>FoodMartTestCase</code> is a unit test which runs against the FoodMart
 * database.
 *
 * @author jhyde
 * @since 29 March, 2002
 * @version $Id$
 **/
public class FoodMartTestCase extends TestCase {
	protected static final String nl = System.getProperty("line.separator");

	public FoodMartTestCase(String name) {
		super(name);
	}

	public Result runQuery(String queryString) {
		Connection connection = getConnection();
		Query query = connection.parseQuery(queryString);
		return connection.execute(query);
	}

	protected Connection getConnection() {
		return TestContext.instance().getFoodMartConnection(false);
	}

	/**
	 * Runs a query, and asserts that the result has a given number of columns
	 * and rows.
	 */
	protected void assertSize(String queryString, int columnCount, int rowCount) {
		Result result = runQuery(queryString);
		Axis[] axes = result.getAxes();
		assertTrue(axes.length == 2);
		assertTrue(axes[0].positions.length == columnCount);
		assertTrue(axes[1].positions.length == rowCount);
	}

	/**
	 * Runs a query, and asserts that it throws an exception which contains
	 * the given pattern.
	 */
	public void assertThrows(String queryString, String pattern) {
		Throwable throwable = TestContext.instance().executeFoodMartCatch(
				queryString);
		checkThrowable(throwable, pattern);
	}

	private void checkThrowable(Throwable throwable, String pattern) {
		if (throwable == null) {
			fail("query did not yield an exception");
		}
		String stackTrace = getStackTrace(throwable);
		if (stackTrace.indexOf(pattern) < 0) {
			fail("query's error does not match pattern '" + pattern +
					"'; error is [" + stackTrace + "]");
		}
	}

	/** Executes a query in a given connection. **/
	public Result execute(Connection connection, String queryString) {
		Query query = connection.parseQuery(queryString);
		return connection.execute(query);
	}

	/**
	 * Runs a query with a given expression on an axis, and returns the whole
	 * axis.
	 */
	public Axis executeAxis2(String cube, String expression) {
		Result result = execute(
				TestContext.instance().getFoodMartConnection(false),
				"select {" + expression + "} on columns from " + cube);
		return result.getAxes()[0];
	}

	/**
	 * Runs a query with a given expression on an axis, on a given connection,
	 * and returns the whole axis.
	 */
	Axis executeAxis2(Connection connection, String expression) {
		Result result = execute(connection, "select {" + expression + "} on columns from Sales");
		return result.getAxes()[0];
	}
	/**
	 * Runs a query with a given expression on an axis, and returns the single
	 * member.
	 */
	public Member executeAxis(String expression) {
		Result result = TestContext.instance().executeFoodMart(
				"select {" + expression + "} on columns from Sales");
		Axis axis = result.getAxes()[0];
		switch (axis.positions.length) {
		case 0:
			// The mdx "{...}" operator eliminates null members (that is,
			// members for which member.isNull() is true). So if "expression"
			// yielded just the null member, the array will be empty.
			return null;
		case 1:
			// Java nulls should never happen during expression evaluation.
			Position position = axis.positions[0];
			Util.assertTrue(position.members.length == 1);
			Member member = position.members[0];
			Util.assertTrue(member != null);
			return member;
		default:
			throw Util.newInternal(
					"expression " + expression + " yielded " +
					axis.positions.length + " positions");
		}
	}

	/**
	 * Runs a query and checks that the result is a given string.
	 */
	public void runQueryCheckResult(QueryAndResult queryAndResult) {
		runQueryCheckResult(queryAndResult.query, queryAndResult.result);
	}

	/**
	 * Runs a query and checks that the result is a given string.
	 */
	public void runQueryCheckResult(String query, String desiredResult) {
		Result result = runQuery(query);
		String resultString = toString(result);
		if (desiredResult != null) {
			assertEquals(desiredResult, resultString);
		}
	}

	/**
	 * Runs a query.
	 */
	public Result execute(String queryString) {
		return TestContext.instance().executeFoodMart(queryString);
	}

	/**
	 * Runs a query with a given expression on an axis, and asserts that it
	 * throws an error which matches a particular pattern.
	 */
	public void assertAxisThrows(String expression, String pattern) {
		assertAxisThrows(TestContext.instance().getFoodMartConnection(false),
				expression, pattern);
	}
	/**
	 * Runs a query with a given expression on an axis, and asserts that it
	 * throws an error which matches a particular pattern.
	 */
	public void assertAxisThrows(Connection connection, String expression, String pattern) {
		Throwable throwable = null;
		try {
			Util.discard(execute(connection,
					"select {" + expression + "} on columns from Sales"));
		} catch (Throwable e) {
			throwable = e;
		}
		checkThrowable(throwable, pattern);
	}

	/**
	 * Runs a query on the "Sales" cube with a given expression on an axis, and
	 * asserts that it returns the expected string.
	 */
	public void assertAxisReturns(String expression, String expected) {
		assertAxisReturns("Sales", expression, expected);
	}
	/**
	 * Runs a query with a given expression on an axis, and asserts that it
	 * returns the expected string.
	 */
	public void assertAxisReturns(String cube, String expression, String expected) {
		Axis axis = executeAxis2(cube, expression);
		assertEquals(expected, toString(axis.positions));
	}
	/**
	 * Runs a query with a given expression on an axis, and asserts that it
	 * returns the expected string.
	 */
	public void assertAxisReturns(Connection connection, String expression, String expected) {
		Axis axis = executeAxis2(connection, expression);
		assertEquals(expected, toString(axis.positions));
	}
	/**
	 * Converts a {@link Throwable} to a stack trace.
	 */
	private static String getStackTrace(Throwable e) {
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		e.printStackTrace(pw);
		pw.flush();
		return sw.toString();
	}

	/** Executes an expression against the FoodMart database to form a single
	 * cell result set, then returns that cell's formatted value. **/
	public String executeExpr(String expression) {
		Result result = TestContext.instance().executeFoodMart(
				"with member [Measures].[Foo] as '" +
				expression +
				"' select {[Measures].[Foo]} on columns from Sales");
		Cell cell = result.getCell(new int[]{0});
		return cell.getFormattedValue();
	}

	/** Executes an expression which returns a boolean result. **/
	public String executeBooleanExpr(String expression) {
		return executeExpr("Iif(" + expression + ",\"true\",\"false\")");
	}

	/**
	 * Runs an expression, and asserts that it gives an error which contains
	 * a particular pattern. The error might occur during parsing, or might
	 * be contained within the cell value.
	 */
	public void assertExprThrows(String expression, String pattern) {
		Throwable throwable = null;
		try {
			Result result = TestContext.instance().executeFoodMart(
					"with member [Measures].[Foo] as '" +
					expression +
					"' select {[Measures].[Foo]} on columns from Sales");
			Cell cell = result.getCell(new int[]{0});
			if (cell.isError()) {
				throwable = (Throwable) cell.getValue();
			}
		} catch (Throwable e) {
			throwable = e;
		}
		checkThrowable(throwable, pattern);
	}

	/**
	 * Converts a set of positions into a string. Useful if you want to check
	 * that an axis has the results you expected.
	 */
	public String toString(Position[] positions) {
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < positions.length; i++) {
			Position position = positions[i];
			if (i > 0) {
				sb.append(nl);
			}
			if (position.members.length != 1) {
				sb.append("{");
			}
			for (int j = 0; j < position.members.length; j++) {
				Member member = position.members[j];
				if (j > 0) {
					sb.append(", ");
				}
				sb.append(member.getUniqueName());
			}
			if (position.members.length != 1) {
				sb.append("}");
			}
		}
		return sb.toString();
	}

	/** Formats {@link Result}. **/
	public String toString(Result result) {
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		result.print(pw);
		pw.flush();
		return sw.toString();
	}


    static class QueryAndResult {
        String query;
        String result;
        QueryAndResult(String query, String result) {
            this.query = query;
            this.result = result;
        }
    }
}

/**
 * Similar to {@link Runnable}, except classes which implement
 * <code>ChooseRunnable</code> choose what to do based upon an integer
 * parameter.
 */
interface ChooseRunnable {
	void run(int i);
}

/**
 * Runs a test case in several parallel threads, catching exceptions from
 * each one, and succeeding only if they all succeed.
 */
class TestCaseForker {
	TestCase testCase;
	int timeoutMs;
	Thread[] threads;
	ArrayList failures = new ArrayList();
	ChooseRunnable chooseRunnable;

	public TestCaseForker(
			TestCase testCase, int timeoutMs, int threadCount,
			ChooseRunnable chooseRunnable) {
		this.testCase = testCase;
		this.timeoutMs = timeoutMs;
		this.threads = new Thread[threadCount];
		this.chooseRunnable = chooseRunnable;
	}

	public void run() {
		ThreadGroup threadGroup = null;//new ThreadGroup("TestCaseForker thread group");
		for (int i = 0; i < threads.length; i++) {
			final int threadIndex = i;
			this.threads[i] = new Thread(threadGroup, "thread #" + threadIndex) {
				public void run() {
					try {
						chooseRunnable.run(threadIndex);
					} catch (Throwable e) {
						e.printStackTrace();
						failures.add(e);
					}
				}
			};
		}
		for (int i = 0; i < threads.length; i++) {
			threads[i].start();
		}
		for (int i = 0; i < threads.length; i++) {
			try {
				threads[i].join(timeoutMs);
			} catch (InterruptedException e) {
				failures.add(
						Util.newInternal(
								e, "Interrupted after " + timeoutMs + "ms"));
				break;
			}
		}
		if (failures.size() > 0) {
			for (int i = 0; i < failures.size(); i++) {
				Throwable throwable = (Throwable) failures.get(i);
				throwable.printStackTrace();
			}
			testCase.fail(failures.size() + " threads failed");
		}
	}
}

// End FoodMartTestCase.java
