/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2003-2003 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Feb 13, 2003
*/
package mondrian.test;

import mondrian.olap.Connection;
import mondrian.olap.Query;
import mondrian.olap.Parameter;

/**
 * A <code>ParameterTest</code> is a test suite for functionality relating to
 * parameters.
 *
 * @author jhyde
 * @since Feb 13, 2003
 * @version $Id$
 **/
public class ParameterTest extends FoodMartTestCase {
	public ParameterTest(String name) {
		super(name);
	}

	public void testParameterOnAxis() {
		runQueryCheckResult(
				"select {[Measures].[Unit Sales]} on rows," + nl +
				" {Parameter(\"GenderParam\",[Gender],[Gender].[M],\"Which gender?\")} on columns" + nl +
				"from Sales",

				"Axis #0:" + nl +
				"{}" + nl +
				"Axis #1:" + nl +
				"{[Gender].[All Gender].[M]}" + nl +
				"Axis #2:" + nl +
				"{[Measures].[Unit Sales]}" + nl +
				"Row #0: 135,215" + nl);
	}
	public void testNumericParameter() {
		String s = executeExpr("Parameter(\"N\",NUMERIC,2+3,\"A numeric parameter\")");
		assertEquals("5",s);
	}
	public void testStringParameter() {
		String s = executeExpr("Parameter(\"S\",STRING,\"x\" || \"y\",\"A string parameter\")");
		assertEquals("xy", s);
	}
	public void testNumericParameterStringValueFails() {
		assertExprThrows("Parameter(\"S\",NUMERIC,\"x\" || \"y\",\"A string parameter\")",
				"Default value of parameter 'S' is inconsistent with its type, NUMERIC");
	}
	public void testParameterNonHierarchyFails() {
		assertExprThrows("Parameter(\"Foo\",[Time].[Year],[Time].[1997],\"Foo\")",
				"Invalid type for parameter 'Foo'; expecting NUMERIC, STRING or a hierarchy");
	}
	public void testParameterWithExpressionForHierarchyFails() {
		assertExprThrows("Parameter(\"Foo\",[Gender].DefaultMember.Hierarchy,[Gender].[M],\"Foo\")",
				"Invalid hierarchy for parameter 'Foo'");
	}
	public void _testDerivedParameterFails() {
		assertExprThrows("Parameter(\"X\",NUMERIC,Parameter(\"Y\",NUMERIC,1)+2)",
				"Parameter may not be derived from another parameter");
	}
	public void testParameterInSlicer() {
		runQueryCheckResult(
				"select {[Measures].[Unit Sales]} on rows," + nl +
				" {[Marital Status].children} on columns" + nl +
				"from Sales where Parameter(\"GenderParam\",[Gender],[Gender].[M],\"Which gender?\")",
				"Axis #0:" + nl +
				"{[Gender].[All Gender].[M]}" + nl +
				"Axis #1:" + nl +
				"{[Marital Status].[All Marital Status].[M]}" + nl +
				"{[Marital Status].[All Marital Status].[S]}" + nl +
				"Axis #2:" + nl +
				"{[Measures].[Unit Sales]}" + nl +
				"Row #0: 66,460" + nl +
				"Row #0: 68,755" + nl);
	}
	/**
	 * Parameter in slicer and expression on columns axis are both of [Gender]
	 * hierarchy, which is illegal.
	 */
	public void _testParameterDuplicateDimensionFails() {
		assertThrows(
				"select {[Measures].[Unit Sales]} on rows," + nl +
				" {[Gender].[F]} on columns" + nl +
				"from Sales where Parameter(\"GenderParam\",[Gender],[Gender].[M],\"Which gender?\")",
				"Invalid hierarchy for parameter 'GenderParam'");
	}

	/** Mondrian can not handle forward references */
	public void dontTestParamRef() {
		String s = executeExpr(
				"Parameter(\"X\",STRING,\"x\",\"A string\") || " +
				"ParamRef(\"Y\") || " +
				"\".\" ||" +
				"ParamRef(\"X\") || " +
				"Parameter(\"Y\",STRING,\"y\" || \"Y\",\"Other string\")");
		assertEquals("xyY.xyY",s);
	}
	public void testParamRefWithoutParamFails() {
		assertExprThrows("ParamRef(\"Y\")", "Parameter 'Y' is referenced but never defined");
	}
	public void testParamDefinedTwiceFails() {
		assertThrows(
				"select {[Measures].[Unit Sales]} on rows," + nl +
				" {Parameter(\"P\",[Gender],[Gender].[M],\"Which gender?\")," + nl +
				"  Parameter(\"P\",[Gender],[Gender].[F],\"Which gender?\")} on columns" + nl +
				"from Sales",
				"Parameter 'P' is defined more than once");
	}
	public void testParameterMetadata() {
		Connection connection = getConnection();
		Query query = connection.parseQuery(
				"with member [Measures].[A string] as " + nl +
				"   Parameter(\"S\",STRING,\"x\" || \"y\",\"A string parameter\")" + nl +
				" member [Measures].[A number] as " + nl +
				"   Parameter(\"N\",NUMERIC,2+3,\"A numeric parameter\")" + nl +
				"select {[Measures].[Unit Sales]} on rows," + nl +
				" {Parameter(\"P\",[Gender],[Gender].[F],\"Which gender?\")," + nl +
				"  Parameter(\"Q\",[Gender],[Gender].DefaultMember,\"Another gender?\")} on columns" + nl +
				"from Sales");
		Parameter[] parameters = query.getParameters();
		assertEquals("S", parameters[0].getName());
		assertEquals("N", parameters[1].getName());
		assertEquals("P", parameters[2].getName());
		assertEquals("Q", parameters[3].getName());
		parameters[2].setValue("[Gender].[M]", query);
		assertEquals("with member [Measures].[A string] as 'Parameter(\"S\", STRING, (\"x\" || \"y\"), \"A string parameter\")'" + nl +
				"  member [Measures].[A number] as 'Parameter(\"N\", NUMERIC, (2.0 + 3.0), \"A numeric parameter\")'" + nl +
				"select {Parameter(\"P\", [Gender], [Gender].[All Gender].[M], \"Which gender?\"), Parameter(\"Q\", [Gender], [Gender].DefaultMember, \"Another gender?\")} ON columns," + nl +
				"  {[Measures].[Unit Sales]} ON rows" + nl +
				"from [Sales]" + nl,
				query.toString());
	}
}

// End ParameterTest.java