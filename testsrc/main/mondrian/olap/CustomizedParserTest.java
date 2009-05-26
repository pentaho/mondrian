/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2004-2009 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
 */
package mondrian.olap;

import mondrian.test.FoodMartTestCase;
import mondrian.olap.fun.*;

import java.util.*;

/**
 * Tests a customized MDX Parser.
 *
 * @author Rushan Chen
 * @version $Id$
 */
public class CustomizedParserTest extends FoodMartTestCase {

    Parser p = new Parser();
    public CustomizedParserTest(String name) {
        super(name);
    }

    CustomizedFunctionTable getCustomizedFunctionTable(Set<String> funNameSet) {
        Set<FunDef> specialFunctions = new HashSet<FunDef>();
        specialFunctions.add(new ParenthesesFunDef(Category.Numeric));

        CustomizedFunctionTable cftab =
            new CustomizedFunctionTable(funNameSet, specialFunctions);
        cftab.init();
        return cftab;
    }

    private String wrapExpr(String expr) {
        return
            "with member [Measures].[Foo] as "
            + expr
            + "\n select from [Sales]";
    }

    private void checkErrorMsg(Throwable e, String expectedErrorMsg) {
        while (e.getCause() != null && !e.getCause().equals(e)) {
            e = e.getCause();
        }
        String actualMsg = e.getMessage();
        assertEquals(expectedErrorMsg, actualMsg);
    }

    private Query getParsedQueryForExpr(
        CustomizedFunctionTable cftab,
        String expr,
        boolean strictValidation)
    {
        String mdx = wrapExpr(expr);
        return ((ConnectionBase) getConnection()).parseQuery(
            mdx, cftab, strictValidation);
    }

    private Query getParsedQueryForExpr(
        CustomizedFunctionTable cftab,
        String expr)
    {
        return getParsedQueryForExpr(cftab, expr, false);
    }

    public void testAddition() {
        Set<String> functionNameSet = new HashSet<String>();
        functionNameSet.add("+");
        CustomizedFunctionTable cftab =
            getCustomizedFunctionTable(functionNameSet);

        try {
            Query q =
                getParsedQueryForExpr(
                    cftab,
                    "([Measures].[Store Cost] + [Measures].[Unit Sales])");
            q.resolve(q.createValidator(cftab));
        } catch (Throwable e) {
            fail(e.getMessage());
        }
    }

    public void testSubtraction() {
        Set<String> functionNameSet = new HashSet<String>();
        functionNameSet.add("-");
        CustomizedFunctionTable cftab =
            getCustomizedFunctionTable(functionNameSet);

        try {
            Query q =
                getParsedQueryForExpr(
                    cftab,
                    "([Measures].[Store Cost] - [Measures].[Unit Sales])");
            q.resolve(q.createValidator(cftab));
        } catch (Throwable e) {
            fail(e.getMessage());
        }
    }

    public void testSingleMultiplication() {
        Set<String> functionNameSet = new HashSet<String>();
        functionNameSet.add("*");
        CustomizedFunctionTable cftab =
            getCustomizedFunctionTable(functionNameSet);

        try {
            Query q =
                getParsedQueryForExpr(
                    cftab,
                    "[Measures].[Store Cost] * [Measures].[Unit Sales]");
            q.resolve(q.createValidator(cftab));
        } catch (Throwable e) {
            fail(e.getMessage());
        }
    }

    public void testMultipleMultiplication() {
        Set<String> functionNameSet = new HashSet<String>();
        functionNameSet.add("*");
        CustomizedFunctionTable cftab =
            getCustomizedFunctionTable(functionNameSet);

        try {
            Query q =
                getParsedQueryForExpr(
                    cftab,
                    "([Measures].[Store Cost] * [Measures].[Unit Sales] * [Measures].[Store Sales])");
            q.resolve(q.createValidator(cftab));
        } catch (Throwable e) {
            fail(e.getMessage());
        }
    }

    public void testLiterals() {
        Set<String> functionNameSet = new HashSet<String>();
        functionNameSet.add("+");
        CustomizedFunctionTable cftab =
            getCustomizedFunctionTable(functionNameSet);

        try {
            Query q =
                getParsedQueryForExpr(
                    cftab,
                    "([Measures].[Store Cost] + 10)");
            q.resolve(q.createValidator(cftab));
        } catch (Throwable e) {
            fail(e.getMessage());
        }
    }

    public void testMissingObjectFail() {
        Set<String> functionNameSet = new HashSet<String>();
        functionNameSet.add("+");
        CustomizedFunctionTable cftab =
            getCustomizedFunctionTable(functionNameSet);

        try {
            Query q =
                getParsedQueryForExpr(
                    cftab,
                    "'[Measures].[Store Cost] + [Measures].[Unit Salese]'");
            q.resolve(q.createValidator(cftab));
            // Shouldn't reach here
            fail("Expected error did not occur.");
        } catch (Throwable e) {
            checkErrorMsg(e,
            "Mondrian Error:MDX object '[Measures].[Unit Salese]' not found in cube 'Sales'");
        }
    }

    public void testMissingObjectFailWithStrict() {
        testMissingObject(true);
    }

    public void testMissingObjectSucceedWithoutStrict() {
        testMissingObject(false);
    }

    private void testMissingObject(boolean strictValidation) {
        Set<String> functionNameSet = new HashSet<String>();
        functionNameSet.add("+");
        CustomizedFunctionTable cftab =
            getCustomizedFunctionTable(functionNameSet);

        MondrianProperties properties = MondrianProperties.instance();
        boolean oldIgnoreInvalidMembers =
            properties.IgnoreInvalidMembers.get();
        boolean oldIgnoreInvalidMembersDuringQuery =
            properties.IgnoreInvalidMembersDuringQuery.get();

        try {
            properties.IgnoreInvalidMembers.set(true);
            properties.IgnoreInvalidMembersDuringQuery.set(true);
            Query q =
                getParsedQueryForExpr(
                    cftab,
                    "'[Measures].[Store Cost] + [Measures].[Unit Salese]'",
                    strictValidation);
            q.resolve(q.createValidator(cftab));
            // Shouldn't reach here if strictValidation
            fail(
                "Expected error does not occur when strictValidation is set:"
                + strictValidation);
        } catch (Throwable e) {
            properties.IgnoreInvalidMembers.set(oldIgnoreInvalidMembers);
            properties.IgnoreInvalidMembersDuringQuery.set(
                oldIgnoreInvalidMembersDuringQuery);
            if (strictValidation) {
                checkErrorMsg(
                    e,
                    "Mondrian Error:MDX object '[Measures].[Unit Salese]' not found in cube 'Sales'");
            } else {
                checkErrorMsg(
                    e,
                    "Expected error does not occur when strictValidation is set:"
                    + strictValidation);
            }
        }
    }

    public void testMultiplicationFail() {
        Set<String> functionNameSet = new HashSet<String>();
        functionNameSet.add("+");
        CustomizedFunctionTable cftab =
            getCustomizedFunctionTable(functionNameSet);

        try {
            Query q =
                getParsedQueryForExpr(
                    cftab,
                    "([Measures].[Store Cost] * [Measures].[Unit Sales])");
            q.resolve(q.createValidator(cftab));
            // Shouldn't reach here
            fail("Expected error did not occur.");
        } catch (Throwable e) {
            checkErrorMsg(e,
            "Mondrian Error:No function matches signature '<Member> * <Member>'");
        }
    }

    public void testMixingAttributesFail() {
        Set<String> functionNameSet = new HashSet<String>();
        functionNameSet.add("+");
        CustomizedFunctionTable cftab =
            getCustomizedFunctionTable(functionNameSet);

        try {
            Query q =
                getParsedQueryForExpr(
                    cftab,
                    "([Measures].[Store Cost] + [Store].[Store Country])");
            q.resolve(q.createValidator(cftab));
            // Shouldn't reach here
            fail("Expected error did not occur.");
        } catch (Throwable e) {
            checkErrorMsg(e,
                "Mondrian Error:No function matches signature '<Member> + <Level>'");
        }
    }

    public void testCrossJoinFail() {
        Set<String> functionNameSet = new HashSet<String>();
        functionNameSet.add("+");
        functionNameSet.add("-");
        functionNameSet.add("*");
        functionNameSet.add("/");
        CustomizedFunctionTable cftab =
            getCustomizedFunctionTable(functionNameSet);

        try {
            Query q =
                getParsedQueryForExpr(
                    cftab,
                    "CrossJoin([Measures].[Store Cost], [Measures].[Unit Sales])");
            q.resolve(q.createValidator(cftab));
            // Shouldn't reach here
            fail("Expected error did not occur.");
        } catch (Throwable e) {
            checkErrorMsg(e,
            "Mondrian Error:No function matches signature 'CrossJoin(<Member>, <Member>)'");
        }
    }

    public void testMeasureSlicerFail() {
        Set<String> functionNameSet = new HashSet<String>();
        functionNameSet.add("+");
        functionNameSet.add("-");
        functionNameSet.add("*");
        functionNameSet.add("/");
        CustomizedFunctionTable cftab =
            getCustomizedFunctionTable(functionNameSet);

        try {
            Query q =
                getParsedQueryForExpr(
                    cftab,
                    "([Measures].[Store Cost], [Gender].[F])");
            q.resolve(q.createValidator(cftab));
            // Shouldn't reach here
            fail("Expected error did not occur.");
        } catch (Throwable e) {
            checkErrorMsg(e,
            "Mondrian Error:No function matches signature '(<Member>, <Member>)'");
        }
    }

    public void testTupleFail() {
        Set<String> functionNameSet = new HashSet<String>();
        functionNameSet.add("+");
        functionNameSet.add("-");
        functionNameSet.add("*");
        functionNameSet.add("/");
        CustomizedFunctionTable cftab =
            getCustomizedFunctionTable(functionNameSet);

        try {
            Query q =
                getParsedQueryForExpr(
                    cftab,
                    "([Store].[USA], [Gender].[F])");
            q.resolve(q.createValidator(cftab));
            // Shouldn't reach here
            fail("Expected error did not occur.");
        } catch (Throwable e) {
            checkErrorMsg(
                e,
                "Mondrian Error:No function matches signature '(<Member>, <Member>)'");
        }
    }

    /**
     * Mondrian is not strict about referencing a dimension member in calculated
     * measures.
     *
     * The following expression passes parsing and validation.
     * Its computation is strange: the result is as if the measure is defined as
     *  ([Measures].[Store Cost] + [Measures].[Store Cost])
     */
    public void testMixingMemberLimitation() {
        Set<String> functionNameSet = new HashSet<String>();
        functionNameSet.add("+");
        CustomizedFunctionTable cftab =
            getCustomizedFunctionTable(functionNameSet);

        try {
            Query q =
                getParsedQueryForExpr(
                    cftab,
                    "([Measures].[Store Cost] + [Store].[USA])");
            q.resolve(q.createValidator(cftab));
            // Shouldn't reach here
            fail("Expected error did not occur.");
        } catch (Throwable e) {
            checkErrorMsg(e, "Expected error did not occur.");
        }
    }
}

// End CustomizedParserTest.java
