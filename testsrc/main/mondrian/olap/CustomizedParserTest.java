/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2007 Julian Hyde and others.
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
        ParenthesesFunDef specialPerentheseFun = new ParenthesesFunDef(Category.Numeric);
        specialFunctions.add(specialPerentheseFun);

        CustomizedFunctionTable cftab = 
            new CustomizedFunctionTable(funNameSet, specialFunctions);        
        cftab.init();
        return cftab;
    }

    private String wrapExpr(String expr) {
        return "with member [Measures].[Foo] as " +
        expr +
        "\n select from [Sales]";
    }

    private void checkErrorMsg(Throwable e, String expectedErrorMsg) {
        while (e.getCause() != null && !e.getCause().equals(e)) {
            e = e.getCause();
        }
        String actualMsg = e.getMessage();
        assertEquals(fold(expectedErrorMsg), actualMsg);        
    }

    private Query getParsedQueryForExpr(CustomizedFunctionTable cftab, String expr) {
        String mdx = wrapExpr(expr);
        Query q = getConnection().parseQuery(mdx, cftab);
        return q;
    }
    
    public void testAddition() {
        Set<String> functionNameSet = new HashSet<String>();
        functionNameSet.add("+");
        CustomizedFunctionTable cftab = getCustomizedFunctionTable(functionNameSet);
        
        try {
            Query q = 
                getParsedQueryForExpr(cftab,
                    "([Measures].[Store Cost] + [Measures].[Unit Sales])");
            q.resolve(q.createValidator(cftab));
        } catch (Throwable e) {
            fail(e.getMessage());
        }        
    }

    public void testSubtraction() {
        Set<String> functionNameSet = new HashSet<String>();
        functionNameSet.add("-");
        CustomizedFunctionTable cftab = getCustomizedFunctionTable(functionNameSet);

        try {
            Query q = 
                getParsedQueryForExpr(cftab,
                "([Measures].[Store Cost] - [Measures].[Unit Sales])");
            q.resolve(q.createValidator(cftab));
        } catch (Throwable e) {
            fail(e.getMessage());
        }
    }

    public void testSingleMultiplication() {
        Set<String> functionNameSet = new HashSet<String>();
        functionNameSet.add("*");
        CustomizedFunctionTable cftab = getCustomizedFunctionTable(functionNameSet);

        try {
            Query q = 
                getParsedQueryForExpr(cftab,
                "[Measures].[Store Cost] * [Measures].[Unit Sales]");
            q.resolve(q.createValidator(cftab));
        } catch (Throwable e) {
            fail(e.getMessage());
        }             
    }


    public void testMultipleMultiplication() {
        Set<String> functionNameSet = new HashSet<String>();
        functionNameSet.add("*");
        CustomizedFunctionTable cftab = getCustomizedFunctionTable(functionNameSet);

        try {
            Query q = 
                getParsedQueryForExpr(cftab,
                    "([Measures].[Store Cost] * [Measures].[Unit Sales] * [Measures].[Store Sales])");
            q.resolve(q.createValidator(cftab));
        } catch (Throwable e) {
            fail(e.getMessage());
        }                
    }

    public void testLiterals() {
        Set<String> functionNameSet = new HashSet<String>();
        functionNameSet.add("+");
        CustomizedFunctionTable cftab = getCustomizedFunctionTable(functionNameSet);

        try {
            Query q = 
                getParsedQueryForExpr(cftab, "([Measures].[Store Cost] + 10)");
            q.resolve(q.createValidator(cftab));
        } catch (Throwable e) {
            fail(e.getMessage());
        }
    }

    public void testMissingObjectFail() {
        Set<String> functionNameSet = new HashSet<String>();
        functionNameSet.add("+");
        CustomizedFunctionTable cftab = getCustomizedFunctionTable(functionNameSet);

        try {
            Query q = 
                getParsedQueryForExpr(cftab, "([Measures].[Store Cost] + [Measures].[Unit Salese])");
            q.resolve(q.createValidator(cftab));
            // Shouldn't reach here
            fail();
        } catch (Throwable e) {
            checkErrorMsg(e,
            "Mondrian Error:MDX object '[Measures].[Unit Salese]' not found in cube 'Sales'");            
        }
    }
    
    public void testMultiplicationFail() {
        Set<String> functionNameSet = new HashSet<String>();
        functionNameSet.add("+");
        CustomizedFunctionTable cftab = getCustomizedFunctionTable(functionNameSet);

        try {
            Query q = 
                getParsedQueryForExpr(cftab, "([Measures].[Store Cost] * [Measures].[Unit Sales])");
            q.resolve(q.createValidator(cftab));
            // Shouldn't reach here
            fail();
        } catch (Throwable e) {
            checkErrorMsg(e,
            "Mondrian Error:No function matches signature '<Member> * <Member>'");            
        }
    }

    public void testMixingAttributesFail() {
        Set<String> functionNameSet = new HashSet<String>();
        functionNameSet.add("+");
        CustomizedFunctionTable cftab = getCustomizedFunctionTable(functionNameSet);

        try {
            Query q = 
                getParsedQueryForExpr(cftab, "([Measures].[Store Cost] + [Customers].[Name])");
            q.resolve(q.createValidator(cftab));
            // Shouldn't reach here
            fail();            
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
        CustomizedFunctionTable cftab = getCustomizedFunctionTable(functionNameSet);

        try {
            Query q = 
                getParsedQueryForExpr(cftab, "CrossJoin([Measures].[Store Cost], [Measures].[Unit Sales])");
            q.resolve(q.createValidator(cftab));
            // Shouldn't reach here
            fail();            
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
        CustomizedFunctionTable cftab = getCustomizedFunctionTable(functionNameSet);

        try {
            Query q = 
                getParsedQueryForExpr(cftab, "([Measures].[Store Cost], [Gender].[F])");
            q.resolve(q.createValidator(cftab));            
            // Shouldn't reach here            
            fail();
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
        CustomizedFunctionTable cftab = getCustomizedFunctionTable(functionNameSet);

        try {
            Query q = 
                getParsedQueryForExpr(cftab, "([Store].[USA], [Gender].[F])");
            q.resolve(q.createValidator(cftab));
            // Shouldn't reach here
            fail();            
        } catch (Throwable e) {
            checkErrorMsg(e, 
            "Mondrian Error:No function matches signature '(<Member>, <Member>)'");
        }        
    }    

}

//End CustomizedParserTest.java
