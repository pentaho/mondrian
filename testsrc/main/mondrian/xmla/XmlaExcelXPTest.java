/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2002-2010 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.xmla;

import mondrian.test.DiffRepository;

/**
 * Test suite for compatibility of Mondrian XMLA with Excel XP.
 * Simba (the maker of the O2X bridge) supplied captured request/response
 * soap messages between Excel XP and SQL Server. These form the
 * basis of the output files in the  excel_XP directory.
 *
 * @author Richard M. Emberson
 * @version $Id$
 */
public class XmlaExcelXPTest extends XmlaBaseTestCase {

    protected String getSessionId(Action action) {
        return getSessionId("XmlaExcel2000Test", action);
    }

    static class Callback extends XmlaRequestCallbackImpl {
        Callback() {
            super("XmlaExcel2000Test");
        }
    }

    public XmlaExcelXPTest() {
    }

    public XmlaExcelXPTest(String name) {
        super(name);
    }

    protected Class<? extends XmlaRequestCallback> getServletCallbackClass() {
        return Callback.class;
    }

    protected DiffRepository getDiffRepos() {
        return DiffRepository.lookup(XmlaExcelXPTest.class);
    }

    public void test01() throws Exception {
        helperTest(false);
    }

    // BeginSession
    public void test02() throws Exception {
        helperTest(false);
    }

    public void test03() throws Exception {
        helperTest(true);
    }

    public void test04() throws Exception {
        helperTest(true);
    }

    public void test05() throws Exception {
        helperTest(true);
    }

    public void test06() throws Exception {
        helperTest(true);
    }

    // BeginSession
    public void test07() throws Exception {
        helperTest(false);
    }

    public void test08() throws Exception {
        helperTest(true);
    }

    public void test09() throws Exception {
        helperTest(true);
    }

    public void test10() throws Exception {
        helperTest(true);
    }

    public void test11() throws Exception {
        helperTest(true);
    }

    public void test12() throws Exception {
        helperTest(true);
    }

    public void test13() throws Exception {
        helperTest(true);
    }

    public void test14() throws Exception {
        helperTest(true);
    }

    public void test15() throws Exception {
        helperTest(true);
    }

    public void test16() throws Exception {
        helperTest(true);
    }

    public void test17() throws Exception {
        helperTest(true);
    }

    // The slicerAxis is empty in Mondrian by not empty in SQLServer.
    // The xml schema returned by SQL Server is not the version 1.0
    // schema returned by Mondrian.
    // Values are correct.
    public void _test18() throws Exception {
        helperTest(true);
    }

    public void test19() throws Exception {
        helperTest(true);
    }

    public void test20() throws Exception {
        helperTest(true);
    }

    // Same issue as test18: slicerAxis
    public void _test21() throws Exception {
        helperTest(true);
    }

    // Same issue as test18: slicerAxis
    public void _test22() throws Exception {
        helperTest(true);
    }

    public void test23() throws Exception {
        helperTest(true);
    }

    public void test24() throws Exception {
        helperTest(true);
    }

    public void testExpect01() throws Exception {
        helperTestExpect(false);
    }

    public void testExpect02() throws Exception {
        helperTestExpect(false);
    }

    public void testExpect03() throws Exception {
        helperTestExpect(true);
    }

    public void testExpect04() throws Exception {
        helperTestExpect(true);
    }

    public void testExpect05() throws Exception {
        helperTestExpect(true);
    }

    public void testExpect06() throws Exception {
        helperTestExpect(true);
    }
}

// End XmlaExcelXPTest.java
