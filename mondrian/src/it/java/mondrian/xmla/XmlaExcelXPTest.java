/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/


package mondrian.xmla;

import mondrian.test.DiffRepository;

/**
 * Test suite for compatibility of Mondrian XMLA with Excel XP.
 * Simba (the maker of the O2X bridge) supplied captured request/response
 * soap messages between Excel XP and SQL Server. These form the
 * basis of the output files in the  excel_XP directory.
 *
 * @author Richard M. Emberson
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

    public void test01() {
        helperTest(false);
    }

    // BeginSession
    public void test02() {
        helperTest(false);
    }

    public void test03() {
        helperTest(true);
    }

    public void test04() {
        helperTest(true);
    }

    public void test05() {
        helperTest(true);
    }

    public void test06() {
        helperTest(true);
    }

    // BeginSession
    public void test07() {
        helperTest(false);
    }

    public void test08() {
        helperTest(true);
    }

    public void test09() {
        helperTest(true);
    }

    public void test10() {
        helperTest(true);
    }

    public void test11() {
        helperTest(true);
    }

    public void test12() {
        helperTest(true);
    }

    public void test13() {
        helperTest(true);
    }

    public void test14() {
        helperTest(true);
    }

    public void test15() {
        helperTest(true);
    }

    public void test16() {
        helperTest(true);
    }

    public void test17() {
        helperTest(true);
    }

    // The slicerAxis is empty in Mondrian by not empty in SQLServer.
    // The xml schema returned by SQL Server is not the version 1.0
    // schema returned by Mondrian.
    // Values are correct.
    public void _test18() {
        helperTest(true);
    }

    public void test19() {
        helperTest(true);
    }

    public void test20() {
        helperTest(true);
    }

    // Same issue as test18: slicerAxis
    public void _test21() {
        helperTest(true);
    }

    // Same issue as test18: slicerAxis
    public void _test22() {
        helperTest(true);
    }

    public void test23() {
        helperTest(true);
    }

    public void test24() {
        helperTest(true);
    }

    public void testExpect01() {
        helperTestExpect(false);
    }

    public void testExpect02() {
        helperTestExpect(false);
    }

    public void testExpect03() {
        helperTestExpect(true);
    }

    public void testExpect04() {
        helperTestExpect(true);
    }

    public void testExpect05() {
        helperTestExpect(true);
    }

    public void testExpect06() {
        helperTestExpect(true);
    }
}

// End XmlaExcelXPTest.java
