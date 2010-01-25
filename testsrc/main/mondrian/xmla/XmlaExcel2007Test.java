/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2010-2010 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.xmla;

import mondrian.test.DiffRepository;

/**
 * Test suite for compatibility of Mondrian XMLA with Excel 2007.
 *
 * @author Richard M. Emberson
 * @version $Id$
 */
public class XmlaExcel2007Test extends XmlaBaseTestCase {

    protected String getSessionId(Action action) {
        return getSessionId("XmlaExcel2000Test", action);
    }

    static class Callback extends XmlaRequestCallbackImpl {
        Callback() {
            super("XmlaExcel2000Test");
        }
    }

    public XmlaExcel2007Test() {
    }

    public XmlaExcel2007Test(String name) {
        super(name);
    }

    protected Class<? extends XmlaRequestCallback> getServletCallbackClass() {
        return Callback.class;
    }

    protected DiffRepository getDiffRepos() {
        return DiffRepository.lookup(XmlaExcel2007Test.class);
    }

    /**
     * <p>Testcase for <a href="http://jira.pentaho.com/browse/MONDRIAN-679">
     * bug MONDRIAN-679, "VisualTotals gives ClassCastException when called via
     * XMLA"</a>.
     *
     * @throws Exception on error
     */
    public void test01() throws Exception {
        helperTest(false);
    }

    /**
     * Test that checks that (a) member properties are in correct format for
     * Excel 2007, (b) the slicer axis is in the correct format for Excel 2007.
     *
     * @throws Exception on error
     */
    public void testMemberPropertiesAndSlicer() throws Exception {
        helperTestExpect(false);
    }

    /**
     * Test that executes MDSCHEMA_PROPERTIES with
     * {@link org.olap4j.metadata.Property.TypeFlag#MEMBER}.
     *
     * @throws Exception on error
     */
    public void testMdschemaPropertiesMember() throws Exception {
        helperTest(false);
    }

    /**
     * Test that executes MDSCHEMA_PROPERTIES with
     * {@link org.olap4j.metadata.Property.TypeFlag#CELL}.
     *
     * @throws Exception on error
     */
    public void testMdschemaPropertiesCell() throws Exception {
        helperTest(false);
    }
}

// End XmlaExcel2007Test.java
