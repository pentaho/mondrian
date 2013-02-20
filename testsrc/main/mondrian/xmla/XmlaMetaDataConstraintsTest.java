/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2005 Julian Hyde
// Copyright (C) 2005-2011 Pentaho and others
// All Rights Reserved.
*/
package mondrian.xmla;

import mondrian.olap.*;
import mondrian.rolap.RolapConnectionProperties;
import mondrian.test.DiffRepository;
import mondrian.test.TestContext;

import java.util.*;

/**
 * This test creates 2 catalogs and constraints on one of them.
 * Then it runs a few queries to check that the filtering
 * occurs as expected.
 */
public class XmlaMetaDataConstraintsTest extends XmlaBaseTestCase {

    protected Map<String, String> getCatalogNameUrls(TestContext testContext) {
        if (catalogNameUrls == null) {
            catalogNameUrls = new TreeMap<String, String>();
            String connectString = testContext.getConnectString();
            Util.PropertyList connectProperties =
                        Util.parseConnectString(connectString);
            String catalog = connectProperties.get(
                RolapConnectionProperties.Catalog.name());
            catalogNameUrls.put("FoodMart", catalog);
            catalogNameUrls.put("FoodMart2", catalog);
        }
        return catalogNameUrls;
    }

    public void testDBSchemataFiltered() throws Exception {
        doTest(
            RowsetDefinition.DBSCHEMA_SCHEMATA.name());
    }

    public void testDBSchemataFilteredByRestraints() throws Exception {
        doTest(
            RowsetDefinition.DBSCHEMA_SCHEMATA.name());
    }

    public void testCatalogsFiltered() throws Exception {
        doTest(
            RowsetDefinition.DBSCHEMA_CATALOGS.name());
    }

    public void testCatalogsFilteredByRestraints() throws Exception {
        doTest(
            RowsetDefinition.DBSCHEMA_CATALOGS.name());
    }

    public void testCubesFiltered() throws Exception {
        doTest(
            RowsetDefinition.MDSCHEMA_CUBES.name());
    }

    public void testCubesFilteredByRestraints() throws Exception {
        doTest(
            RowsetDefinition.MDSCHEMA_CUBES.name());
    }

    private void doTest(String requestType)
        throws Exception
    {
        Properties props = new Properties();
        props.setProperty(REQUEST_TYPE_PROP, requestType);
        props.setProperty(DATA_SOURCE_INFO_PROP, DATA_SOURCE_INFO);
        props.setProperty(CATALOG_NAME_PROP, "FoodMart2");

        doTest(requestType, props, TestContext.instance());
    }

    protected DiffRepository getDiffRepos() {
        return DiffRepository.lookup(XmlaMetaDataConstraintsTest.class);
    }

    protected Class<? extends XmlaRequestCallback> getServletCallbackClass() {
        return null;
    }

    protected String getSessionId(Action action) {
        throw new UnsupportedOperationException();
    }
}

// End XmlaMetaDataConstraintsTest.java
