/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2005 Julian Hyde
// Copyright (C) 2005-2013 Pentaho and others
// All Rights Reserved.
*/
package mondrian.xmla;

import mondrian.olap.*;
import mondrian.olap.Util.PropertyList;
import mondrian.rolap.RolapConnectionProperties;
import mondrian.test.DiffRepository;
import mondrian.test.TestContext;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStreamReader;
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

            // read the catalog and copy it to another temp file.
            File outputFile1 = null;
            File outputFile2 = null;
            try {
                // Output
                outputFile1 = File.createTempFile("cat1", ".xml");
                outputFile2 = File.createTempFile("cat2", ".xml");
                outputFile1.deleteOnExit();
                outputFile2.deleteOnExit();
                BufferedWriter bw1 =
                    new BufferedWriter(new FileWriter(outputFile1));
                BufferedWriter bw2 =
                    new BufferedWriter(new FileWriter(outputFile2));

                // Input
                DataInputStream in =
                    new DataInputStream(Util.readVirtualFile(catalog));
                BufferedReader br =
                    new BufferedReader(new InputStreamReader(in));

                String strLine;
                while ((strLine = br.readLine()) != null)   {
                    bw1.write(
                        strLine.replaceAll("FoodMart", "FoodMart1schema"));
                    bw1.newLine();
                    bw2.write(
                        strLine.replaceAll("FoodMart", "FoodMart2schema"));
                    bw2.newLine();
                }

                in.close();
                bw1.close();
                bw2.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            catalogNameUrls.put("FoodMart1", outputFile1.getAbsolutePath());
            catalogNameUrls.put("FoodMart2", outputFile2.getAbsolutePath());
        }
        return catalogNameUrls;
    }

    protected String filterConnectString(String original) {
        PropertyList props = Util.parseConnectString(original);
        if (props.get(RolapConnectionProperties.Catalog.name()) != null) {
            props.remove(RolapConnectionProperties.Catalog.name());
        }
        return props.toString();
    }

    public void testDBSchemataFiltered() throws Exception {
        doTest(
            RowsetDefinition.DBSCHEMA_SCHEMATA.name(), "FoodMart2");
        doTest(
            RowsetDefinition.DBSCHEMA_SCHEMATA.name(), "FoodMart1");
    }

    public void testDBSchemataFilteredByRestraints() throws Exception {
        doTest(
            RowsetDefinition.DBSCHEMA_SCHEMATA.name(), "FoodMart2");
        doTest(
            RowsetDefinition.DBSCHEMA_SCHEMATA.name(), "FoodMart1");
    }

    public void testCatalogsFiltered() throws Exception {
        doTest(
            RowsetDefinition.DBSCHEMA_CATALOGS.name(), "FoodMart2");
        doTest(
            RowsetDefinition.DBSCHEMA_CATALOGS.name(), "FoodMart1");
    }

    public void testCatalogsFilteredByRestraints() throws Exception {
        doTest(
            RowsetDefinition.DBSCHEMA_CATALOGS.name(), "FoodMart2");
        doTest(
            RowsetDefinition.DBSCHEMA_CATALOGS.name(), "FoodMart1");
    }

    public void testCubesFiltered() throws Exception {
        doTest(
            RowsetDefinition.MDSCHEMA_CUBES.name(), "FoodMart2");
        doTest(
            RowsetDefinition.MDSCHEMA_CUBES.name(), "FoodMart1");
    }

    public void testCubesFilteredByRestraints() throws Exception {
        doTest(
            RowsetDefinition.MDSCHEMA_CUBES.name(), "FoodMart2");
        doTest(
            RowsetDefinition.MDSCHEMA_CUBES.name(), "FoodMart1");
    }

    private void doTest(String requestType, String catalog)
        throws Exception
    {
        Properties props = new Properties();
        props.setProperty(REQUEST_TYPE_PROP, requestType);
        props.setProperty(DATA_SOURCE_INFO_PROP, DATA_SOURCE_INFO);
        props.setProperty(CATALOG_NAME_PROP, catalog);

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
