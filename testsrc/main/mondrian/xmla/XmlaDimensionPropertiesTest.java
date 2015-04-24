/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2015 Pentaho and others
// All Rights Reserved.
*/
package mondrian.xmla;

import mondrian.test.DiffRepository;
import mondrian.test.TestContext;

import java.util.Properties;

/**
 * Test of dimension properties in xmla response.
 * Checks each property is added to its own hierarchy.
 *  - fix for MONDRIAN-2302 issue.
 *
 * @author Yury_Bakhmutski.
 */
public class XmlaDimensionPropertiesTest extends XmlaBaseTestCase {

    public void testOneHierarchyProperties() throws Exception {
        executeTest();
    }

    public void testTwoHierarchiesProperties() throws Exception {
        executeTest();
    }

    private void executeTest() throws Exception {
        final String dimensions = defineDimensions();
        TestContext context = TestContext.instance().createSubstitutingCube(
            "HR", dimensions);
        String requestType = "EXECUTE";
        Properties props = getDefaultRequestProperties(requestType);
        doTest(requestType, props, context);
    }

    private String defineDimensions() {
        String result =
            "<Dimension name=\"Store\" foreignKey=\"employee_id\" >\r\n"
            + "  <Hierarchy hasAll=\"true\" primaryKey=\"employee_id\"\r\n"
            + "      primaryKeyTable=\"employee\">\r\n"
            + "    <Join leftKey=\"store_id\" rightKey=\"store_id\">\r\n"
            + "      <Table name=\"employee\"/>\r\n"
            + "      <Table name=\"store\"/>\r\n"
            + "    </Join>\r\n"
            + "    <Level name=\"Store Country\" table=\"store\" column=\"store_country\"\r\n"
            + "        uniqueMembers=\"true\"/>\r\n"
            + "    <Level name=\"Store State\" table=\"store\" column=\"store_state\"\r\n"
            + "        uniqueMembers=\"true\"/>\r\n"
            + "    <Level name=\"Store City\" table=\"store\" column=\"store_city\"\r\n"
            + "        uniqueMembers=\"false\"/>\r\n"
            + "    <Level name=\"Store Name\" table=\"store\" column=\"store_name\"\r\n"
            + "        uniqueMembers=\"true\">\r\n"
            + "      <Property name=\"Store Manager\" column=\"store_manager\"/>\r\n"
            + "    </Level>\r\n"
            + "  </Hierarchy>\r\n"
            + "</Dimension>\r\n"
            + "<Dimension name=\"Store Type\" foreignKey=\"employee_id\">\r\n"
            + "  <Hierarchy hasAll=\"true\" primaryKeyTable=\"employee\" primaryKey=\"employee_id\">\r\n"
            + "    <Join leftKey=\"store_id\" rightKey=\"store_id\">\r\n"
            + "      <Table name=\"employee\"/>\r\n"
            + "      <Table name=\"store\"/>\r\n"
            + "    </Join>\r\n"
            + "          <Level name=\"Store Type\" table=\"store\" column=\"store_type\" uniqueMembers=\"true\">\r\n"
            + "      <Property name=\"Store Manager\" column=\"store_manager\"/>\r\n"
            + "    </Level>\r\n"
            + "  </Hierarchy>\r\n"
            + "</Dimension>\r\n";
        return result;
    }

    @Override
    protected DiffRepository getDiffRepos() {
        return DiffRepository.lookup(XmlaDimensionPropertiesTest.class);
    }

    @Override
    protected Class<? extends XmlaRequestCallback> getServletCallbackClass() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected String getSessionId(Action action) {
        throw new UnsupportedOperationException();
    }
}

// End XmlaDimensionPropertiesTest.java
