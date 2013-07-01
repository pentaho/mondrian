/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2010 Pentaho and others
// All Rights Reserved.
*/
package mondrian.xmla.test;

import mondrian.olap.Util;
import mondrian.rolap.RolapConnectionProperties;
import mondrian.spi.CatalogLocator;
import mondrian.spi.impl.CatalogLocatorImpl;
import mondrian.test.DiffRepository;
import mondrian.test.TestContext;
import mondrian.xmla.DataSourcesConfig;

import org.apache.log4j.Logger;

import org.eigenbase.xom.*;

import org.olap4j.impl.Olap4jUtil;

import java.io.StringReader;
import java.net.URL;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Common utilities for XML/A testing, used in test suite and
 * example XML/A web pages. Refactored from XmlaTest.
 *
 * @author Sherman Wood
 */
public class XmlaTestContext {

    private static final Logger LOGGER =
        Logger.getLogger(XmlaTestContext.class);

    public static final String CATALOG_NAME = "FoodMart";
    public static final String DATASOURCE_NAME = "FoodMart";
    public static final String DATASOURCE_DESCRIPTION =
        "Mondrian FoodMart Test data source";
    public static final String DATASOURCE_INFO =
        "Provider=Mondrian;DataSource=FoodMart;";
    public static final Map<String, String> ENV =
        Olap4jUtil.mapOf(
            "catalog", CATALOG_NAME,
            "datasource", DATASOURCE_NAME);
    private static DataSourcesConfig.DataSources DATASOURCES;
    public static final CatalogLocator CATALOG_LOCATOR =
        new CatalogLocatorImpl();
    private String connectString;

    public XmlaTestContext() {
        super();
    }

    public String getConnectString() {
        if (connectString != null) {
            return connectString;
        }

        connectString = TestContext.instance().getConnectString();

        // Deal with MySQL and other connect strings with & in them
        connectString = connectString.replaceAll("&", "&amp;");
        return connectString;
    }

    public DataSourcesConfig.DataSources dataSources() {
        if (DATASOURCES != null) {
            return DATASOURCES;
        }

        StringReader dsConfigReader = new StringReader(
            getDataSourcesString());
        try {
            final Parser xmlParser = XOMUtil.createDefaultParser();
            final DOMWrapper def = xmlParser.parse(dsConfigReader);
            DATASOURCES = new DataSourcesConfig.DataSources(def);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return DATASOURCES;
    }

    public String getDataSourcesString() {
        Util.PropertyList connectProperties =
            Util.parseConnectString(getConnectString());
        String catalogUrl = connectProperties.get(
            RolapConnectionProperties.Catalog.name());
        return
            "<?xml version=\"1.0\"?>"
            + "<DataSources>"
            + "   <DataSource>"
            + "       <DataSourceName>"
            + DATASOURCE_NAME
            + "</DataSourceName>"
            + "       <DataSourceDescription>"
            + DATASOURCE_DESCRIPTION
            + "</DataSourceDescription>"
            + "       <URL>http://localhost:8080/mondrian/xmla</URL>"
            + "       <DataSourceInfo>"
            + getConnectString()
            + "</DataSourceInfo>"
            + "       <ProviderName>Mondrian</ProviderName>"
            + "       <ProviderType>MDP</ProviderType>"
            + "       <AuthenticationMode>Unauthenticated</AuthenticationMode>"
            + "       <Catalogs>"
            + "          <Catalog name='FoodMart'><Definition>"
            + catalogUrl
            + "</Definition></Catalog>"
            + "       </Catalogs>"
            + "   </DataSource>"
            + "</DataSources>";
    }

    public static String xmlFromTemplate(
        String xmlText, Map<String, String> env)
    {
        StringBuffer buf = new StringBuffer();
        Pattern pattern = Pattern.compile("\\$\\{([^}]+)\\}");
        Matcher matcher = pattern.matcher(xmlText);
        while (matcher.find()) {
            String varName = matcher.group(1);
            String varValue = env.get(varName);
            if (varValue != null) {
                matcher.appendReplacement(buf, varValue);
            } else {
                matcher.appendReplacement(buf, "\\${$1}");
            }
        }
        matcher.appendTail(buf);

        return buf.toString();
    }

    /**
     * Returns a list of sample XML requests.
     *
     * <p>Each item is a pair of strings: {test name, request}.
     *
     * <p>NOTE: This method is called from <code>xmlaTest.jsp</code>. Do not
     * remove it if you can't find calls from Java.
     *
     * @return List of sample XML requests
     */
    public String[][] defaultRequests() {
        // Assume that the ref file is in the same tree (WEB-INF/classes) as
        // DiffRepository.class.
        URL refUrl =
            DiffRepository.class.getClassLoader().getResource(
                "mondrian/xmla/test/XmlaTest.ref.xml");
        DiffRepository diffRepos = new DiffRepository(refUrl);
        List<String[]> stringList = new ArrayList<String[]>();
        for (String testName : diffRepos.getTestCaseNames()) {
            String templateRequest = diffRepos.get(testName, "request");
            String request = xmlFromTemplate(templateRequest, ENV);
            stringList.add(new String[] {testName, request});
        }
        return stringList.toArray(new String[stringList.size()][]);
    }
}

// End XmlaTestContext.java
