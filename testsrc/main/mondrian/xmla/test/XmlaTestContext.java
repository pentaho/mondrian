/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2006 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.xmla.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.ServletContext;

import mondrian.olap.Util;
import mondrian.olap.MondrianProperties;
import mondrian.spi.CatalogLocator;
import mondrian.spi.impl.CatalogLocatorImpl;
import mondrian.test.TestContext;
import mondrian.xmla.DataSourcesConfig;
import mondrian.xmla.XmlaUtil;

import org.apache.log4j.Logger;
import org.eigenbase.xom.DOMWrapper;
import org.eigenbase.xom.Parser;
import org.eigenbase.xom.XOMUtil;
import org.w3c.dom.Element;

/**
 * Common utilities for XML/A testing, used in test suite and
 * example XML/A web pages. Refactored from XmlaTest.
 *
 * @author Sherman Wood
 * @version $Id$
 */
public class XmlaTestContext {

    private static final Logger LOGGER =
            Logger.getLogger(XmlaTestContext.class);

    public static final String CATALOG_NAME = "FoodMart";
    public static final String DATASOURCE_NAME = "MondrianFoodMart";
    public static final String DATASOURCE_DESCRIPTION = "Mondrian FoodMart Test data source";
    public static final String DATASOURCE_INFO = "Provider=Mondrian;DataSource=MondrianFoodMart;";
    public static final Map<String, String> ENV =
        new HashMap<String, String>() {
            {
                put("catalog", CATALOG_NAME);
                put("datasource", DATASOURCE_INFO);
            }
        };
    private static DataSourcesConfig.DataSources DATASOURCES;
    public static final CatalogLocator CATALOG_LOCATOR = new CatalogLocatorImpl();
    private String connectString;

    public XmlaTestContext() {
        super();
    }

    public String getConnectString() {
        if (connectString != null) {
            return connectString;
        }

        connectString = TestContext.getConnectString();

        // Deal with MySQL and other connect strings with & in them
        connectString = connectString.replaceAll("&", "&amp;");
        return connectString;

    }

    public DataSourcesConfig.DataSources dataSources() {
        if (DATASOURCES != null) {
            return DATASOURCES;
        }

        Util.PropertyList connectProperties =
            Util.parseConnectString(getConnectString());
        String catalogUrl = connectProperties.get("catalog");

        StringReader dsConfigReader =
                new StringReader("<?xml version=\"1.0\"?>" +
                        "<DataSources>" +
                        "   <DataSource>" +
                        "       <DataSourceName>" + DATASOURCE_INFO + "</DataSourceName>" +
                        "       <DataSourceDescription>" + DATASOURCE_DESCRIPTION + "</DataSourceDescription>" +
                        "       <URL>http://localhost:8080/mondrian/xmla</URL>" +
                        "       <DataSourceInfo>" + getConnectString() + "</DataSourceInfo>" +
                        "       <ProviderName>Mondrian</ProviderName>" +
                        "       <ProviderType>MDP</ProviderType>" +
                        "       <AuthenticationMode>Unauthenticated</AuthenticationMode>" +
                        "       <Catalogs>" +
                        "          <Catalog name='FoodMart'><Definition>" +
                        catalogUrl +
                        "</Definition></Catalog>" +
                        "       </Catalogs>" +
                        "   </DataSource>" +
                        "</DataSources>");
        try {
            final Parser xmlParser = XOMUtil.createDefaultParser();
            final DOMWrapper def = xmlParser.parse(dsConfigReader);
            DATASOURCES = new DataSourcesConfig.DataSources(def);
        } catch (Exception e) {

        }

        return DATASOURCES;
    }

    public static String xmlFromTemplate(
        String xmlText, Map<String, String> env) {
        
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
}

// End XmlaTestContext.java
