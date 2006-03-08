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
 *
 */
public class XmlaTestContext {

    private static final Logger LOGGER =
            Logger.getLogger(XmlaTestContext.class);

	public static final String CATALOG_NAME = "FoodMart";
    public static final String DATASOURCE_NAME = "MondrianFoodMart";
    public static final String DATASOURCE_DESCRIPTION = "Mondrian FoodMart Test data source";
    public static final String DATASOURCE_INFO = "Provider=Mondrian;DataSource=MondrianFoodMart;";
    public static final Map ENV = new HashMap() {
        {
            put("catalog", CATALOG_NAME);
            put("datasource", DATASOURCE_INFO);
        }
    };
    private static DataSourcesConfig.DataSources DATASOURCES;
    public static final CatalogLocator CATALOG_LOCATOR = new CatalogLocatorImpl();
    public static final String[] REQUEST_ELEMENT_NAMES = new String[]{
            "Discover", "Execute"
    };

    private Object[] DEFAULT_REQUEST_RESPONSE_PAIRS = null;

    private ServletContext servletContext;
    private String connectString;

    public XmlaTestContext() {
    	super();
    }

    public XmlaTestContext(ServletContext context) {
    	super();
    	this.servletContext = context;
    }

    public String getConnectString() {
    	if (connectString != null) {
    		return connectString;
    	}

    	if (servletContext != null) {
    		MondrianProperties.instance().populate();
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

        StringReader dsConfigReader = new StringReader(
                "<?xml version=\"1.0\"?>" +
                "<DataSources>" +
                "   <DataSource>" +
                "       <DataSourceName>" + DATASOURCE_INFO + "</DataSourceName>" +
                "       <DataSourceDescription>" + DATASOURCE_DESCRIPTION + "</DataSourceDescription>" +
                "       <URL>http://localhost:8080/mondrian/xmla</URL>" +
                "       <DataSourceInfo>" + getConnectString() + "</DataSourceInfo>" +
                "       <ProviderName>Mondrian</ProviderName>" +
                "       <ProviderType>MDP</ProviderType>" +
                "       <AuthenticationMode>Unauthenticated</AuthenticationMode>" +
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


    public File[] retrieveQueryFiles() {
        MondrianProperties properties = MondrianProperties.instance();
        String filePattern = properties.QueryFilePattern.get();

        final Pattern pattern = filePattern == null ? null : Pattern.compile(filePattern);

        File queryFilesDir = servletContext != null ? new File(XmlaTestContext.class.getResource("./queryFiles").getFile())
        										   : new File("testsrc/main/mondrian/xmla/test/queryFiles");
        LOGGER.debug("Loading XML/A test data from queryFilesDir=" + queryFilesDir.getAbsolutePath() +
                " (exists=" + queryFilesDir.exists() + ")");
        File[] files = queryFilesDir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                if (name.endsWith(".xml")) {
                    if (pattern == null) {
                        return true;
                    } else {
                        return pattern.matcher(name).matches();
                    }
                }

                return false;
            }
        });

        return files;
    }

    public static Element[] extractXmlaCycle(File file, Map env) {
        Element xmlacycleElem = XmlaUtil.text2Element(xmlFromTemplate(file, env));
        for (int i = 0; i < XmlaTestContext.REQUEST_ELEMENT_NAMES.length; i++) {
            String requestElemName = XmlaTestContext.REQUEST_ELEMENT_NAMES[i];
            Element requestElem = XmlaUtil.firstChildElement(xmlacycleElem, null, requestElemName);
            if (requestElem != null) {
                Element responseElem = XmlaUtil.firstChildElement(xmlacycleElem, null, requestElemName + "Response");
                if (responseElem == null) {
                    throw new RuntimeException("Invalid XML/A query file");
                } else {
                    return new Element[]{requestElem, responseElem};
                }
            }
        }
        return null;
    }

    private static String xmlFromTemplate(File file, Map env) {
        StringBuffer buf = new StringBuffer();
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
            String line;
            while ((line = reader.readLine()) != null) {
                buf.append(line).append("\n");
            }
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (Exception ignored) {
                }
            }
        }

        String xmlText = buf.toString();
        buf.setLength(0);
        Pattern pattern = Pattern.compile("\\$\\{([^}]+)\\}");
        Matcher matcher = pattern.matcher(xmlText);
        while (matcher.find()) {
            String varName = matcher.group(1);
            String varValue = (String) env.get(varName);
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
     * Retrieve test query files as usable objects.
     * @param env
     * @return new Object[]{fileName:String, request:org.w3c.dom.Element, resposne:org.w3c.dom.Element}
     */
    public Object[] fileRequestResponsePairs(Map env) {
        File[] files = retrieveQueryFiles();

        Object[] pairs = new Object[files.length];
        for (int i = 0; i < pairs.length; i++) {
            Element[] requestAndResponse = extractXmlaCycle(files[i], env);
            pairs[i] = new Object[] {files[i].getName(), requestAndResponse[0], requestAndResponse[1]};
        }

        return pairs;
    }

    public Object[] defaultRequestResponsePairs() {
    	if (DEFAULT_REQUEST_RESPONSE_PAIRS == null) {
    		DEFAULT_REQUEST_RESPONSE_PAIRS = fileRequestResponsePairs(ENV);
    	}
    	return DEFAULT_REQUEST_RESPONSE_PAIRS;
    }

}
