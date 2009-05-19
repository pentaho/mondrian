/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2004-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.test.comp;

import javax.xml.parsers.DocumentBuilder;

import java.io.File;
import java.io.FilenameFilter;
import java.util.regex.Pattern;

import mondrian.olap.*;
import mondrian.test.FoodMartTestCase;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import junit.framework.TestSuite;

/**
 * Unit test based upon an XML file.
 *
 * <p>The file consists of an MDX statement and the expected result. The
 * executes the MDX statement and fails if the actual result does not match the
 * expected result.
 *
 * <p>Here is a typical XML file:
 * <blockquote><pre>&lt;mdbTest&gt;
 *     &lt;mdxQuery&gt;
 *         WITH MEMBER [Customers].[Hierarchy Name]
 *             AS '[Customers].[All Customers].[USA].[CA].hierarchy.Name'
 *         SELECT {[Customers].[Hierarchy Name]} on columns
 *         From [Sales]
 *     &lt;/mdxQuery&gt;
 *     &lt;dataResult&gt;
 *         &lt;slicer&gt;
 *             &lt;dimensions&gt;
 *                 &lt;dim&gt;[Measures]&lt;/dim&gt;
 *                 &lt;dim&gt;[Time]&lt;/dim&gt;
 *                 &lt;dim&gt;[Product]&lt;/dim&gt;
 *                 &lt;dim&gt;[Store]&lt;/dim&gt;
 *                 &lt;dim&gt;[Store Size in SQFT]&lt;/dim&gt;
 *                 &lt;dim&gt;[Store Type]&lt;/dim&gt;
 *                 &lt;dim&gt;[Promotions]&lt;/dim&gt;
 *                 &lt;dim&gt;[Education Level]&lt;/dim&gt;
 *                 &lt;dim&gt;[Marital Status]&lt;/dim&gt;
 *                 &lt;dim&gt;[Yearly Income]&lt;/dim&gt;
 *                 &lt;dim&gt;[Promotion Media]&lt;/dim&gt;
 *                 &lt;dim&gt;[Gender]&lt;/dim&gt;
 *             &lt;/dimensions&gt;
 *             &lt;tuples&gt;
 *                 &lt;tuple&gt;
 *                     &lt;member&gt;[Measures].[Unit Sales]&lt;/member&gt;
 *                     &lt;member&gt;[Time].[1997]&lt;/member&gt;
 *                     &lt;member&gt;[Product].[All Products]&lt;/member&gt;
 *                     &lt;member&gt;[Store].[All Stores]&lt;/member&gt;
 *                     &lt;member&gt;[Store Size in SQFT].[All Store Size in SQFTs]&lt;/member&gt;
 *                     &lt;member&gt;[Store Type].[All Store Types]&lt;/member&gt;
 *                     &lt;member&gt;[Promotions].[All Promotions]&lt;/member&gt;
 *                     &lt;member&gt;[Education Level].[All Education Levels]&lt;/member&gt;
 *                     &lt;member&gt;[Marital Status].[All Marital Status]&lt;/member&gt;
 *                     &lt;member&gt;[Yearly Income].[All Yearly Incomes]&lt;/member&gt;
 *                     &lt;member&gt;[Promotion Media].[All Media]&lt;/member&gt;
 *                     &lt;member&gt;[Gender].[All Gender]&lt;/member&gt;
 *                 &lt;/tuple&gt;
 *             &lt;/tuples&gt;
 *         &lt;/slicer&gt;
 *         &lt;columns&gt;
 *             &lt;dimensions&gt;
 *                 &lt;dim&gt;[Customers]&lt;/dim&gt;
 *             &lt;/dimensions&gt;
 *             &lt;tuples&gt;
 *                 &lt;tuple&gt;
 *                     &lt;member&gt;[Customers].[Hierarchy Name]&lt;/member&gt;
 *                 &lt;/tuple&gt;
 *             &lt;/tuples&gt;
 *         &lt;/columns&gt;
 *         &lt;data&gt;
 *             &lt;drow&gt;
 *                 &lt;cell&gt;Customers&lt;/cell&gt;
 *             &lt;/drow&gt;
 *         &lt;/data&gt;
 *     &lt;/dataResult&gt;
 * &lt;/mdbTest&gt;</pre>
 * </blockquote>
 */
public class ResultComparatorTest extends FoodMartTestCase {

    private File file;

    public ResultComparatorTest(String name) {
        super(name);
        file = new File(name);
    }

    public ResultComparatorTest() {
    }

    public ResultComparatorTest(File file) {
        super(file.getName());
        this.file = file;
    }

    protected void runTest() throws Exception {
        DocumentBuilder db = XmlUtility.createDomParser(
            false, true, false, new XmlUtility.UtilityErrorHandler());

        Document doc = db.parse(file);

        Element queryNode = (Element) doc.getElementsByTagName("mdxQuery").item(0);
        Element expectedResult = (Element) doc.getElementsByTagName(
                "dataResult").item(0);
        if (!isDefaultNullMemberRepresentation() &&
                resultHasDefaultNullMemberRepresentation(expectedResult)) {
            return;
        }
        String queryString = XmlUtility.decodeEncodedString(
            queryNode.getFirstChild().getNodeValue());

        Connection cxn = getConnection();
        try {
            Query query = cxn.parseQuery(queryString);
            Result result = cxn.execute(query);

            ResultComparator comp = new ResultComparator(expectedResult, result);

            comp.compareResults();
        } finally {
            cxn.close();
        }
    }

    private boolean resultHasDefaultNullMemberRepresentation(
        Element expectedResult)
    {
        return XmlUtility.toString(expectedResult).indexOf("#null") != -1;
    }

    public static TestSuite suite() {
        TestSuite suite = new TestSuite();
        MondrianProperties properties = MondrianProperties.instance();
        String filePattern = properties.QueryFilePattern.get();
        String fileDirectory = properties.QueryFileDirectory.get();

        final Pattern pattern =
            filePattern == null
                ? null
                : Pattern.compile(filePattern);
        final String directory =
            fileDirectory == null
                ? "testsrc" + File.separatorChar + "queryFiles"
                : fileDirectory;

        File[] files = new File(directory).listFiles(
            new FilenameFilter() {
                public boolean accept(File dir, String name) {
                    if (name.startsWith("query") && name.endsWith(".xml")) {
                        if (pattern == null) {
                            return true;
                        } else {
                            return pattern.matcher(name).matches();
                        }
                    }
                    return false;
                }
            });
        if (files == null) {
            files = new File[0];
        }
        for (int idx = 0; idx < files.length; idx++) {
            suite.addTest(new ResultComparatorTest(files[idx]));
        }

        return suite;
    }
}

// End ResultComparatorTest.java
