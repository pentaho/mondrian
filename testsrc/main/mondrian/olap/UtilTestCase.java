/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2009 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap;

import junit.framework.TestCase;

import java.util.*;
import java.sql.Driver;

import mondrian.util.*;
import mondrian.test.TestContext;

/**
 * Tests for methods in {@link mondrian.olap.Util}.
 */
public class UtilTestCase extends TestCase {
    public UtilTestCase(String s) {
        super(s);
    }

    public void testParseConnectStringSimple() {
        // Simple connect string
        Util.PropertyList properties = Util.parseConnectString("foo=x;bar=y;foo=z");
        assertEquals("y", properties.get("bar"));
        assertEquals("y", properties.get("BAR")); // get is case-insensitive
        assertNull(properties.get(" bar")); // get does not ignore spaces
        assertEquals("z", properties.get("foo")); // later occurrence overrides
        assertNull(properties.get("kipper"));
        assertEquals(2, properties.list.size());
        assertEquals("foo=z; bar=y", properties.toString());
    }

    public void testParseConnectStringComplex() {
        Util.PropertyList properties = Util.parseConnectString(
                "normalProp=value;" +
                "emptyValue=;" +
                " spaceBeforeProp=abc;" +
                " spaceBeforeAndAfterProp =def;" +
                " space in prop = foo bar ;" +
                "equalsInValue=foo=bar;" +
                "semiInProp;Name=value;" +
                " singleQuotedValue = 'single quoted value ending in space ' ;" +
                " doubleQuotedValue = \"=double quoted value preceded by equals\" ;" +
                " singleQuotedValueWithSemi = 'one; two';" +
                " singleQuotedValueWithSpecials = 'one; two \"three''four=five'");
        assertEquals(11, properties.list.size());
        String value;
        value = properties.get("normalProp");
        assertEquals("value", value);
        value = properties.get("emptyValue");
        assertEquals("", value); // empty string, not null!
        value = properties.get("spaceBeforeProp");
        assertEquals("abc", value);
        value = properties.get("spaceBeforeAndAfterProp");
        assertEquals("def", value);
        value = properties.get("space in prop");
        assertEquals(value, "foo bar");
        value = properties.get("equalsInValue");
        assertEquals("foo=bar", value);
        value = properties.get("semiInProp;Name");
        assertEquals("value", value);
        value = properties.get("singleQuotedValue");
        assertEquals("single quoted value ending in space ", value);
        value = properties.get("doubleQuotedValue");
        assertEquals("=double quoted value preceded by equals", value);
        value = properties.get("singleQuotedValueWithSemi");
        assertEquals(value, "one; two");
        value = properties.get("singleQuotedValueWithSpecials");
        assertEquals(value, "one; two \"three'four=five");

        assertEquals(
            "normalProp=value;"
                + " emptyValue=;"
                + " spaceBeforeProp=abc;"
                + " spaceBeforeAndAfterProp=def;"
                + " space in prop=foo bar;"
                + " equalsInValue=foo=bar;"
                + " semiInProp;Name=value;"
                + " singleQuotedValue=single quoted value ending in space ;"
                + " doubleQuotedValue==double quoted value preceded by equals;"
                + " singleQuotedValueWithSemi='one; two';"
                + " singleQuotedValueWithSpecials='one; two \"three''four=five'",
            properties.toString());
    }

    public void testConnectStringMore() {
        p("singleQuote=''''", "singleQuote", "'");
        p("doubleQuote=\"\"\"\"", "doubleQuote", "\"");
        p("empty= ;foo=bar", "empty", "");
    }

    /**
     * Testcase for bug 1938151, "StringIndexOutOfBoundsException instead of a
     * meaningful error"
     */
    public void testBug1938151 () {
        Util.PropertyList properties;

        // ends in semi
        properties = Util.parseConnectString("foo=true; bar=xxx;");
        assertEquals(2, properties.list.size());

        // ends in semi+space
        properties = Util.parseConnectString("foo=true; bar=xxx; ");
        assertEquals(2, properties.list.size());

        // ends in space
        properties = Util.parseConnectString("   ");
        assertEquals(0, properties.list.size());

        // actual testcase for bug
        properties = Util.parseConnectString(
            "provider=mondrian; JdbcDrivers=org.hsqldb.jdbcDriver;"
                + "Jdbc=jdbc:hsqldb:./sql/sampledata;"
                + "Catalog=C:\\cygwin\\home\\src\\jfreereport\\engines\\classic\\extensions-mondrian\\demo\\steelwheels.mondrian.xml;"
                + "JdbcUser=sa; JdbcPassword=; ");
        assertEquals(6, properties.list.size());
        assertEquals("", properties.get("JdbcPassword"));
    }

    /**
     * Checks that <code>connectString</code> contains a property called
     * <code>name</code>, whose value is <code>value</code>.
     */
    void p(String connectString, String name, String expectedValue) {
        Util.PropertyList list = Util.parseConnectString(connectString);
        String value = list.get(name);
        assertEquals(expectedValue, value);
    }

    public void testOleDbSpec() {
        p("Provider='MSDASQL'", "Provider", "MSDASQL");
        p("Provider='MSDASQL.1'", "Provider", "MSDASQL.1");

        if (false) {
        // If no Provider keyword is in the string, the OLE DB Provider for
        // ODBC (MSDASQL) is the default value. This provides backward
        // compatibility with ODBC connection strings. The ODBC connection
        // string in the following example can be passed in, and it will
        // successfully connect.
        p("Driver={SQL Server};Server={localhost};Trusted_Connection={yes};db={Northwind};", "Provider", "MSDASQL");
        }

        // Specifying a Keyword
        //
        // To identify a keyword used after the Provider keyword, use the
        // property description of the OLE DB initialization property that you
        // want to set. For example, the property description of the standard
        // OLE DB initialization property DBPROP_INIT_LOCATION is
        // Location. Therefore, to include this property in a connection
        // string, use the keyword Location.
        p("Provider='MSDASQL';Location='3Northwind'", "Location", "3Northwind");
        // Keywords can contain any printable character except for the equal
        // sign (=).
        p("Jet OLE DB:System Database=c:\\system.mda", "Jet OLE DB:System Database", "c:\\system.mda");
        p("Authentication;Info=Column 5", "Authentication;Info", "Column 5");
        // If a keyword contains an equal sign (=), it must be preceded by an
        // additional equal sign to indicate that it is part of the keyword.
        p("Verification==Security=True", "Verification=Security", "True");
        // If multiple equal signs appear, each one must be preceded by an
        // additional equal sign.
        p("Many====One=Valid", "Many==One", "Valid");
        p("TooMany===False", "TooMany=", "False");
        // Setting Values That Use Reserved Characters
        //
        // To include values that contain a semicolon, single-quote character,
        // or double-quote character, the value must be enclosed in double
        // quotes.
        p("ExtendedProperties=\"Integrated Security='SSPI';Initial Catalog='Northwind'\"", "ExtendedProperties", "Integrated Security='SSPI';Initial Catalog='Northwind'");
        // If the value contains both a semicolon and a double-quote character,
        // the value can be enclosed in single quotes.
        p("ExtendedProperties='Integrated Security=\"SSPI\";Databse=\"My Northwind DB\"'", "ExtendedProperties", "Integrated Security=\"SSPI\";Databse=\"My Northwind DB\"");
        // The single quote is also useful if the value begins with a
        // double-quote character.
        p("DataSchema='\"MyCustTable\"'", "DataSchema", "\"MyCustTable\"");
        // Conversely, the double quote can be used if the value begins with a
        // single quote.
        p("DataSchema=\"'MyOtherCustTable'\"", "DataSchema", "'MyOtherCustTable'");
        // If the value contains both single-quote and double-quote characters,
        // the quote character used to enclose the value must be doubled each
        // time it occurs within the value.
        p("NewRecordsCaption='\"Company''s \"new\" customer\"'", "NewRecordsCaption", "\"Company's \"new\" customer\"");
        p("NewRecordsCaption=\"\"\"Company's \"\"new\"\" customer\"\"\"", "NewRecordsCaption", "\"Company's \"new\" customer\"");
        // Setting Values That Use Spaces
        //
        // Any leading or trailing spaces around a keyword or value are
        // ignored. However, spaces within a keyword or value are allowed and
        // recognized.
        p("MyKeyword=My Value", "MyKeyword", "My Value");
        p("MyKeyword= My Value ;MyNextValue=Value", "MyKeyword", "My Value");
        // To include preceding or trailing spaces in the value, the value must
        // be enclosed in either single quotes or double quotes.
        p("MyKeyword=' My Value  '", "MyKeyword", " My Value  ");
        p("MyKeyword=\"  My Value \"", "MyKeyword", "  My Value ");
        if (false) {
            // (Not supported.)
            //
            // If the keyword does not correspond to a standard OLE DB
            // initialization property (in which case the keyword value is
            // placed in the Extended Properties (DBPROP_INIT_PROVIDERSTRING)
            // property), the spaces around the value will be included in the
            // value even though quote marks are not used. This is to support
            // backward compatibility for ODBC connection strings. Trailing
            // spaces after keywords might also be preserved.
        }
        if (false) {
            // (Not supported)
            //
            // Returning Multiple Values
            //
            // For standard OLE DB initialization properties that can return
            // multiple values, such as the Mode property, each value returned
            // is separated with a pipe (|) character. The pipe character can
            // have spaces around it or not.
            //
            // Example   Mode=Deny Write|Deny Read
        }
        // Listing Keywords Multiple Times
        //
        // If a specific keyword in a keyword=value pair occurs multiple times
        // in a connection string, the last occurrence listed is used in the
        // value set.
        p("Provider='MSDASQL';Location='Northwind';Cache Authentication='True';Prompt='Complete';Location='Customers'", "Location", "Customers");
        // One exception to the preceding rule is the Provider keyword. If this
        // keyword occurs multiple times in the string, the first occurrence is
        // used.
        p("Provider='MSDASQL';Location='Northwind'; Provider='SQLOLEDB'", "Provider", "MSDASQL");
        if (false) {
            // (Not supported)
            //
            // Setting the Window Handle Property
            //
            // To set the Window Handle (DBPROP_INIT_HWND) property in a
            // connection string, a long integer value is typically used.
        }
    }

    public void testQuoteMdxIdentifier() {
        assertEquals("[San Francisco]", Util.quoteMdxIdentifier("San Francisco"));
        assertEquals("[a [bracketed]] string]", Util.quoteMdxIdentifier("a [bracketed] string"));
        assertEquals("[Store].[USA].[California]",
            Util.quoteMdxIdentifier(
                Arrays.asList(
                    new Id.Segment("Store", Id.Quoting.QUOTED),
                    new Id.Segment("USA", Id.Quoting.QUOTED),
                    new Id.Segment("California", Id.Quoting.QUOTED))));
    }

    public void testBufReplace() {
        // Replace with longer string. Search pattern at beginning & end.
        checkReplace("xoxox", "x", "yy", "yyoyyoyy");

        // Replace with shorter string.
        checkReplace("xxoxxoxx", "xx", "z", "zozoz");

        // Replace with empty string.
        checkReplace("xxoxxoxx", "xx", "", "oo");

        // Replacement string contains search string. (A bad implementation
        // might loop!)
        checkReplace("xox", "x", "xx", "xxoxx");

        // Replacement string combines with characters in the original to
        // match search string.
        checkReplace("cacab", "cab", "bb", "cabb");

        // Seek string does not exist.
        checkReplace("the quick brown fox", "coyote", "wolf",
                "the quick brown fox");

        // Empty buffer.
        checkReplace("", "coyote", "wolf", "");

        // Empty seek string. This is a bit mean!
        checkReplace("fox", "", "dog", "dogfdogodogxdog");
    }

    private static void checkReplace(
            String original, String seek, String replace, String expected) {
        // Check whether the JDK does what we expect. (If it doesn't it's
        // probably a bug in the test, not the JDK.)
        assertEquals(expected, original.replaceAll(seek, replace));

        // Check the StringBuffer version of replace.
        StringBuilder buf = new StringBuilder(original);
        StringBuilder buf2 = Util.replace(buf, 0, seek, replace);
        assertTrue(buf == buf2);
        assertEquals(expected, buf.toString());

        // Check the String version of replace.
        assertEquals(expected, Util.replace(original, seek, replace));
    }

    public void testImplode() {
        List<Id.Segment> fooBar = Arrays.asList(
            new Id.Segment("foo", Id.Quoting.UNQUOTED),
            new Id.Segment("bar", Id.Quoting.UNQUOTED));
        assertEquals("[foo].[bar]", Util.implode(fooBar));

        List<Id.Segment> empty = Collections.emptyList();
        assertEquals("", Util.implode(empty));

        List<Id.Segment> nasty = Arrays.asList(
            new Id.Segment("string", Id.Quoting.UNQUOTED),
            new Id.Segment("with", Id.Quoting.UNQUOTED),
            new Id.Segment("a [bracket] in it", Id.Quoting.UNQUOTED));
        assertEquals("[string].[with].[a [bracket]] in it]",
            Util.implode(nasty));
    }

    public void testParseIdentifier() {
        List<Id.Segment> strings =
                Util.parseIdentifier("[string].[with].[a [bracket]] in it]");
        assertEquals(3, strings.size());
        assertEquals("a [bracket] in it", strings.get(2).name);

        strings = Util.parseIdentifier("[Worklog].[All].[calendar-[LANGUAGE]].js]");
        assertEquals(3, strings.size());
        assertEquals("calendar-[LANGUAGE].js", strings.get(2).name);

        try {
            strings = Util.parseIdentifier("[foo].bar");
            Util.discard(strings);
            fail("expected exception");
        } catch (MondrianException e) {
            assertEquals(
                "Mondrian Error:Invalid member identifier '[foo].bar'",
                e.getMessage());
        }

        try {
            strings = Util.parseIdentifier("[foo].[bar");
            Util.discard(strings);
            fail("expected exception");
        } catch (MondrianException e) {
            assertEquals(
                "Mondrian Error:Invalid member identifier '[foo].[bar'",
                e.getMessage());
        }
    }

    public void testReplaceProperties() {
        Map<String, String> map = new HashMap<String, String>();
        map.put("foo", "bar");
        map.put("empty", "");
        map.put("null", null);
        map.put("foobarbaz", "bang!");
        map.put("malformed${foo", "groovy");

        assertEquals("abarb", Util.replaceProperties("a${foo}b", map));
        assertEquals("twicebarbar", Util.replaceProperties("twice${foo}${foo}", map));
        assertEquals("bar at start", Util.replaceProperties("${foo} at start", map));
        assertEquals("xyz", Util.replaceProperties("x${empty}y${empty}${empty}z", map));
        assertEquals("x${nonexistent}bar", Util.replaceProperties("x${nonexistent}${foo}", map));

        // malformed tokens are left as is
        assertEquals("${malformedbarbar", Util.replaceProperties("${malformed${foo}${foo}", map));

        // string can contain '$'
        assertEquals("x$foo", Util.replaceProperties("x$foo", map));

        // property with empty name is always ignored -- even if it's in the map
        assertEquals("${}", Util.replaceProperties("${}", map));
        map.put("", "v");
        assertEquals("${}", Util.replaceProperties("${}", map));

        // if a property's value is null, it's as if it doesn't exist
        assertEquals("${null}", Util.replaceProperties("${null}", map));

        // nested properties are expanded, but not recursively
        assertEquals("${foobarbaz}", Util.replaceProperties("${foo${foo}baz}", map));
    }

    public void testWildcard() {
        assertEquals(
            ".\\QFoo\\E.|\\QBar\\E.*\\QBAZ\\E",
            Util.wildcardToRegexp(
                Arrays.asList("_Foo_", "Bar%BAZ")));
    }

    public void testCamel() {
        assertEquals(
            "FOO_BAR",
            Util.camelToUpper("FooBar"));
        assertEquals(
            "FOO_BAR",
            Util.camelToUpper("fooBar"));
        assertEquals(
            "URL",
            Util.camelToUpper("URL"));
        assertEquals(
            "URLTO_CLICK_ON",
            Util.camelToUpper("URLtoClickOn"));
        assertEquals(
            "",
            Util.camelToUpper(""));
    }

    public void testParseCommaList() {
        assertEquals(new ArrayList<String>(), Util.parseCommaList(""));
        assertEquals(Arrays.asList("x"), Util.parseCommaList("x"));
        assertEquals(Arrays.asList("x", "y"), Util.parseCommaList("x,y"));
        assertEquals(Arrays.asList("x,y"), Util.parseCommaList("x,,y"));
        assertEquals(Arrays.asList(",x", "y"), Util.parseCommaList(",,x,y"));
        assertEquals(Arrays.asList("x,", "y"), Util.parseCommaList("x,,,y"));
        assertEquals(Arrays.asList("x,,y"), Util.parseCommaList("x,,,,y"));
        // ignore trailing comma
        assertEquals(Arrays.asList("x", "y"), Util.parseCommaList("x,y,"));
        assertEquals(Arrays.asList("x", "y,"), Util.parseCommaList("x,y,,"));
    }

    public void testUnionIterator() {
        final List<String> xyList = Arrays.asList("x", "y");
        final List<String> abcList = Arrays.asList("a", "b", "c");
        final List<String> emptyList = Collections.emptyList();

        String total = "";
        for (String s : Util.union(xyList, abcList)) {
            total += s + ";";
        }
        assertEquals("x;y;a;b;c;", total);

        total = "";
        for (String s : Util.union(xyList, emptyList)) {
            total += s + ";";
        }
        assertEquals("x;y;", total);

        total = "";
        for (String s : Util.union(emptyList, xyList, emptyList)) {
            total += s + ";";
        }
        assertEquals("x;y;", total);

        total = "";
        for (String s : Util.<String>union()) {
            total += s + ";";
        }
        assertEquals("", total);

        total = "";
        UnionIterator<String> unionIterator =
            new UnionIterator<String>(xyList, abcList);
        while (unionIterator.hasNext()) {
            total += unionIterator.next() + ";";
        }
        assertEquals("x;y;a;b;c;", total);

        if (Util.Retrowoven) {
            // Retrowoven code gives 'ArrayStoreException' when it encounters
            // 'Util.union()' applied to java.util.Iterator objects.
            return;
        }

        total = "";
        for (String s : Util.union((Iterable<String>) xyList, abcList)) {
            total += s + ";";
        }
        assertEquals("x;y;a;b;c;", total);
    }

    public void testAreOccurrencesEqual() {
        assertFalse(Util.areOccurencesEqual(Collections.<String>emptyList()));
        assertTrue(Util.areOccurencesEqual(Arrays.asList("x")));
        assertTrue(Util.areOccurencesEqual(Arrays.asList("x", "x")));
        assertFalse(Util.areOccurencesEqual(Arrays.asList("x", "y")));
        assertFalse(Util.areOccurencesEqual(Arrays.asList("x", "y", "x")));
        assertTrue(Util.areOccurencesEqual(Arrays.asList("x", "x", "x")));
        assertFalse(Util.areOccurencesEqual(Arrays.asList("x", "x", "y", "z")));
    }

    /**
     * Tests {@link mondrian.util.ServiceDiscovery}.
     */
    public void testServiceDiscovery() {
        final ServiceDiscovery<Driver>
            serviceDiscovery = ServiceDiscovery.forClass(Driver.class);
        final List<Class<Driver>> list = serviceDiscovery.getImplementor();
        assertFalse(list.isEmpty());

        // Check that discovered classes include AT LEAST:
        // JdbcOdbcDriver (in the JDK),
        // MondrianOlap4jDriver (in mondrian) and
        // XmlaOlap4jDriver (in olap4j.jar).
        List<String> expectedClassNames =
            new ArrayList<String>(
                Arrays.asList(
                    // Usually on the list, but not guaranteed:
                    // "sun.jdbc.odbc.JdbcOdbcDriver",
                    "mondrian.olap4j.MondrianOlap4jDriver",
                    "org.olap4j.driver.xmla.XmlaOlap4jDriver"));
        for (Class<Driver> driverClass : list) {
            expectedClassNames.remove(driverClass.getName());
        }
        assertTrue(expectedClassNames.toString(), expectedClassNames.isEmpty());
    }

    /**
     * Tests {@link mondrian.util.CoordinateIterator}.
     */
    public void testCoordinateIterator() {
        // no axes, should produce one result
        CoordinateIterator iter = new CoordinateIterator(new int[]{});
        assertTrue(iter.hasNext());
        assertEqualsArray(iter.next(), new int[] {});

        // one axis of length n, should produce n elements
        iter = new CoordinateIterator(new int[]{2});
        assertTrue(iter.hasNext());
        assertEqualsArray(iter.next(), new int[] {0});
        assertTrue(iter.hasNext());
        assertEqualsArray(iter.next(), new int[] {1});
        assertFalse(iter.hasNext());

        // one axis of length 0, should produce 0 elements
        iter = new CoordinateIterator(new int[]{0});
        assertFalse(iter.hasNext());

        // two axes of length 0, should produce 0 elements
        iter = new CoordinateIterator(new int[]{0, 0});
        assertFalse(iter.hasNext());

        // five axes of length 0, should produce 0 elements
        iter = new CoordinateIterator(new int[]{0, 0, 0, 0, 0});
        assertFalse(iter.hasNext());

        // two axes, neither empty
        iter = new CoordinateIterator(new int[]{2, 3});
        assertTrue(iter.hasNext());
        assertEqualsArray(iter.next(), new int[] {0, 0});
        assertTrue(iter.hasNext());
        assertEqualsArray(iter.next(), new int[] {0, 1});
        assertTrue(iter.hasNext());
        assertEqualsArray(iter.next(), new int[] {0, 2});
        assertTrue(iter.hasNext());
        assertEqualsArray(iter.next(), new int[] {1, 0});
        assertTrue(iter.hasNext());
        assertEqualsArray(iter.next(), new int[] {1, 1});
        assertTrue(iter.hasNext());
        assertEqualsArray(iter.next(), new int[] {1, 2});
        assertFalse(iter.hasNext());

        // three axes, one of length 0, should produce 0 elements
        iter = new CoordinateIterator(new int[]{10, 0, 2});
        assertFalse(iter.hasNext());
        iter = new CoordinateIterator(new int[]{0, 10, 2});
        assertFalse(iter.hasNext());

        // if any axis has negative length, produces 0 elements
        iter = new CoordinateIterator(new int[]{3, 4, 5, -6, 7});
        assertFalse(iter.hasNext());
        iter = new CoordinateIterator(new int[]{3, 4, 5, 6, -7});
        assertFalse(iter.hasNext());
        iter = new CoordinateIterator(new int[]{-3, 4, 5, 6, 7});
        assertFalse(iter.hasNext());
    }

    /**
     * Asserts that two integer arrays have equal length and contents.
     *
     * @param expected Expected integer array
     * @param actual Actual integer array
     */
    public void assertEqualsArray(int[] expected, int[] actual) {
        if (expected == null) {
            assertEquals(expected, actual);
        } else {
            List<Integer> expectedList = new ArrayList<Integer>();
            for (int i : expected) {
                expectedList.add(i);
            }
            List<Integer> actualList = new ArrayList<Integer>();
            for (int i : actual) {
                actualList.add(i);
            }
            assertEquals(expectedList, actualList);
        }
    }


    public void testTextFormatter() {
        /*

                         | 1997                                                |
                         | Q1                       | Q2                       |
                         |                          | 4                        |
                         | Unit Sales | Store Sales | Unit Sales | Store Sales |
----+----+---------------+------------+-------------+------------+-------------+
USA | CA | Los Angeles   |            |             |            |             |
    | WA | Seattle       |            |             |            |             |
    | CA | San Francisco |            |             |            |             |

                     1997
                     Q1                     Q2
                                            4
                     Unit Sales Store Sales Unit Sales Store Sales
=== == ============= ========== =========== ========== ===========
USA CA Los Angeles           12        34.5         13       35.60
    WA Seattle               12        34.5         13       35.60
    CA San Francisco         12        34.5         13       35.60

         */
        final String queryString =
            "select\n"
            + "  crossjoin(\n"
            + "    {[Time].[1997].[Q1], [Time].[1997].[Q2].[4]},\n"
            + "    {[Measures].[Unit Sales], [Measures].[Store Sales]}) on 0,\n"
            + "  {[USA].[CA].[Los Angeles],\n"
            + "   [USA].[WA].[Seattle],\n"
            + "   [USA].[CA].[San Francisco]} on 1\n"
            + "FROM [Sales]";
        assertFormat(
            queryString,
            TestContext.Format.TRADITIONAL,
            TestContext.fold(
                "Axis #0:\n"
                + "{}\n"
                + "Axis #1:\n"
                + "{[Time].[1997].[Q1], [Measures].[Unit Sales]}\n"
                + "{[Time].[1997].[Q1], [Measures].[Store Sales]}\n"
                + "{[Time].[1997].[Q2].[4], [Measures].[Unit Sales]}\n"
                + "{[Time].[1997].[Q2].[4], [Measures].[Store Sales]}\n"
                + "Axis #2:\n"
                + "{[Store].[All Stores].[USA].[CA].[Los Angeles]}\n"
                + "{[Store].[All Stores].[USA].[WA].[Seattle]}\n"
                + "{[Store].[All Stores].[USA].[CA].[San Francisco]}\n"
                + "Row #0: 6,373\n"
                + "Row #0: 13,736.97\n"
                + "Row #0: 1,865\n"
                + "Row #0: 3,917.49\n"
                + "Row #1: 6,098\n"
                + "Row #1: 12,760.64\n"
                + "Row #1: 2,121\n"
                + "Row #1: 4,444.06\n"
                + "Row #2: 439\n"
                + "Row #2: 936.51\n"
                + "Row #2: 149\n"
                + "Row #2: 327.33\n"));

        // Same query, Rectangular format
        assertFormat(
            queryString,
            TestContext.Format.COMPACT_RECTANGULAR,
            TestContext.fold(
                "                     1997       1997        1997       1997\n"
                + "                     Q1         Q1          Q2         Q2\n"
                + "                                            4          4\n"
                + "                     Unit Sales Store Sales Unit Sales Store Sales\n"
                + "=== == ============= ========== =========== ========== ===========\n"
                + "USA CA Los Angeles   6,373      13,736.97   1,865      3,917.49\n"
                + "USA WA Seattle       6,098      12,760.64   2,121      4,444.06\n"
                + "USA CA San Francisco 439        936.51      149        327.33\n"));

        // Similar query with an 'all' member on rows. Need an extra column.
        assertFormat(
            "select\n"
            + "  crossjoin(\n"
            + "    {[Time].[1997].[Q1], [Time].[1997].[Q2].[4]},\n"
            + "    {[Measures].[Unit Sales], [Measures].[Store Sales]}) on 0,\n"
            + "  {[Store],\n"
            + "   [Store].[USA],\n"
            + "   [Store].[USA].[CA],\n"
            + "   [Store].[USA].[CA].[Los Angeles],\n"
            + "   [Store].[USA].[WA]} on 1\n"
            + "FROM [Sales]",
            TestContext.Format.COMPACT_RECTANGULAR,
            TestContext.fold(
                "                              1997       1997        1997       1997\n"
                + "                              Q1         Q1          Q2         Q2\n"
                + "                                                     4          4\n"
                + "                              Unit Sales Store Sales Unit Sales Store Sales\n"
                + "========== === == =========== ========== =========== ========== ===========\n"
                + "All Stores                    66,291     139,628.35  20,179     42,878.25\n"
                + "All Stores USA                66,291     139,628.35  20,179     42,878.25\n"
                + "All Stores USA CA             16,890     36,175.20   6,382      13,605.89\n"
                + "All Stores USA CA Los Angeles 6,373      13,736.97   1,865      3,917.49\n"
                + "All Stores USA WA             30,114     63,282.86   9,896      20,926.37\n"));

        // TODO: test with rows axis empty
        // TODO: test with cols axis empty
        // TODO: test with 0 axes
        // TODO: test with 1 axes
        // TODO: test with 3 axes
        // TODO: formatter should right-justify cells
        // TODO: implement & test non-compact rect formatter
        // TODO: eliminate repeated captions (e.g. 'Q1   Q1' becomes 'Q1')
        //       but make sure that they are only eliminated if the parent
        //       is the same
    }

    private void assertFormat(
        String queryString,
        TestContext.Format format,
        String expected)
    {
        Result result =
            TestContext.instance().executeQuery(queryString);
        String resultString =
            TestContext.toString(result, format);
        TestContext.assertEqualsVerbose(
            expected,
            resultString);
    }
}

// End UtilTestCase.java
