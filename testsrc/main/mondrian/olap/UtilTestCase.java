/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2004-2009 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap;

import junit.framework.TestCase;

import java.util.*;
import java.sql.Driver;

import mondrian.util.*;

/**
 * Tests for methods in {@link mondrian.olap.Util}.
 */
public class UtilTestCase extends TestCase {
    public UtilTestCase(String s) {
        super(s);
    }

    public void testParseConnectStringSimple() {
        // Simple connect string
        Util.PropertyList properties =
            Util.parseConnectString("foo=x;bar=y;foo=z");
        assertEquals("y", properties.get("bar"));
        assertEquals("y", properties.get("BAR")); // get is case-insensitive
        assertNull(properties.get(" bar")); // get does not ignore spaces
        assertEquals("z", properties.get("foo")); // later occurrence overrides
        assertNull(properties.get("kipper"));
        assertEquals(2, properties.list.size());
        assertEquals("foo=z; bar=y", properties.toString());
    }

    public void testParseConnectStringComplex() {
        Util.PropertyList properties =
            Util.parseConnectString(
                "normalProp=value;"
                + "emptyValue=;"
                + " spaceBeforeProp=abc;"
                + " spaceBeforeAndAfterProp =def;"
                + " space in prop = foo bar ;"
                + "equalsInValue=foo=bar;"
                + "semiInProp;Name=value;"
                + " singleQuotedValue = "
                + "'single quoted value ending in space ' ;"
                + " doubleQuotedValue = "
                + "\"=double quoted value preceded by equals\" ;"
                + " singleQuotedValueWithSemi = 'one; two';"
                + " singleQuotedValueWithSpecials = "
                + "'one; two \"three''four=five'");
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
     * Test case for bug <a href="http://jira.pentaho.com/browse/MONDRIAN-397">
     * MONDRIAN-397, "Connect string parser gives
     * StringIndexOutOfBoundsException instead of a meaningful error"</a>.
     */
    public void testBugMondrian397() {
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
            + "Catalog=C:\\cygwin\\home\\src\\jfreereport\\engines\\classic"
            + "\\extensions-mondrian\\demo\\steelwheels.mondrian.xml;"
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
        p(
            "Driver={SQL Server};Server={localhost};Trusted_Connection={yes};"
            + "db={Northwind};", "Provider", "MSDASQL");
        }

        // Specifying a Keyword
        //
        // To identify a keyword used after the Provider keyword, use the
        // property description of the OLE DB initialization property that you
        // want to set. For example, the property description of the standard
        // OLE DB initialization property DBPROP_INIT_LOCATION is
        // Location. Therefore, to include this property in a connection
        // string, use the keyword Location.
        p(
            "Provider='MSDASQL';Location='3Northwind'",
            "Location",
            "3Northwind");
        // Keywords can contain any printable character except for the equal
        // sign (=).
        p(
            "Jet OLE DB:System Database=c:\\system.mda",
            "Jet OLE DB:System Database",
            "c:\\system.mda");
        p(
            "Authentication;Info=Column 5",
            "Authentication;Info",
            "Column 5");
        // If a keyword contains an equal sign (=), it must be preceded by an
        // additional equal sign to indicate that it is part of the keyword.
        p(
            "Verification==Security=True",
            "Verification=Security",
            "True");
        // If multiple equal signs appear, each one must be preceded by an
        // additional equal sign.
        p("Many====One=Valid", "Many==One", "Valid");
        p("TooMany===False", "TooMany=", "False");
        // Setting Values That Use Reserved Characters
        //
        // To include values that contain a semicolon, single-quote character,
        // or double-quote character, the value must be enclosed in double
        // quotes.
        p(
            "ExtendedProperties=\"Integrated Security='SSPI';"
            + "Initial Catalog='Northwind'\"",
            "ExtendedProperties",
            "Integrated Security='SSPI';Initial Catalog='Northwind'");
        // If the value contains both a semicolon and a double-quote character,
        // the value can be enclosed in single quotes.
        p(
            "ExtendedProperties='Integrated Security=\"SSPI\";"
            + "Databse=\"My Northwind DB\"'",
            "ExtendedProperties",
            "Integrated Security=\"SSPI\";Databse=\"My Northwind DB\"");
        // The single quote is also useful if the value begins with a
        // double-quote character.
        p(
            "DataSchema='\"MyCustTable\"'",
            "DataSchema",
            "\"MyCustTable\"");
        // Conversely, the double quote can be used if the value begins with a
        // single quote.
        p(
            "DataSchema=\"'MyOtherCustTable'\"",
            "DataSchema",
            "'MyOtherCustTable'");
        // If the value contains both single-quote and double-quote characters,
        // the quote character used to enclose the value must be doubled each
        // time it occurs within the value.
        p(
            "NewRecordsCaption='\"Company''s \"new\" customer\"'",
            "NewRecordsCaption",
            "\"Company's \"new\" customer\"");
        p(
            "NewRecordsCaption=\"\"\"Company's \"\"new\"\" customer\"\"\"",
            "NewRecordsCaption",
            "\"Company's \"new\" customer\"");
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
        p(
            "Provider='MSDASQL';Location='Northwind';"
            + "Cache Authentication='True';Prompt='Complete';"
            + "Location='Customers'",
            "Location",
            "Customers");
        // One exception to the preceding rule is the Provider keyword. If this
        // keyword occurs multiple times in the string, the first occurrence is
        // used.
        p(
            "Provider='MSDASQL';Location='Northwind'; Provider='SQLOLEDB'",
            "Provider",
            "MSDASQL");
        if (false) {
            // (Not supported)
            //
            // Setting the Window Handle Property
            //
            // To set the Window Handle (DBPROP_INIT_HWND) property in a
            // connection string, a long integer value is typically used.
        }
    }

    /**
     * Unit test for {@link Util#convertOlap4jConnectStringToNativeMondrian}.
     */
    public void testConvertConnectString() {
        assertEquals(
            "Provider=Mondrian; Datasource=jdbc/SampleData;"
            + "Catalog=foodmart/FoodMart.xml;",
            Util.convertOlap4jConnectStringToNativeMondrian(
                "jdbc:mondrian:Datasource=jdbc/SampleData;"
                + "Catalog=foodmart/FoodMart.xml;"));
    }

    public void testQuoteMdxIdentifier() {
        assertEquals(
            "[San Francisco]", Util.quoteMdxIdentifier("San Francisco"));
        assertEquals(
            "[a [bracketed]] string]",
            Util.quoteMdxIdentifier("a [bracketed] string"));
        assertEquals(
            "[Store].[USA].[California]",
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
        checkReplace(
            "the quick brown fox", "coyote", "wolf",
            "the quick brown fox");

        // Empty buffer.
        checkReplace("", "coyote", "wolf", "");

        // Empty seek string. This is a bit mean!
        checkReplace("fox", "", "dog", "dogfdogodogxdog");
    }

    private static void checkReplace(
        String original, String seek, String replace, String expected)
    {
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
        assertEquals(
            "[string].[with].[a [bracket]] in it]",
            Util.implode(nasty));
    }

    public void testParseIdentifier() {
        List<Id.Segment> strings =
                Util.parseIdentifier("[string].[with].[a [bracket]] in it]");
        assertEquals(3, strings.size());
        assertEquals("a [bracket] in it", strings.get(2).name);

        strings =
            Util.parseIdentifier("[Worklog].[All].[calendar-[LANGUAGE]].js]");
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

        assertEquals(
            "abarb",
            Util.replaceProperties("a${foo}b", map));
        assertEquals(
            "twicebarbar",
            Util.replaceProperties("twice${foo}${foo}", map));
        assertEquals(
            "bar at start",
            Util.replaceProperties("${foo} at start", map));
        assertEquals(
            "xyz",
            Util.replaceProperties("x${empty}y${empty}${empty}z", map));
        assertEquals(
            "x${nonexistent}bar",
            Util.replaceProperties("x${nonexistent}${foo}", map));

        // malformed tokens are left as is
        assertEquals(
            "${malformedbarbar",
            Util.replaceProperties("${malformed${foo}${foo}", map));

        // string can contain '$'
        assertEquals("x$foo", Util.replaceProperties("x$foo", map));

        // property with empty name is always ignored -- even if it's in the map
        assertEquals("${}", Util.replaceProperties("${}", map));
        map.put("", "v");
        assertEquals("${}", Util.replaceProperties("${}", map));

        // if a property's value is null, it's as if it doesn't exist
        assertEquals("${null}", Util.replaceProperties("${null}", map));

        // nested properties are expanded, but not recursively
        assertEquals(
            "${foobarbaz}",
            Util.replaceProperties("${foo${foo}baz}", map));
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
        for (String s : UnionIterator.over(xyList, abcList)) {
            total += s + ";";
        }
        assertEquals("x;y;a;b;c;", total);

        total = "";
        for (String s : UnionIterator.over(xyList, emptyList)) {
            total += s + ";";
        }
        assertEquals("x;y;", total);

        total = "";
        for (String s : UnionIterator.over(emptyList, xyList, emptyList)) {
            total += s + ";";
        }
        assertEquals("x;y;", total);

        total = "";
        for (String s : UnionIterator.<String>over()) {
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
        for (String s : UnionIterator.over((Iterable<String>) xyList, abcList))
        {
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
     * Unit test for {@link mondrian.util.ArrayStack}.
     */
    public void testArrayStack() {
        final ArrayStack<String> stack = new ArrayStack<String>();
        assertEquals(0, stack.size());
        stack.add("a");
        assertEquals(1, stack.size());
        assertEquals("a", stack.peek());
        stack.push("b");
        assertEquals(2, stack.size());
        assertEquals("b", stack.peek());
        assertEquals("b", stack.pop());
        assertEquals(1, stack.size());
        stack.add(0, "z");
        assertEquals("a", stack.peek());
        assertEquals(2, stack.size());
        stack.push(null);
        assertEquals(3, stack.size());
        assertEquals(stack, Arrays.asList("z", "a", null));
        String z = "";
        for (String s : stack) {
            z += s;
        }
        assertEquals("zanull", z);
        stack.clear();
        try {
            String x = stack.peek();
            fail("expected error, got " + x);
        } catch (EmptyStackException e) {
            // ok
        }
        try {
            String x = stack.pop();
            fail("expected error, got " + x);
        } catch (EmptyStackException e) {
            // ok
        }
    }

    /**
     * Tests {@link Util#appendArrays(Object[], Object[][])}.
     */
    public void testAppendArrays() {
        String[] a0 = {"a", "b", "c"};
        String[] a1 = {"foo", "bar"};
        String[] empty = {};

        final String[] strings1 = Util.appendArrays(a0, a1);
        assertEquals(5, strings1.length);
        assertEquals(
            Arrays.asList("a", "b", "c", "foo", "bar"),
            Arrays.asList(strings1));

        final String[] strings2 = Util.appendArrays(
            empty, a0, empty, a1, empty);
        assertEquals(
            Arrays.asList("a", "b", "c", "foo", "bar"),
            Arrays.asList(strings2));

        Number[] n0 = {Math.PI};
        Integer[] i0 = {123, null, 45};
        Float[] f0 = {0f};

        final Number[] numbers = Util.appendArrays(n0, i0, f0);
        assertEquals(5, numbers.length);
        assertEquals(
            Arrays.asList((Number) Math.PI, 123, null, 45, 0f),
            Arrays.asList(numbers));
    }
}

// End UtilTestCase.java
