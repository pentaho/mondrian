/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2005 Julian Hyde and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap;

import junit.framework.TestCase;

import java.util.Map;
import java.util.HashMap;

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
    }

    public void testConnectStringMore() {
        p("singleQuote=''''", "singleQuote", "'");
        p("doubleQuote=\"\"\"\"", "doubleQuote", "\"");
        p("empty= ;foo=bar", "empty", "");
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
        assertEquals("[Store].[USA].[California]", Util.quoteMdxIdentifier(new String[]{"Store", "USA", "California"}));
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
        StringBuffer buf = new StringBuffer(original);
        StringBuffer buf2 = Util.replace(buf, 0, seek, replace);
        assertTrue(buf == buf2);
        assertEquals(expected, buf.toString());

        // Check the String version of replace.
        assertEquals(expected, Util.replace(original, seek, replace));
    }

    public void testImplode() {
        String[] fooBar = {"foo", "bar"};
        assertEquals("[foo].[bar]", Util.implode(fooBar));

        String[] empty = {};
        assertEquals("", Util.implode(empty));

        String[] nasty = {"string", "with", "a [bracket] in it"};
        assertEquals("[string].[with].[a [bracket]] in it]",
                Util.implode(nasty));
    }

    public void testExplode() {
        String[] strings =
                Util.explode("[string].[with].[a [bracket]] in it]");
        assertEquals(3, strings.length);
        assertEquals("a [bracket] in it", strings[2]);
    }

    public void testReplaceProperties() {
        Map map = new HashMap();
        map.put("foo", "bar");
        map.put("empty", "");
        map.put("null", null);
        map.put("foobarbaz", "bang!");
        map.put("nonString", new Object() {
            public String toString() {
                return "non string value";
            }
        });
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

        // values don't have to be strings
        assertEquals("non string value", Util.replaceProperties("${nonString}", map));

        // nested properties are expanded, but not recursively
        assertEquals("${foobarbaz}", Util.replaceProperties("${foo${foo}baz}", map));
    }
}

// End UtilTestCase.java
