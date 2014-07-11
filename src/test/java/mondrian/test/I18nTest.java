/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.test;

import mondrian.olap.*;
import mondrian.rolap.*;
import mondrian.util.*;

import org.olap4j.*;
import org.olap4j.mdx.*;
import org.olap4j.metadata.*;
import org.olap4j.metadata.Cube;
import org.olap4j.metadata.Schema;

import java.sql.SQLException;
import java.util.*;

/**
 * Test suite for internalization and localization.
 *
 * @see mondrian.util.FormatTest
 *
 * @author jhyde
 * @since September 22, 2005
 */
public class I18nTest extends FoodMartTestCase {
    public static final char Euro = '\u20AC';
    public static final char Nbsp = '\u00A0';
    public static final char EA = '\u00e9'; // e acute
    public static final char UC = '\u00FB'; // u circumflex

    public void testFormat() {
        // Make sure Util is loaded, so that the LocaleFormatFactory gets
        // registered.
        Util.discard(Util.nl);

        Locale spanish = new Locale("es", "ES");
        Locale german = new Locale("de", "DE");

        // Thousands and decimal separators are different in Spain
        Format numFormat = new Format("#,000.00", spanish);
        assertEquals("123.456,79", numFormat.format(new Double(123456.789)));

        // Currency too
        Format currencyFormat = new Format("Currency", spanish);
        assertEquals(
            "1.234.567,79 " + Euro,
            currencyFormat.format(new Double(1234567.789)));

        // Dates
        Format dateFormat = new Format("Medium Date", spanish);
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.YEAR, 2005);
        calendar.set(Calendar.MONTH, 0); // January, 0-based
        calendar.set(Calendar.DATE, 22);
        java.util.Date date = calendar.getTime();
        assertEquals("22-ene-05", dateFormat.format(date));

        // Dates in German
        dateFormat = new Format("Long Date", german);
        assertEquals("Samstag, Januar 22, 2005", dateFormat.format(date));
    }

    public void testAutoFrench() {
        // Create a connection in French.
        String localeName = "fr_FR";
        String resultString = "12" + Nbsp + "345,67";
        assertFormatNumber(localeName, resultString);
    }

    public void testAutoSpanish() {
        // Format a number in (Peninsular) spanish.
        assertFormatNumber("es", "12.345,67");
    }

    public void testAutoMexican() {
        // Format a number in Mexican spanish.
        assertFormatNumber("es_MX", "12,345.67");
    }

    private void assertFormatNumber(String localeName, String resultString) {
        final Util.PropertyList properties =
            TestContext.instance().getConnectionProperties().clone();
        properties.put(RolapConnectionProperties.Locale.name(), localeName);
        Connection connection =
            DriverManager.getConnection(properties, null);
        Query query = connection.parseQuery(
            "WITH MEMBER [Measures].[Foo] AS ' 12345.67 ',\n"
            + " FORMAT_STRING='#,###.00'\n"
            + "SELECT {[Measures].[Foo]} ON COLUMNS\n"
            + "FROM [Sales]");
        Result result = connection.execute(query);
        String actual = TestContext.toString(result);
        TestContext.assertEqualsVerbose(
            "Axis #0:\n"
            + "{}\n"
            + "Axis #1:\n"
            + "{[Measures].[Foo]}\n"
            + "Row #0: " + resultString + "\n",
            actual);
    }

    /** Unit test for captions and descriptions defined using annotations. */
    public void testSimple() throws SQLException {
        final OlapConnection olapConnection =
            getTestContext()
                .insertCube(
                    "<Localization>\n"
                    + "  <Locales>\n"
                    + "    <Locale locale='en'/>\n"
                    + "    <Locale locale='fr'/>\n"
                    + "    <Locale locale='fr-CA'/>\n"
                    + "    <Locale locale='de-DE'/>\n"
                    + "  </Locales>\n"
                    + "</Localization>\n")
                .getOlap4jConnection();

        final Schema olapSchema = olapConnection.getOlapSchema();

        final Set<String> localeNames = new TreeSet<String>();
        for (Locale locale : olapSchema.getSupportedLocales()) {
            localeNames.add(locale.toString());
        }
        assertEquals(
            new HashSet<String>(Arrays.asList("en", "fr", "fr_CA", "de_DE")),
            localeNames);

        olapConnection.setLocale(Locale.US);
        final Cube salesCubeUs = olapSchema.getCubes().get("Sales");
        assertEquals("Sales", salesCubeUs.getCaption());

        // Switch locales. Note that we have to re-read metadata from the
        // root (getOlapSchema()).
        olapConnection.setLocale(Locale.GERMANY);
        final Cube salesCubeGerman = olapSchema.getCubes().get("Sales");
        assertEquals("Verkaufen", salesCubeGerman.getCaption());
        assertEquals("Cube Verkaufen", salesCubeGerman.getDescription());

        olapConnection.setLocale(Locale.FRANCE);
        final Cube salesCubeFrance = olapSchema.getCubes().get("Sales");
        assertEquals("Ventes", salesCubeFrance.getCaption());
        assertEquals("Cube des ventes", salesCubeFrance.getDescription());

        // According to the olap4j spec,
        // behavior is undefined (e.g. the US sales cube might be invalid).
        // In the mondrian-olap4j driver, the cube object is the same under
        // all locales, and switches based on the connection's locale.
        assertEquals("Ventes", salesCubeUs.getCaption());

        // Reset locale.
        olapConnection.setLocale(Locale.US);
    }

    /** Unit test for captions and descriptions loaded from resource file. */
    public void testFileMissing() throws SQLException {
        getTestContext()
            .insertCube(
                "<Localization>\n"
                + "  <Locales>\n"
                + "    <Locale locale='en-US'/>\n"
                + "    <Locale locale='fr'/>\n"
                + "    <Locale locale='fr-CA'/>\n"
                + "    <Locale locale='de-DE'/>\n"
                + "  </Locales>\n"
                + "  <Translations>\n"
                + "    <Translation path='../target/test-classes/mondrian/test/I18nTest_${locale}.properties'/>\n"
                + "  </Translations>\n"
                + "</Localization>\n")
            .assertErrorList().containsError(
                "(?s).*Error reading resource file.*",
                "<Translation path='../target/test-classes/mondrian/test/I18nTest_${locale}.properties'/>");
    }

    /** Unit test for captions and descriptions loaded from resource file. */
    public void testFromFile() throws SQLException {
        final OlapConnection olapConnection =
            getTestContext()
                .insertCube(
                    "<Localization>\n"
                    + "  <Locales>\n"
                    + "    <Locale locale='en-US'/>\n"
                    + "    <Locale locale='fr'/>\n"
                    + "    <Locale locale='fr-CA'/>\n"
                    + "  </Locales>\n"
                    + "  <Translations>\n"
                    + "    <Translation path='../target/test-classes/mondrian/test/I18nTest_${locale}.properties'/>\n"
                    + "  </Translations>\n"
                    + "</Localization>\n")
                .getOlap4jConnection();

        olapConnection.setLocale(Locale.US);

        final Schema olapSchema = olapConnection.getOlapSchema();

        final Cube salesCubeUs = olapSchema.getCubes().get("Sales");

        // Resource from file overrides resource from annotation. (Is this what
        // we want?)
        assertEquals(
            "Caption of Sales cube in en-US", salesCubeUs.getCaption());

        // Switch locales. Note that we have to re-read metadata from the
        // root (getOlapSchema()).
        olapConnection.setLocale(Locale.GERMANY);
        final Cube salesCubeGerman =
            olapSchema.getCubes().get("Sales");
        assertEquals("Verkaufen", salesCubeGerman.getCaption());
        assertEquals("Cube Verkaufen", salesCubeGerman.getDescription());

        olapConnection.setLocale(Locale.FRANCE);
        final Cube salesCubeFrance =
            olapSchema.getCubes().get("Sales");
        assertEquals("Ventes", salesCubeFrance.getCaption());
        assertEquals("Cube des ventes", salesCubeFrance.getDescription());

        // According to the olap4j spec,
        // behavior is undefined (e.g. the US sales cube might be invalid).
        // In the mondrian-olap4j driver, the cube object is the same under
        // all locales, and switches based on the connection's locale.
        assertEquals("Ventes", salesCubeUs.getCaption());


        // Now the resources coming from files.

        olapConnection.setLocale(Locale.FRANCE);

        // olap4j Schema class does not have i18n APIs yet. So get resources
        // through the back door.
        assertEquals(
            "Caption of Sales schema in fr",
            ((MetadataElement) olapSchema).getCaption());
        assertEquals(
            "Description of Sales schema in fr",
            ((MetadataElement) olapSchema).getDescription());

        olapConnection.setLocale(Locale.CANADA_FRENCH);

        // Description is overridden in fr-CA, but not caption.
        assertEquals(
            "Caption of Sales schema in fr",
            ((MetadataElement) olapSchema).getCaption());
        assertEquals(
            "Description of Sales schema in fr-CA",
            ((MetadataElement) olapSchema).getDescription());

        assertEquals(
            "Caption of Unit Sales measure in fr",
            olapSchema.getCubes().get("Sales")
                .getDimensions().get("Measures")
                .getHierarchies().get(0)
                .getRootMembers().get("Unit Sales")
                .getCaption());

        assertEquals(
            "Profit",
            // TODO: should be "Caption of Profit calculated measure in fr",
            olapSchema.getCubes().get("Sales")
                .lookupMember(parse("[Measures].[Profit]"))
                .getCaption());

        assertEquals(
            "Caption of Time shared dimension in fr",
            olapSchema.getSharedDimensions().get("Time").getCaption());

        assertEquals(
            "Caption of shared Weekly hierarchy in fr",
            olapSchema.getSharedDimensions().get("Time").getHierarchies()
                .get("Weekly").getCaption());

        assertEquals(
            "Caption of shared Year level in fr",
            olapSchema.getSharedDimensions().get("Time").getHierarchies()
                .get("Weekly").getLevels().get("Year").getCaption());

        assertEquals(
            "Caption of Time dimension in Sales cube in fr",
            olapSchema.getCubes().get("Sales").getDimensions().get("Time")
                .getCaption());

        assertEquals(
            "Caption of Weekly hierarchy in Sales cube in fr",
            olapSchema.getCubes().get("Sales").getDimensions().get("Time")
                .getHierarchies().get("Weekly").getCaption());

        assertEquals(
            "Caption of shared Year level in fr",
            // TODO: should be "Caption of Year level in Sales cube in fr",
            olapSchema.getCubes().get("Sales").getDimensions().get("Time")
                .getHierarchies().get("Weekly").getLevels().get("Year")
                .getCaption());

        // The Month level does not have a caption, so it inherits the caption
        // from the Month attribute
        assertEquals(
            "Month",
            // TODO: should be "Caption of Month attribute in fr",
            olapSchema.getCubes().get("Sales").getDimensions().get("Time")
                .getHierarchies().get("Time").getLevels().get("Month")
                .getCaption());

        try {
            assertEquals(
                "Caption of Top Sellers named set in Sales cube in fr",
                olapSchema.getCubes().get("Sales").getSets().get("Top Sellers")
                    .getCaption());
        } catch (NullPointerException e) {
            // TODO: fix me
        }

        // level 1 member
        assertEquals(
            "Caption of member USA in fr",
            olapSchema.getCubes().get("Sales")
                .lookupMember(parse("[Store].[Stores].[USA]"))
                .getCaption());

        // level 2 member
        assertEquals(
            "Caption of member CA in fr",
            olapSchema.getCubes().get("Sales")
                .lookupMember(parse("[Store].[Stores].[USA].[CA]"))
                .getCaption());

        // level 2 member, resource defined against shared dimension
        assertEquals(
            "Caption of shared member OR in fr",
            olapSchema.getCubes().get("Sales")
                .lookupMember(parse("[Store].[Stores].[USA].[OR]"))
                .getCaption());

        // look for equivalent member (defined from same shared dimension) in
        // a different cube. should not be found
        assertEquals(
            "CA",
            olapSchema.getCubes().get("HR")
                .lookupMember(parse("[Store].[Stores].[USA].[CA]"))
                .getCaption());

        // same resource as previous, equivalent member from another cube
        assertEquals(
            "Caption of shared member OR in fr",
            olapSchema.getCubes().get("HR")
                .lookupMember(parse("[Store].[Stores].[USA].[OR]"))
                .getCaption());

        // all member
        assertEquals(
            "Caption of member All Stores in fr",
            olapSchema.getCubes().get("Sales")
                .lookupMember(parse("[Store].[Stores].[All Stores]"))
                .getCaption());

        // all member of attribute hierarchy
        assertEquals(
            "Caption of member All Gender in fr",
            olapSchema.getCubes().get("Sales")
                .lookupMember(parse("[Customer].[Gender].[All Gender]"))
                .getCaption());

        // level 1 member of attribute hierarchy
        assertEquals(
            "Caption of member F in fr",
            olapSchema.getCubes().get("Sales")
                .lookupMember(parse("[Customer].[Gender].[F]"))
                .getCaption());

        // TODO: test named set in schema, e.g. "[Best Customers].set.caption"
        Util.discard(Bug.olap4jUpgrade("bug-31") && Bug.BugOlap4j31Fixed);

        // TODO: test attribute hierarchy (resource coming from attribute)

        // TODO: test level in attribute hierarchy (resource coming from
        // attribute)

        // TODO: test calculated member in non-Measures dimension

        // TODO: test property

        // Reset locale.
        olapConnection.setLocale(Locale.US);
    }

    private static List<IdentifierSegment> parse(String x) {
        return IdentifierNode.parseIdentifier(x)
            .getSegmentList();
    }
}

// End I18nTest.java
