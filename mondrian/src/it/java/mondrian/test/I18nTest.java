/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2017 Hitachi Vantara..  All rights reserved.
*/

package mondrian.test;

import mondrian.olap.*;
import mondrian.rolap.RolapConnectionProperties;
import mondrian.util.Format;

import java.util.Calendar;
import java.util.Locale;

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
}

// End I18nTest.java

