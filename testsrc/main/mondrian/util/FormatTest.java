/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2008 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.util;

import junit.framework.TestCase;

import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.Arrays;

import mondrian.olap.Util;
import mondrian.test.I18nTest;

/**
 * Unit test for {@link Format}.
 *
 * @author jhyde
 * @version $Id$
 * @since May 26, 2006
 */
public class FormatTest extends TestCase {

    private final Format.FormatLocale localeFra = Format.createLocale(
            '.', // thousandSeparator = ',' in en
            ',', // decimalPlaceholder = '.' in en
            "-", // dateSeparator = "/" in en
            "#", // timeSeparator = ":" in en
            "FF", // currencySymbol = "$" in en
//            "#.##0-00FF", // currencyFormat = "$#,##0.##" in en
            "#,##0.00FF", // currencyFormat = "$#,##0.##" in en
            new String[] {
                "", "Dim", "Lun", "Mar", "Mer", "Jeu", "Ven", "Sam"},
            new String[] {
                "", "Dimanche", "Lundi", "Mardi", "Mercredi", "Jeudi",
                "Vendredi", "Samedi"},
            new String[] {
                "Jan", "Fev", "Mar", "Avr", "Mai", "Jui", "Jui", "Aou",
                "Sep", "Oct", "Nov", "Dec", ""},
            new String[] {
                "Janvier", "Fevrier", "Mars", "Avril", "Mai", "Juin",
                "Juillet", "Aout", "Septembre", "Octobre", "Novembre",
                "Decembre", ""},
            Locale.FRENCH);

    /** Locale gleaned from Java's German locale. */
    private final Format.FormatLocale localeDe = Format.createLocale(
            Locale.GERMAN);

    final Double d = new Double(3141592.653589793);

    final Date date = makeCalendar(1969, 4, 29, 20, 9, 6); // note that month #3 == April
    final Date date2 = makeCalendar(2010, 9, 7, 6, 5, 4); // 06:05:04 am, 7th sep 2010

    /**
     * Exhaustive tests on various numbers.
     */
    public void testNumbers() {
        checkNumbersInLocale(null);
    }

    public void testFrenchNumbers() {
        checkNumbersInLocale(localeFra);
    }

    private void checkNumbersInLocale(Format.FormatLocale locale) {
        //                   format                   +6          -6           0           .6          null
        //                   ======================== =========== ============ =========== =========== =========
        checkNumbers(locale, "",                      "6",        "-6",        "0",        "0.6",      "");
        checkNumbers(locale, "0",                     "6",        "-6",        "0",        "1",        "");
        checkNumbers(locale, "0.00",                  "6.00",     "-6.00",     "0.00",     "0.60",     "");
        checkNumbers(locale, "#,##0",                 "6",        "-6",        "0",        "1",        "");
        checkNumbers(locale, "#,##0.00;;;Nil",        "6.00",     "-6.00",     "0.00",     "0.60",     "Nil");
        checkNumbers(locale, "$#,##0;($#,##0)",       "$6",       "($6)",      "$0",       "$1",       "");
        checkNumbers(locale, "$#,##0.00;($#,##0.00)", "$6.00",    "($6.00)",   "$0.00",    "$0.60",    "");
        checkNumbers(locale, "0%",                    "600%",     "-600%",     "0%",       "60%",      "");
        checkNumbers(locale, "0.00%",                 "600.00%",  "-600.00%",  "0.00%",    "60.00%",   "");
        checkNumbers(locale, "0.00E+00",              "6.00E+00", "-6.00E+00", "0.00E+00", "6.00E-01", "");
        checkNumbers(locale, "0.00E-00",              "6.00E00",  "-6.00E00",  "0.00E00",  "6.00E-01", "");
        checkNumbers(locale, "$#,##0;;\\Z\\e\\r\\o",  "$6",       "$-6",       "Zero",     "$1",       "");
        checkNumbers(locale, "#,##0.0 USD",           "6.0 USD",  "-6.0 USD",  "0.0 USD",  "0.6 USD",  "");
        checkNumbers(locale, "General Number",        "6",        "-6",        "0",        "0.6",      "");
        checkNumbers(locale, "Currency",              "$6.00",    "($6.00)",   "$0.00",    "$0.60",    "");
        checkNumbers(locale, "Fixed",                 "6",        "-6",        "0",        "1",        "");
        checkNumbers(locale, "Standard",              "6",        "-6",        "0",        "1",        "");
        checkNumbers(locale, "Percent",               "600.00%",  "-600.00%",  "0.00%",    "60.00%",   "");
        checkNumbers(locale, "Scientific",            "6.00e+00", "-6.00e+00", "0.00e+00", "6.00e-01", "");
        checkNumbers(locale, "True/False",            "True",     "True",      "False",    "True",     "False");
        checkNumbers(locale, "On/Off",                "On",       "On",        "Off",      "On",       "Off");
        checkNumbers(locale, "Yes/No",                "Yes",      "Yes",       "No",       "Yes",      "No");
    }

    private void checkNumbers(
            Format.FormatLocale locale,
            String format,
            String result6,
            String resultNeg6,
            String result0,
            String resultPoint6,
            String resultEmpty) {
        checkNumber(locale, format, new Double(6), result6);
        checkNumber(locale, format, new Double(-6), resultNeg6);
        checkNumber(locale, format, new Double(0), result0);
        checkNumber(locale, format, new Double(.6), resultPoint6);
        checkNumber(locale, format, null, resultEmpty);
        checkNumber(locale, format, Long.valueOf(6), result6);
        checkNumber(locale, format, Long.valueOf(-6), resultNeg6);
        checkNumber(locale, format, Long.valueOf(0), result0);
    }

    private void checkNumber(
            Format.FormatLocale locale, String format,
            Number number, String expectedResult) {
        if (locale == localeFra) {
            expectedResult = convertToFrench(expectedResult, format);
        }
        checkFormat(locale, number, format, expectedResult);
    }

    private static String convertToFrench(String result, String format) {
        if (result.startsWith("(") && result.endsWith(")")) {
            result = result.substring(1, result.length() - 1);
            return "(" + convertToFrench(result, format) + ")";
        }
        result = result.replace('.', '!');
        result = result.replace(',', '.');
        result = result.replace('!', ',');
        if (format.equals("Currency") && result.startsWith("$")) {
            result = result.substring(1) + "FF";
        }
        return result;
    }

    public void testTrickyNumbers() {
        checkFormat(null, new Double(40.385), "##0.0#", "40.39");
        checkFormat(null, new Double(40.386), "##0.0#", "40.39");
        checkFormat(null, new Double(40.384), "##0.0#", "40.38");
        checkFormat(null, new Double(40.385), "##0.#", "40.4");
        checkFormat(null, new Double(40.38), "##0.0#", "40.38");
        checkFormat(null, new Double(-40.38), "##0.0#", "-40.38");
        checkFormat(null, new Double(0.040385), "#0.###", "0.04");
        checkFormat(null, new Double(0.040385), "#0.000", "0.040");
        checkFormat(null, new Double(0.040385), "#0.####", "0.0404");
        checkFormat(null, new Double(0.040385), "00.####", "00.0404");
        checkFormat(null, new Double(0.040385), ".00#", ".04");
        checkFormat(null, new Double(0.040785), ".00#", ".041");
        checkFormat(null, new Double(99.9999), "##.####", "99.9999");
        checkFormat(null, new Double(99.9999), "##.###", "100");
        checkFormat(null, new Double(99.9999), "##.00#", "100.00");
        checkFormat(null, new Double(.00099), "#.00", ".00");
        checkFormat(null, new Double(.00099), "#.00#", ".001");
        checkFormat(null, new Double(12.34), "#.000##", "12.340");

        // "Standard" must use thousands separator, and round
        checkFormat(null, new Double(1234567.89), "Standard", "1,234,568");

        // must use correct alternate for 0
        checkFormat(null, new Double(0), "$#,##0;;\\Z\\e\\r\\o", "Zero");
    }

    public void testSmallNegativeNumbers() {
        // Bug 1492365, "Small negative numbers are printed as '-0'".
        checkFormat(null, new Double(-0.006), "#.0", ".0");
        checkFormat(null, new Double(-0.006), "#.00", "-.01");
        checkFormat(null, new Double(-0.0500001), "#.0", "-.1");
        checkFormat(null, new Double(-0.0499999), "#.0", ".0");

        // Percent
        checkFormat(null, new Double(-0.00006), "#.0%", ".0%");
        checkFormat(null, new Double(-0.0006), "#.0%", "-.1%");
        checkFormat(null, new Double(-0.0004), "#.0%", ".0%");
        checkFormat(null, new Double(-0.0005), "#.0%", "-.1%");
        checkFormat(null, new Double(-0.0005000001), "#.0%", "-.1%");
        checkFormat(null, new Double(-0.00006), "#.00%", "-.01%");
        checkFormat(null, new Double(-0.00004), "#.00%", ".00%");
        checkFormat(null, new Double(-0.00006), "00000.00%", "-00000.01%");
        checkFormat(null, new Double(-0.00004), "00000.00%", "00000.00%");

        // The +ve format gives "-0.01", but the negative format gives "0.0",
        // so we move onto the "Nil" format.
        checkFormat(null, new Double(-0.001), "0.##;(0.##);Nil", "Nil");
        checkFormat(null, new Double(-0.01), "0.##;(0.##);Nil", "(0.01)");
        checkFormat(null, new Double(-0.01), "0.##;(0.#);Nil", "Nil");
    }

    public void testNumberRoundingBug() {
        checkFormat(null, new Double(0.50), "0", "1");
        checkFormat(null, new Double(-1.5), "0", "-2");
        checkFormat(null, new Double(-0.50), "0", "-1");
        checkFormat(null, new Double(-0.99999999), "0.0", "-1.0");
        checkFormat(null, new Double (-0.45), "#.0", "-.5");
        checkFormat(null, new Double (-0.45), "0", "0");
        checkFormat(null, new Double(-0.49999), "0", "0");
        checkFormat(null, new Double(-0.49999), "0.0", "-0.5");
        checkFormat(null, new Double(0.49999), "0", "0");
        checkFormat(null, new Double(0.49999), "#.0", ".5");
    }

    public void testCurrencyBug() {
        // The following case illustrates an outstanding bug.
        // Should be able to override '.' to '-',
        // so result should be '3.141.592-65 FF',
        // but it's actually this stupid string where the value appears twice.
        checkFormat(localeFra, d, "#.##0-00 FF", "3141592,654-3141592,654 FF");
    }

    private static Date makeCalendar(
            final int year,
            final int month,
            final int date,
            final int hourOfDay,
            final int minute,
            final int second) {
        Calendar calendar = Calendar.getInstance();
        calendar.set(year, month - 1, date, hourOfDay, minute, second);
        return calendar.getTime();
    }

    public void testDates() {
        checkDate("dd-mmm-yy",     "29-Apr-69",  "29-Avr-69",  "29-Apr-69");
        checkDate("h:mm:ss AM/PM", "8:09:06 PM", "8#09#06 PM", "8:09:06 PM");
        checkDate("hh:mm",         "20:09",      "20#09",      "20:09");
        checkDate("Long Date",     "Tuesday, April 29, 1969",
                                   "Mardi, Avril 29, 1969",
                                   "Dienstag, April 29, 1969");
        checkDate("Medium Date",   "29-Apr-69",  "29-Avr-69",  "29-Apr-69");
        checkDate("Short Date",    "4/29/69",    "4-29-69",    "4.29.69");
        checkDate("Long Time",     "8:09:06 PM", "8#09#06 PM", "8:09:06 PM");
        checkDate("Medium Time",   "8:09 PM",    "8#09 PM",    "8:09 PM");
        checkDate("Short Time",    "20:09",      "20#09",      "20:09");
    }

    private void checkDate(String format, String en, String fr, String de) {
        // check date in english
        checkFormat(null, date, format, en);

        // check date in french
        checkFormat(localeFra, date, format, fr);

        // check date in german
        checkFormat(localeDe, date, format, de);
    }

    public void testAllTokens() {
        for (int i = 0; i < Format.tokens.length; i++) {
            Format.Token fe = Format.tokens[i];
            Object o;
            if (fe.isNumeric()) {
                o = d;
            } else if (fe.isDate()) {
                o = date;
            } else if (fe.isString()) {
                o = "mondrian";
            } else {
                o = d;
            }
            checkFormat(null, o, fe.token);
        }
    }

    public void testTrickyDates() {
        // must not throw exception
        checkFormat(null, date2, "mm/##/yy", "09/##/10");

        // must recognize lowercase "dd"
        checkFormat(null, date2, "mm/dd/yy", "09/07/10");

        // must print '7' not '07'
        checkFormat(null, date2, "mm/d/yy", "09/7/10");

        // must not decrement month by one (cuz java.util.Calendar is 0-based)
        checkFormat(null, date2, "mm/dd/yy", "09/07/10");

        // must recognize "mmm"
        checkFormat(null, date2, "mmm/dd/yyyy", "Sep/07/2010");

        // "mm" means minute, not month, when following "hh"
        checkFormat(null, date2, "hh/mm/ss", "06/05/04");

        // must recognize "Long Date" etc.
        checkFormat(null, date2, "Long Date", "Tuesday, September 07, 2010");

        // international currency symbol
        checkFormat(null, new Double(1.2), "" + Format.intlCurrencySymbol + "#", "$1");
    }

    public void testFrenchLocale() {
        Format.FormatLocale fr = Format.createLocale(Locale.FRANCE);
        assertEquals("#,##0.00 " + I18nTest.Euro, fr.currencyFormat);
        assertEquals(I18nTest.Euro + "", fr.currencySymbol);
        assertEquals("/", fr.dateSeparator);
        assertEquals(
            "[, dimanche, lundi, mardi, mercredi, jeudi, vendredi, samedi]",
            Arrays.asList(fr.daysOfWeekLong).toString());
        assertEquals(
            "[, dim., lun., mar., mer., jeu., ven., sam.]",
            Arrays.asList(fr.daysOfWeekShort).toString());
        assertEquals("[janvier, f" + I18nTest.EA + "vrier, mars, avril, mai, juin," +
            " juillet, ao" + I18nTest.UC + "t, septembre, octobre, novembre, d" + I18nTest.EA + "cembre, ]",
            Arrays.asList(fr.monthsLong).toString());
        assertEquals("[janv., f" + I18nTest.EA + "vr., mars, avr., mai, juin," +
            " juil., ao" + I18nTest.UC + "t, sept., oct., nov., d" + I18nTest.EA + "c., ]",
            Arrays.asList(fr.monthsShort).toString());
        assertEquals(',', fr.decimalPlaceholder);
        assertEquals(I18nTest.Nbsp, fr.thousandSeparator);
        assertEquals(":", fr.timeSeparator);
    }

    private void checkFormat(
            Format.FormatLocale locale,
            Object o,
            String formatString)
    {
        Format format = new Format(formatString, locale);
        String actualResult = format.format(o);
        Util.discard(actualResult);
    }

    private void checkFormat(
            Format.FormatLocale locale,
            Object o,
            String formatString,
            String expectedResult)
    {
        Format format = new Format(formatString, locale);
        String actualResult = format.format(o);
        assertEquals(expectedResult, actualResult);
    }

    public void testCache() {
        StringBuilder buf = new StringBuilder(Format.CacheLimit * 2 + 10);
        buf.append("0.");
        for (int i = 0; i < Format.CacheLimit * 2; ++i) {
            final Format format = Format.get(buf.toString(), null);
            final String s = format.format(i);
            assertEquals(i + "", s);
            buf.append("#");
        }
    }
}

// End FormatTest.java
