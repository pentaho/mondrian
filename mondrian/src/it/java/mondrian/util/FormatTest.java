/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2018 Hitachi Vantara..  All rights reserved.
*/

package mondrian.util;

import junit.framework.Assert;
import mondrian.olap.Util;
import mondrian.test.I18nTest;

import junit.framework.TestCase;

import java.math.BigDecimal;
import java.util.*;

/**
 * Unit test for {@link Format}.
 *
 * @author jhyde
 * @since May 26, 2006
 */
public class FormatTest extends TestCase {

    private final Format.FormatLocale localeFra = Format.createLocale(
        '.', // thousandSeparator = ',' in en
        ',', // decimalPlaceholder = '.' in en
        "-", // dateSeparator = "/" in en
        "#", // timeSeparator = ":" in en
        "FF", // currencySymbol = "$" in en
//      "#.##0-00FF", // currencyFormat = "$#,##0.##" in en
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
        Locale.GERMANY);

    final Number d = new BigDecimal("3141592.653589793");

    // note that month #3 == April
    final Date date = makeCalendar(1969, 4, 29, 20, 9, 6);

    // 06:05:04 am, 7th sep 2010
    final Date date2 = makeCalendar(2010, 9, 7, 6, 5, 4);

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
        //  format         +6          -6           0           .6          null
        //  ============== =========== ============ =========== =========== ====
        checkNumbers(
            "",            "6",        "-6",        "0",        "0.6",      "",
            locale);
        checkNumbers(
            "0",           "6",        "-6",        "0",        "1",        "",
            locale);
        checkNumbers(
            "0.00",        "6.00",     "-6.00",     "0.00",     "0.60",     "",
            locale);
        checkNumbers(
            "#,##0",       "6",        "-6",        "0",        "1",        "",
            locale);
        checkNumbers(
            "#,##0.00;;;N", "6.00",    "-6.00",     "0.00",     "0.60",     "N",
            locale);
        checkNumbers(
            "$#,##0;($#,##0)", "$6",   "($6)",      "$0",       "$1",       "",
            locale);
        checkNumbers(
            "$#,##0.00;($#,##0.00)", "$6.00", "($6.00)", "$0.00", "$0.60",  "",
            locale);
        checkNumbers(
            "0%",          "600%",     "-600%",     "0%",       "60%",      "",
            locale);
        checkNumbers(
            "0.00%",       "600.00%",  "-600.00%",  "0.00%",    "60.00%",   "",
            locale);
        checkNumbers(
            "0.00E+00",    "6.00E+00", "-6.00E+00", "0.00E+00", "6.00E-01", "",
            locale);
        checkNumbers(
            "0.00E-00",    "6.00E00",  "-6.00E00",  "0.00E00",  "6.00E-01", "",
            locale);
        checkNumbers(
            "$#,##0;;\\Z\\e\\r\\o", "$6", "-$6",    "Zero",     "$1",       "",
            locale);
        checkNumbers(
            "#,##0.0 USD", "6.0 USD",  "-6.0 USD",  "0.0 USD",  "0.6 USD",  "",
            locale);
        checkNumbers(
            "General Number", "6",     "-6",        "0",        "0.6",      "",
            locale);
        checkNumbers(
            "Currency",    "$6.00",    "($6.00)",   "$0.00",    "$0.60",    "",
            locale);
        checkNumbers(
            "Fixed",       "6",        "-6",        "0",        "1",        "",
            locale);
        checkNumbers(
            "Standard",    "6",        "-6",        "0",        "1",        "",
            locale);
        checkNumbers(
            "Percent",     "600.00%",  "-600.00%",  "0.00%",    "60.00%",   "",
            locale);
        checkNumbers(
            "Scientific",  "6.00e+00", "-6.00e+00", "0.00e+00", "6.00e-01", "",
            locale);
        checkNumbers(
            "True/False",  "True",     "True",      "False",    "True", "False",
            locale);
        checkNumbers(
            "On/Off",      "On",       "On",        "Off",      "On",     "Off",
            locale);
        checkNumbers(
            "Yes/No",      "Yes",      "Yes",       "No",       "Yes",     "No",
            locale);
    }

    private void checkNumbers(
        String format,
        String result6,
        String resultNeg6,
        String result0,
        String resultPoint6,
        String resultEmpty,
        Format.FormatLocale locale)
    {
        checkNumber(locale, format, new BigDecimal("6"), result6);
        checkNumber(locale, format, new BigDecimal("-6"), resultNeg6);
        checkNumber(locale, format, new BigDecimal("0"), result0);
        checkNumber(locale, format, new BigDecimal(".6"), resultPoint6);
        checkNumber(locale, format, null, resultEmpty);
        checkNumber(locale, format, 6L, result6);
        checkNumber(locale, format, -6L, resultNeg6);
        checkNumber(locale, format, 0L, result0);
    }

    private void checkNumber(
        Format.FormatLocale locale,
        String format,
        Number number,
        String expectedResult)
    {
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
        checkFormat(null, new BigDecimal("40.385"), "##0.0#", "40.39");
        checkFormat(null, new BigDecimal("40.386"), "##0.0#", "40.39");
        checkFormat(null, new BigDecimal("40.384"), "##0.0#", "40.38");
        checkFormat(null, new BigDecimal("40.385"), "##0.#", "40.4");
        checkFormat(null, new BigDecimal("40.38"), "##0.0#", "40.38");
        checkFormat(null, new BigDecimal("-40.38"), "##0.0#", "-40.38");
        checkFormat(null, new BigDecimal("0.040385"), "#0.###", "0.04");
        checkFormat(null, new BigDecimal("0.040385"), "#0.000", "0.040");
        checkFormat(null, new BigDecimal("0.040385"), "#0.####", "0.0404");
        checkFormat(null, new BigDecimal("0.040385"), "00.####", "00.0404");
        checkFormat(null, new BigDecimal("0.040385"), ".00#", ".04");
        checkFormat(null, new BigDecimal("0.040785"), ".00#", ".041");
        checkFormat(null, new BigDecimal("99.9999"), "##.####", "99.9999");
        checkFormat(null, new BigDecimal("99.9999"), "##", "100");
        checkFormat(null, new BigDecimal("99.9999"), "##.#", "100.");
        checkFormat(null, new BigDecimal("99.9999"), "##.###", "100.");
        checkFormat(null, new BigDecimal("99.9999"), "##.00#", "100.00");
        checkFormat(null, new BigDecimal(".00099"), "#.00", ".00");
        checkFormat(null, new BigDecimal(".00099"), "#.00#", ".001");
        checkFormat(null, new BigDecimal("12.34"), "#.000##", "12.340");

        // "Standard" must use thousands separator, and round
        checkFormat(
            null, new BigDecimal("1234567.89"), "Standard", "1,234,568");

        // must use correct alternate for 0
        checkFormat(null, new BigDecimal("0"), "$#,##0;;\\Z\\e\\r\\o", "Zero");

        // If there is a '.' in the format string SSAS always prints it, even
        // if there are no digits right to decimal.
        checkFormat(null, new BigDecimal("23"), "#.#", "23.");
        checkFormat(null, new BigDecimal("0"), "#.#", ".");

        // escaped semicolon
        final String formatString = "$\\;#;(\\;#);\\;\\Z";
        checkFormat(null, new BigDecimal("1"), formatString, "$;1");
        checkFormat(null, new BigDecimal("-1"), formatString, "(;1)");
        checkFormat(null, new BigDecimal("0"), formatString, ";Z");
        checkFormat(null, null, formatString, "");

        checkFormat(null, new Double("1.9999999999999995E-6"), "#.#######", ".000002");
        checkFormat(null, new Double("4.699999999999999E-6"), "#.#######", ".0000047");
    }

    /**
     * Test case for bug <a href="http://jira.pentaho.com/browse/MONDRIAN-186">
     * MONDRIAN-186</a>, "Small negative numbers are printed as '-0'".
     */
    public void testSmallNegativeNumbers() {
        checkFormat(null, new BigDecimal("-0.006"), "#.0", ".0");
        checkFormat(null, new BigDecimal("-0.006"), "#.00", "-.01");
        checkFormat(null, new BigDecimal("-0.0500001"), "#.0", "-.1");
        checkFormat(null, new BigDecimal("-0.0499999"), "#.0", ".0");

        // Percent
        checkFormat(null, new BigDecimal("-0.00006"), "#.0%", ".0%");
        checkFormat(null, new BigDecimal("-0.0006"), "#.0%", "-.1%");
        checkFormat(null, new BigDecimal("-0.0004"), "#.0%", ".0%");
        checkFormat(null, new BigDecimal("-0.0005"), "#.0%", "-.1%");
        checkFormat(null, new BigDecimal("-0.0005000001"), "#.0%", "-.1%");
        checkFormat(null, new BigDecimal("-0.00006"), "#.00%", "-.01%");
        checkFormat(null, new BigDecimal("-0.00004"), "#.00%", ".00%");
        checkFormat(
            null, new BigDecimal("-0.00006"), "00000.00%", "-00000.01%");
        checkFormat(null, new BigDecimal("-0.00004"), "00000.00%", "00000.00%");
    }

    /**
     * When there are format strings for positive and negative numbers, and
     * a number is too small to appear in either format string, it underflows
     * to 'Nil', and gets to use a third format.
     */
    public void testNil() {
        // The +ve format gives "-0.01", but the negative format gives "0.0",
        // so we move onto the "Nil" format.
        checkFormat(null, new BigDecimal("-0.001"), "0.##;(0.##);Nil", "Nil");
        checkFormat(null, new BigDecimal("-0.01"), "0.##;(0.##);Nil", "(0.01)");
        checkFormat(null, new BigDecimal("-0.01"), "0.##;(0.#);Nil", "Nil");

        // Bug MONDRIAN-434. If there are only two sections, the default third
        // section is '.'.
        checkFormat(null, new BigDecimal("0.00001"), "#.##;(#.##)", ".");
        checkFormat(null, new BigDecimal("0.001"), "0.##;(0.##)", "0.");
        checkFormat(null, new BigDecimal("-0.001"), "0.##;(0.##)", "0.");

        // Zero value and varying numbers of format strings.
        checkFormat(
            null, BigDecimal.ZERO, "\\P\\o\\s", "Pos");
        checkFormat(
            null, BigDecimal.ZERO, "\\P\\o\\s;\\N\\e\\g", "Pos");
        checkFormat(
            null, BigDecimal.ZERO, "\\P\\o\\s;\\N\\e\\g;\\Z\\e\\r\\o", "Zero");
        checkFormat(
            null, BigDecimal.ZERO,
            "\\P\\o\\s;\\N\\e\\g;\\Z\\e\\r\\o;\\N\\u\\l\\l", "Zero");

        // Small negative value and varying numbers of format strings.
        checkFormat(
            null, new BigDecimal("-0.00001"), "\\P\\o\\s", "-Pos");
        checkFormat(
            null, new BigDecimal("-0.00001"), "\\P\\o\\s;\\N\\e\\g", "Neg");
        checkFormat(
            null, new BigDecimal("-0.00001"),
            "\\P\\o\\s;\\N\\e\\g;\\Z\\e\\r\\o", "Neg");
        checkFormat(
            null, new BigDecimal("-0.00001"),
            "\\P\\o\\s;\\N\\e\\g;\\Z\\e\\r\\o;\\N\\u\\l\\l", "Neg");

        checkFormat(
            null, new BigDecimal("-0.001"), "\\P\\o\\s;\\N\\e\\g", "Neg");

        // In the following two cases, note that a small number uses the 3rd
        // format string (for zero) if it underflows the 1st or 2nd format
        // string (for positive or negative numbers). But if underflow is not
        // possible (as in the case of the 'Neg' format string),
        checkFormat(
            null, new BigDecimal("-0.001"), "#.#;(#.#);\\Z\\e\\r\\o",
            "Zero");
        checkFormat(
            null, new BigDecimal("-0.001"), "\\P\\o\\s;\\N\\e\\g;\\Z\\e\\r\\o",
            "Neg");
    }

    /**
     * Null values use the fourth format.
     */
    public void testNull() {
        // Null value with different numbers of strings
        checkFormat(
            null, null, "\\P\\o\\s", "");
        checkFormat(
            null, null, "\\P\\o\\s;\\N\\e\\g", "");
        checkFormat(
            null, null, "\\P\\o\\s;\\N\\e\\g;\\Z\\e\\r\\o", "");
        checkFormat(
            null, null, "\\P\\o\\s;\\N\\e\\g;\\Z\\e\\r\\o;\\N\\u\\l\\l",
            "Null");
        checkFormat(
            null, null, "\\P\\o\\s;;;\\N\\u\\l\\l", "Null");
        checkFormat(
            null, null, "\\P\\o\\s;;;", "");
    }

    public void testNegativeZero() {
        checkFormat(null, new BigDecimal("-0.0"), "#0.000", "0.000");
        checkFormat(null, new BigDecimal("-0.0"), "#0", "0");
        checkFormat(null, new BigDecimal("-0.0"), "#0.0", "0.0");
    }

    /**
     * Test case for bug <a href="http://jira.pentaho.com/browse/MONDRIAN-686">
     * MONDRIAN-686</a>, "Regression: JPivot output invalid - New Variance
     * Percent column".
     */
    public void testPercentWithStyle() {
        checkFormat(
            null,
            new BigDecimal("0.0364"),
            "|#.00%|style='green'",
            "|3.64%|style='green'");
    }

    /**
     * Test case for bug <a href="http://jira.pentaho.com/browse/MONDRIAN-687">
     * MONDRIAN-687</a>, "Format treats negative numbers differently than SSAS".
     */
    public void testNegativePercentWithStyle() {
        if (Bug.BugMondrian687Fixed) {
            checkFormat(
                null,
                new BigDecimal("-0.0364"),
                "|#.00%|style=red",
                "-|3.64%|style=red");
        } else {
            checkFormat(
                null,
                new BigDecimal("-0.0364"),
                "|#.00%|style='red'",
                "|-3.64%|style='red'"); // confirmed on SSAS 2005
        }

        // exercise code for long (and int) values
        if (Bug.BugMondrian687Fixed) {
            checkFormat(
                null,
                -364,
                "|#.00|style=red",
                "-|364.00|style=red");
        } else {
            checkFormat(
                null,
                -364,
                "|#.00|style=red",
                "|-364.00|style=red"); // confirmed on SSAS 2005
        }

        // now with multiple alternate formats
        checkFormat(
            null,
            364,
            "|#.00|style='green';|-#.000|style='red'",
            "|364.00|style='green'"); // confirmed on SSAS 2005
        checkFormat(
            null,
            -364,
            "|#.00|style='green';|-#.000|style='red'",
            "|-364.000|style='red'"); // confirmed on SSAS 2005
    }

    /**
     * Single quotes in format string. SSAS 2005 removes them; Mondrian should
     * also.
     */
    public void testSingleQuotes() {
        if (Bug.BugMondrian687Fixed) {
            checkFormat(
                null,
                3.64,
                "|#.00|style='deep red'",
                "-|364.00|style=deep red"); // confirmed on SSAS 2005
            checkFormat(
                null,
                3.64,
                "|#.00|style=\\'deep red\\'",
                "-|364.00|style='deep red'"); // confirmed on SSAS 2005
        } else {
            checkFormat(
                null,
                -364,
                "|#.00|style='deep red'",
                "|-364.00|style='deep red'");
        }
    }

    public void testNegativePercent() {
        checkFormat(null, new BigDecimal("-0.0364"), "#.00%", "-3.64%");
        checkFormat(null, new BigDecimal("0.0364"), "#.00%", "3.64%");
    }

    public void testNumberRoundingBug() {
        checkFormat(null, new BigDecimal("0.50"), "0", "1");
        checkFormat(null, new BigDecimal("-1.5"), "0", "-2");
        checkFormat(null, new BigDecimal("-0.50"), "0", "-1");
        checkFormat(null, new BigDecimal("-0.99999999"), "0.0", "-1.0");
        checkFormat(null, new BigDecimal("-0.45"), "#.0", "-.5");
        checkFormat(null, new BigDecimal("-0.45"), "0", "0");
        checkFormat(null, new BigDecimal("-0.49999"), "0", "0");
        checkFormat(null, new BigDecimal("-0.49999"), "0.0", "-0.5");
        checkFormat(null, new BigDecimal("0.49999"), "0", "0");
        checkFormat(null, new BigDecimal("0.49999"), "#.0", ".5");
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
        final int second)
    {
        Calendar calendar = Calendar.getInstance();
        calendar.set(year, month - 1, date, hourOfDay, minute, second);
        return calendar.getTime();
    }

    public void testDates() {
        checkDate("dd-mmm-yy",     "29-Apr-69",  "29-Avr-69",  "29-Apr-69");
        checkDate("h:mm:ss AM/PM", "8:09:06 PM", "8#09#06 PM", "8:09:06 PM");
        checkDate("hh:mm",         "20:09",      "20#09",      "20:09");
        checkDate(
            "Long Date",     "Tuesday, April 29, 1969",
            "Mardi, Avril 29, 1969", "Dienstag, April 29, 1969");
        checkDate("Medium Date",   "29-Apr-69",  "29-Avr-69",  "29-Apr-69");
        checkDate("Short Date",    "4/29/69",    "4-29-69",    "4.29.69");
        checkDate("Long Time",     "8:09:06 PM", "8#09#06 PM", "8:09:06 PM");
        checkDate("Medium Time",   "8:09 PM",    "8#09 PM",    "8:09 PM");
        checkDate("Short Time",    "20:09",      "20#09",      "20:09");
        checkDate("mmmmm-dd", "April-29", "Avril-29", "April-29");
        checkDate("M-dd", "4-29", "4-29", "4-29");
        checkDate("hh M HH:mm", "20 4 20:09", "20 4 20#09", "20 4 20:09");
        checkDate("MMMMM-dd", "April-29", "Avril-29", "April-29");
        checkDate("MM-dd", "04-29", "04-29", "04-29");
        checkDate("MMM-dd", "Apr-29", "Avr-29", "Apr-29");
        checkDate("MMMMM-dd-yyyy HH:mm AM/PM", "April-29-1969 20:09 PM", "Avril-29-1969 20#09 PM", "April-29-1969 20:09 PM");
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
        for (Format.Token fe : Format.getTokenList()) {
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
        // All examples have been checked with Excel2003 and AS2005.

        checkFormat(null, date2, "y", "250");
        checkFormat(null, date2, "yy", "10");
        checkFormat(null, date2, "yyy", "10250");

        checkFormat(null, date2, "#", "40428"); // days since 1900
        checkFormat(null, date2, "x#", "x40428");
        checkFormat(null, date2, "x#y", "x40428y");
        checkFormat(null, date2, "x/y", "x/250");

        // Using a date format (such as 'y') or separator ('/' or ':') forces
        // into date mode. '#' is no longer recognized as a numeric format
        // string.
        checkFormat(null, date2, "x/y/#", "x/250/#");
        checkFormat(null, date2, "xy#", "x250#");
        checkFormat(null, date2, "x/#", "x/#");
        checkFormat(null, date2, "x:#", "x:#");
        checkFormat(null, date2, "x-#", "x-40428"); // '-' is not special
        checkFormat(null, date2, "x #", "x 40428"); // ' ' is not special

        // must not throw exception
        checkFormat(null, date2, "mm/##/yy", "09/##/10");

        // must recognize lowercase "dd"
        checkFormat(null, date2, "mm/dd/yy", "09/07/10");

        // must print '7' not '07'
        checkFormat(null, date2, "mm/d/yy", "09/7/10");

        // must not decrement month by one (cuz java.util.Calendar is 0-based)
        checkFormat(null, date2, "mm/dd/yy", "09/07/10");

        // must recognize "MMM"
        checkFormat(null, date2, "MMM/dd/yyyy", "Sep/07/2010");

        // "mmm" is a synonym for "MMMM"
        checkFormat(null, date2, "mmm/dd/yyyy", "Sep/07/2010");

        // must recognize "MMMM"
        checkFormat(null, date2, "MMMM/dd/yyyy", "September/07/2010");

        // "mmmm" is a synonym for "MMMM"
        checkFormat(null, date2, "mmmm/dd/yyyy", "September/07/2010");

        // "mm" means minute, not month, when following "hh"
        checkFormat(null, date2, "hh/mm/ss", "06/05/04");

        // must recognize "Long Date" etc.
        checkFormat(null, date2, "Long Date", "Tuesday, September 07, 2010");
    }

    public void testFrenchLocale() {
        Format.FormatLocale fr = Format.createLocale(Locale.FRANCE);
        assertEquals("#,##0.00 " + I18nTest.Euro, fr.currencyFormat);
        assertEquals(I18nTest.Euro + "", fr.currencySymbol);
        assertEquals("/", fr.dateSeparator);
        assertEquals(
            "[, dimanche, lundi, mardi, mercredi, jeudi, vendredi, samedi]",
            Arrays.toString(fr.daysOfWeekLong));
        assertEquals(
            "[, dim., lun., mar., mer., jeu., ven., sam.]",
            Arrays.toString(fr.daysOfWeekShort));
        assertEquals(
            "[janvier, f" + I18nTest.EA + "vrier, mars, avril, mai, juin,"
            + " juillet, ao" + I18nTest.UC
            + "t, septembre, octobre, novembre, d"
            + I18nTest.EA + "cembre, ]",
            Arrays.toString(fr.monthsLong));
        assertEquals(
            "[janv., f" + I18nTest.EA + "vr., mars, avr., mai, juin,"
            + " juil., ao" + I18nTest.UC + "t, sept., oct., nov., d"
            + I18nTest.EA + "c., ]",
            Arrays.toString(fr.monthsShort));
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
        if (o instanceof BigDecimal) {
            BigDecimal bigDecimal = (BigDecimal) o;
            checkFormat(locale, bigDecimal.doubleValue(), formatString);
            checkFormat(locale, bigDecimal.floatValue(), formatString);
            checkFormat(locale, bigDecimal.longValue(), formatString);
            checkFormat(locale, bigDecimal.intValue(), formatString);
        }
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
        if (o instanceof BigDecimal) {
            BigDecimal bigDecimal = (BigDecimal) o;
            checkFormat(
                locale, bigDecimal.doubleValue(), formatString, expectedResult);

            // Convert value to various data types and make sure there is no
            // error. Do not check the result -- it might be different because
            // of rounding.
            checkFormat(locale, bigDecimal.doubleValue(), formatString);
        }
    }

    public void testCache() {
        StringBuilder buf = new StringBuilder(Format.CacheLimit * 2 + 10);
        buf.append("0.");
        for (int i = 0; i < Format.CacheLimit * 2; ++i) {
            final Format format = Format.get(buf.toString(), null);
            final String s = format.format(i);
            assertEquals(i + ".", s);
            buf.append("#");
        }
    }

    public void testString() {
        // Excel2003
        checkFormat(null, "This Is A Test", ">", "THIS IS A TEST");

        // Excel2003
        checkFormat(null, "This Is A Test", "<", "this is a test");

        // SSAS2005
        checkFormat(null, "hello", "\\f\\i\\x\\e\\d", "hello");

        // SSAS2005
        checkFormat(null, "hello", ">\\f\\i\\x\\e\\d<", "HELLOfixedhello");

        final BigDecimal decimal = new BigDecimal("123.45");
        final int integer = 123;
        final String string = "Foo Bar";

        // ">"
        checkFormat(null, decimal, ">", "123.45");
        checkFormat(null, integer, ">", "123");
        checkFormat(null, string, ">", "FOO BAR"); // SSAS 2005 returns ">"

        // "<"
        checkFormat(null, decimal, "<", "123.45");
        checkFormat(null, integer, "<", "123");
        checkFormat(null, string, "<", "foo bar"); // SSAS 2005 returns "<"

        // "@" (can't figure out how to use this -- SSAS 2005 always ignores)
        checkFormat(null, decimal, "@", "@"); // checked on SSAS 2005
        checkFormat(null, integer, "@", "@"); // checked on SSAS 2005
        checkFormat(null, string, "@", string); // checked on Excel 2003

        // combinations
        checkFormat(null, string, "<@", "foo bar@"); // SSAS 2005 returns "<@"

        // SSAS 2005 returns "<>"; Excel returns "Foo Bar", i.e. unchanged
        checkFormat(null, string, "<>", "foo barFOO BAR");

        checkFormat(null, string, "E", string); // checked on Excel 2003

        // FIXME: SSAS 2005 returns "1.234500E+002"
        checkFormat(null, decimal, "E", "E");

        // SSAS 2005 returns "<E"
        // Excel returns "<123.45"
        checkFormat(null, decimal, "<E", "123.45E");

        // spec and SSAS 2005 disagree
        checkFormat(null, string, "\"fixed\"", string);

//        checkFormat(null, string, "Currency", "Foo Bar");
        checkFormat(null, string, "$#", string);
        checkFormat(null, string, "$#,#.#", string);
    }

    public void testNonNumericValuesUsingNumericFormat() {
        // All of the following have been checked in Excel 2003.

        // string value printed using a numeric format
        checkFormat(null, "foo Bar", "#,#", "foo Bar");
        checkFormat(null, "foo Bar", "#,#;[#]", "foo Bar");
        checkFormat(null, "foo Bar", "#,#;[#];NULL", "foo Bar");
        checkFormat(null, "foo Bar", "#,#;[#];NULL;NIL", "foo Bar");

        // date value printed using a numeric format
        checkFormat(null, date, "#,#;[#];NULL;NIL", "25,324");
        checkFormat(null, date2, "#,#;[#];NULL;NIL", "40,428");

        // date with numeric converted to julian date (days since 1900)
        checkFormat(null, date2, "#", "40428");
        checkFormat(null, date2, "#.##", "40428.25");
        checkFormat(null, date2, "#;[#];NULL", "40428");

        // date value with string format gives long date string
        checkFormat(null, date2, "<", "9/7/10 6:05:04 am");
        checkFormat(null, date2, ">", "9/7/10 6:05:04 AM");

        // numeric value and string format
        checkFormat(null, 123.45E6, "<", "123,450,000"); // Excel gives 12345600
        checkFormat(null, -123.45E6, ">", "-123,450,000");
    }

    public void testFormatThousands() {
        checkFormat(
            null,
            123456.7,
            "######.00",
            "123456.70");
        checkFormat(
            null,
            123456,
            "######",
            "123456");
        checkFormat(
            null,
            123456.7,
            "#,##,###.00",
            "1,23,456.70");
        checkFormat(
            null,
            123456.7,
            "#,##,###",
            "1,23,457");
        checkFormat(
            null,
            9123456.7,
            "#,#.00",
            "9,123,456.70");
        checkFormat(
            null,
            123456.7,
            "#,#",
            "123,457");
        checkFormat(
            null,
            123456789.1,
            "#,#",
            "123,456,789");
        checkFormat(
            null,
            123456.7,
            "##################,#",
            "123,457");
        checkFormat(
            null,
            123456.7,
            "#################,#",
            "123,457");
        checkFormat(
            null,
            123456.7,
            "###,################",
            "123,457");
        checkFormat(
            null,
            0.02,
            "#,###.000",
            ".020");
        checkFormat(
            null,
            0.02,
            "#,##0.000",
            "0.020");
        checkFormat(
            null,
            123456789123l,
            "#,##,#,##,#,##,#,##",
            "1,23,4,56,7,89,1,23");
        checkFormat(
            null,
            123456,
            "#,###;(#,###)",
            "123,456");
        checkFormat(
            null,
            123456,
            "\\$ #,###;(\\$ #,###) ",
            "$ 123,456");
    }

    /**
     * Test case for bug <a href="http://jira.pentaho.com/browse/MONDRIAN-968">
     * MONDRIAN-968</a>, "Thousands formatting does not work. #,###,, <-
     * Multiple Comma not rounding".
     */
    public void testThousandsThousands() {
        final int i = 1234567890;
        if (false) {
        checkFormat(null, i, "#,##0,,", "1,235");
            return;
        }
        checkFormat(null, i, "#,#", "1,234,567,890");

        // Quoth Microsoft:
        // "If one or more commas are specified immediately to the left of the
        // explicit or implicit decimal point, the number to be formatted is
        // divided by 1000 for each comma."
        checkFormat(null, i, "#,##0,,", "1,235");
        checkFormat(null, i, "#,,", "1235");
        checkFormat(null, i, "#,,;(#)", "1235");
        checkFormat(null, i, "#,,,", "1");
        checkFormat(null, i, "#,##0,.0", "1,234,567.9");
        checkFormat(null, i, "#,##0,,.0", "1,234.6");
    }

    /**
     * Tests the international currency symbol parsing
     * in format strings according to different locales.
     */
    public void testCurrency() {
        checkFormat(
            localeDe,
            123456,
            "Currency",
            "123.456,00 \u20AC");
        checkFormat(
            localeDe,
            123456,
            "###,###.00" + Format.intlCurrencySymbol,
            "123.456,00\u20AC");
        checkFormat(
            localeFra,
            123456,
            "###,###.00" + Format.intlCurrencySymbol,
            "123.456,00FF");
        checkFormat(
            localeFra,
            123456,
            "Currency",
            "123.456,00FF");
        // Tests whether the format conversion can fallback to
        // the system default locale to resolve the currency
        // symbol it must use.
        checkFormat(
            Format.createLocale(Locale.JAPANESE),
            123456,
            "Currency",
            "$ 123,456.00");

        // international currency symbol
        checkFormat(
            null,
            new BigDecimal("1.2"),
            "" + Format.intlCurrencySymbol + "#",
            "$1");
    }

    public void testInfinity() {
        String[] strings = {"#", "#.#", "#,###.0"};
        for (String string : strings) {
            checkFormat(
                null,
                Double.POSITIVE_INFINITY,
                string,
                "Infinity");
            checkFormat(
                null,
                Double.NEGATIVE_INFINITY,
                string,
                "-Infinity");
        }
    }

    // PDI-16761
    public void testBigDecimalJavaFormat() {
        BigDecimal bd = new BigDecimal("123456789123456789123456789");
        Format.BasicFormat format = new Format.JavaFormat(Locale.FRENCH);
        StringBuilder result = new StringBuilder();
        format.format(bd, result);
        // It should run without losing precision
        Assert.assertEquals("123 456 789 123 456 789 123 456 789", result.toString());
    }

    /**
     * Test case for bug <a href="http://jira.pentaho.com/browse/MONDRIAN-2613">
     * MONDRIAN-2613</a>,
     * "ArrayIndexOutOfBoundsException in mondrian.util.Format.formatFd2
     * formatting a BigDecimal".
     */
    public void testBigDecimalWithSpecificCustomFormat() {
      //the format string used in the jira case
      final String format = "0000000000000";
      //test data from the jira case
      checkFormat(null, new BigDecimal("109146240.292"), format, "0000109146240");
      checkFormat(null, new BigDecimal("0.123"), format, "0000000000000");
      //additional data
      checkFormat(null, new BigDecimal("1.1"), format, "0000000000001");
      checkFormat(null, new BigDecimal("-1.1"), format, "-0000000000001");
      checkFormat(null, new BigDecimal("0.1"), format, "0000000000000");
      checkFormat(null, new BigDecimal("-0.1"), format, "0000000000000");
      checkFormat(null, new BigDecimal("100000000.1"), format, "0000100000000");
      checkFormat(null, new BigDecimal("-100000000.1"), format, "-0000100000000");
      checkFormat(null, new BigDecimal("100000001.1"), format, "0000100000001");
      checkFormat(null, new BigDecimal("100000000.5"), format, "0000100000001");
      }
}

// End FormatTest.java
