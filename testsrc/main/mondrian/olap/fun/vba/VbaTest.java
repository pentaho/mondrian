/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2007-2014 Pentaho
// All Rights Reserved.
*/
package mondrian.olap.fun.vba;

import mondrian.olap.InvalidArgumentException;
import mondrian.util.Bug;

import junit.framework.TestCase;

import java.text.*;
import java.util.*;

/**
 * Unit tests for implementations of Visual Basic for Applications (VBA)
 * functions.
 *
 * <p>Every function defined in {@link Vba} must have a test here. In addition,
 * there should be MDX tests (usually in
 * {@link mondrian.olap.fun.FunctionTest}) if handling of argument types,
 * result types, operator overloading, exception handling or null handling
 * are non-trivial.
 *
 * @author jhyde
 * @since Dec 31, 2007
 */
public class VbaTest extends TestCase {
    private static final double SMALL = 1e-10d;
    private static final Date SAMPLE_DATE = sampleDate();

    private static final String timeZoneName =
        TimeZone.getDefault().getDisplayName();
    private static final boolean isPST =
        timeZoneName.equals("America/Los_Angeles")
        || timeZoneName.equals("Pacific Standard Time");

    // Conversion functions

    public void testCBool() {
        assertEquals(true, Vba.cBool(Boolean.TRUE)); // not quite to spec
        assertEquals(false, Vba.cBool(Boolean.FALSE)); // not quite to spec
        assertEquals(true, Vba.cBool(1.5));
        assertEquals(true, Vba.cBool("1.5"));
        assertEquals(false, Vba.cBool("0.00"));
        try {
            Object o = Vba.cBool("a");
            fail("expected error, got " + o);
        } catch (RuntimeException e) {
            assertMessage(e, "NumberFormatException");
        }
        // Per the spec, the string "true" is no different from any other
        try {
            Object o = Vba.cBool("true");
            fail("expected error, got " + o);
        } catch (RuntimeException e) {
            assertMessage(e, "NumberFormatException");
        }
    }

    private void assertMessage(RuntimeException e, final String expected) {
        final String message = e.getClass().getName() + ": " + e.getMessage();
        assertTrue(
            "expected message to contain '" + expected + "', got '"
            + message + "'",
            message.indexOf(expected) >= 0);
    }

    public void testCInt() {
        assertEquals(1, Vba.cInt(1));
        assertEquals(1, Vba.cInt(1.4));
        // CInt rounds to the nearest even number
        assertEquals(2, Vba.cInt(1.5));
        assertEquals(2, Vba.cInt(2.5));
        assertEquals(2, Vba.cInt(1.6));
        assertEquals(-1, Vba.cInt(-1.4));
        assertEquals(-2, Vba.cInt(-1.5));
        assertEquals(-2, Vba.cInt(-1.6));
        assertEquals(Integer.MAX_VALUE, Vba.cInt((double) Integer.MAX_VALUE));
        assertEquals(Integer.MIN_VALUE, Vba.cInt((double) Integer.MIN_VALUE));
        assertEquals(
            Short.MAX_VALUE, Vba.cInt(((float) Short.MAX_VALUE) + .4));
        assertEquals(
            Short.MIN_VALUE, Vba.cInt(((float) Short.MIN_VALUE) + .4));
        try {
            Object o = Vba.cInt("a");
            fail("expected error, got " + o);
        } catch (RuntimeException e) {
            assertMessage(e, "NumberFormatException");
        }
    }

    public void testInt() {
        // if negative, Int() returns the closest number less than or
        // equal to the number.
        assertEquals(1, Vba.int_(1));
        assertEquals(1, Vba.int_(1.4));
        assertEquals(1, Vba.int_(1.5));
        assertEquals(2, Vba.int_(2.5));
        assertEquals(1, Vba.int_(1.6));
        assertEquals(-2, Vba.int_(-2));
        assertEquals(-2, Vba.int_(-1.4));
        assertEquals(-2, Vba.int_(-1.5));
        assertEquals(-2, Vba.int_(-1.6));
        assertEquals(Integer.MAX_VALUE, Vba.int_((double) Integer.MAX_VALUE));
        assertEquals(Integer.MIN_VALUE, Vba.int_((double) Integer.MIN_VALUE));
        try {
            Object o = Vba.int_("a");
            fail("expected error, got " + o);
        } catch (RuntimeException e) {
            assertMessage(e, "Invalid parameter.");
        }
    }

    public void testFix() {
        // if negative, Fix() returns the closest number greater than or
        // equal to the number.
        assertEquals(1, Vba.fix(1));
        assertEquals(1, Vba.fix(1.4));
        assertEquals(1, Vba.fix(1.5));
        assertEquals(2, Vba.fix(2.5));
        assertEquals(1, Vba.fix(1.6));
        assertEquals(-1, Vba.fix(-1));
        assertEquals(-1, Vba.fix(-1.4));
        assertEquals(-1, Vba.fix(-1.5));
        assertEquals(-1, Vba.fix(-1.6));
        assertEquals(Integer.MAX_VALUE, Vba.fix((double) Integer.MAX_VALUE));
        assertEquals(Integer.MIN_VALUE, Vba.fix((double) Integer.MIN_VALUE));
        try {
            Object o = Vba.fix("a");
            fail("expected error, got " + o);
        } catch (RuntimeException e) {
            assertMessage(e, "Invalid parameter.");
        }
    }


    public void testCDbl() {
        assertEquals(1.0, Vba.cDbl(1));
        assertEquals(1.4, Vba.cDbl(1.4));
        // CInt rounds to the nearest even number
        assertEquals(1.5, Vba.cDbl(1.5));
        assertEquals(2.5, Vba.cDbl(2.5));
        assertEquals(1.6, Vba.cDbl(1.6));
        assertEquals(-1.4, Vba.cDbl(-1.4));
        assertEquals(-1.5, Vba.cDbl(-1.5));
        assertEquals(-1.6, Vba.cDbl(-1.6));
        assertEquals(Double.MAX_VALUE, Vba.cDbl(Double.MAX_VALUE));
        assertEquals(Double.MIN_VALUE, Vba.cDbl(Double.MIN_VALUE));
        try {
            Object o = Vba.cDbl("a");
            fail("expected error, got " + o);
        } catch (RuntimeException e) {
            assertMessage(e, "NumberFormatException");
        }
    }

    public void testHex() {
        assertEquals("0", Vba.hex(0));
        assertEquals("1", Vba.hex(1));
        assertEquals("A", Vba.hex(10));
        assertEquals("64", Vba.hex(100));
        assertEquals("FFFFFFFF", Vba.hex(-1));
        assertEquals("FFFFFFF6", Vba.hex(-10));
        assertEquals("FFFFFF9C", Vba.hex(-100));
        try {
            Object o = Vba.hex("a");
            fail("expected error, got " + o);
        } catch (RuntimeException e) {
            assertMessage(e, "Invalid parameter.");
        }
    }

    public void testOct() {
        assertEquals("0", Vba.oct(0));
        assertEquals("1", Vba.oct(1));
        assertEquals("12", Vba.oct(10));
        assertEquals("144", Vba.oct(100));
        assertEquals("37777777777", Vba.oct(-1));
        assertEquals("37777777766", Vba.oct(-10));
        assertEquals("37777777634", Vba.oct(-100));
        try {
            Object o = Vba.oct("a");
            fail("expected error, got " + o);
        } catch (RuntimeException e) {
            assertMessage(e, "Invalid parameter.");
        }
    }

    public void testStr() {
        assertEquals(" 0", Vba.str(0));
        assertEquals(" 1", Vba.str(1));
        assertEquals(" 10", Vba.str(10));
        assertEquals(" 100", Vba.str(100));
        assertEquals("-1", Vba.str(-1));
        assertEquals("-10", Vba.str(-10));
        assertEquals("-100", Vba.str(-100));
        assertEquals("-10.123", Vba.str(-10.123));
        assertEquals(" 10.123", Vba.str(10.123));
        try {
            Object o = Vba.oct("a");
            fail("expected error, got " + o);
        } catch (RuntimeException e) {
            assertMessage(e, "Invalid parameter.");
        }
    }

    public void testVal() {
        assertEquals(-1615198.0, Vba.val(" -  1615 198th Street N.E."));
        assertEquals(1615198.0, Vba.val(" 1615 198th Street N.E."));
        assertEquals(1615.198, Vba.val(" 1615 . 198th Street N.E."));
        assertEquals(1615.19, Vba.val(" 1615 . 19 . 8th Street N.E."));
        assertEquals((double)0xffff, Vba.val("&HFFFF"));
        assertEquals(668.0, Vba.val("&O1234"));
    }

    public void testCDate() throws ParseException {
        Date date = new Date();
        assertEquals(date, Vba.cDate(date));
        assertNull(Vba.cDate(null));
        // CInt rounds to the nearest even number
        try {
            assertEquals(
                DateFormat.getDateInstance().parse("Jan 12, 1952"),
                Vba.cDate("Jan 12, 1952"));
            assertEquals(
                DateFormat.getDateInstance().parse("October 19, 1962"),
                Vba.cDate("October 19, 1962"));
            assertEquals(
                DateFormat.getTimeInstance().parse("4:35:47 PM"),
                Vba.cDate("4:35:47 PM"));
            assertEquals(
                DateFormat.getDateTimeInstance().parse(
                    "October 19, 1962 4:35:47 PM"),
                Vba.cDate("October 19, 1962 4:35:47 PM"));
        } catch (ParseException e) {
            e.printStackTrace();
            fail();
        }

        try {
            Vba.cDate("Jan, 1952");
            fail();
        } catch (InvalidArgumentException e) {
            assertTrue(e.getMessage().indexOf("Jan, 1952") >= 0);
        }
    }

    public void testIsDate() throws ParseException {
        // CInt rounds to the nearest even number
        assertFalse(Vba.isDate(null));
        assertTrue(Vba.isDate(new Date()));
        assertTrue(Vba.isDate("Jan 12, 1952"));
        assertTrue(Vba.isDate("October 19, 1962"));
        assertTrue(Vba.isDate("4:35:47 PM"));
        assertTrue(Vba.isDate("October 19, 1962 4:35:47 PM"));
        assertFalse(Vba.isDate("Jan, 1952"));
    }

    // DateTime

    public void testDateAdd() {
        assertEquals("2008/04/24 19:10:45", SAMPLE_DATE);

        // 2008-02-01 0:00:00
        Calendar calendar = Calendar.getInstance();

        calendar.set(2007, 1 /* 0-based! */, 1, 0, 0, 0);
        final Date feb2007 = calendar.getTime();
        assertEquals("2007/02/01 00:00:00", feb2007);

        assertEquals(
            "2008/04/24 19:10:45", Vba.dateAdd("yyyy", 0, SAMPLE_DATE));
        assertEquals(
            "2009/04/24 19:10:45", Vba.dateAdd("yyyy", 1, SAMPLE_DATE));
        assertEquals(
            "2006/04/24 19:10:45", Vba.dateAdd("yyyy", -2, SAMPLE_DATE));
        // partial years interpolate
        final Date sampleDatePlusTwoPointFiveYears =
            Vba.dateAdd("yyyy", 2.5, SAMPLE_DATE);
        if (isPST) {
            // Only run test in PST, because test would produce different
            // results if start and end are not both in daylight savings time.
            final SimpleDateFormat dateFormat =
                new SimpleDateFormat("yyyy/MM/dd HH:mm:ss", Locale.US);
            final String dateString =
                dateFormat.format(
                    sampleDatePlusTwoPointFiveYears);
            // We allow "2010/10/24 07:10:45" for computers that have an out of
            // date timezone database. 2010/10/24 is in daylight savings time,
            // but was not according to the old rules.
            assertTrue(
                "Got " + dateString,
                dateString.equals("2010/10/24 06:40:45")
                    || dateString.equals("2010/10/24 07:10:45"));
        }
        assertEquals("2009/01/24 19:10:45", Vba.dateAdd("q", 3, SAMPLE_DATE));

        // partial months are interesting!
        assertEquals("2008/06/24 19:10:45", Vba.dateAdd("m", 2, SAMPLE_DATE));
        assertEquals("2007/01/01 00:00:00", Vba.dateAdd("m", -1, feb2007));
        assertEquals("2007/03/01 00:00:00", Vba.dateAdd("m", 1, feb2007));
        assertEquals("2007/02/08 00:00:00", Vba.dateAdd("m", .25, feb2007));
        // feb 2008 is a leap month, so a quarter month is 7.25 days
        assertEquals("2008/02/08 06:00:00", Vba.dateAdd("m", 12.25, feb2007));

        assertEquals("2008/05/01 19:10:45", Vba.dateAdd("y", 7, SAMPLE_DATE));
        assertEquals(
            "2008/05/02 01:10:45", Vba.dateAdd("y", 7.25, SAMPLE_DATE));
        assertEquals("2008/04/24 23:10:45", Vba.dateAdd("h", 4, SAMPLE_DATE));
        assertEquals("2008/04/24 20:00:45", Vba.dateAdd("n", 50, SAMPLE_DATE));
        assertEquals("2008/04/24 19:10:36", Vba.dateAdd("s", -9, SAMPLE_DATE));
    }

    public void testDateDiff() {
        // TODO:
    }

    public void testDatePart2() {
        assertEquals(2008, Vba.datePart("yyyy", SAMPLE_DATE));
        assertEquals(2, Vba.datePart("q", SAMPLE_DATE)); // 2nd quarter
        assertEquals(4, Vba.datePart("m", SAMPLE_DATE));
        assertEquals(5, Vba.datePart("w", SAMPLE_DATE)); // thursday
        assertEquals(17, Vba.datePart("ww", SAMPLE_DATE));
        assertEquals(115, Vba.datePart("y", SAMPLE_DATE));
        assertEquals(19, Vba.datePart("h", SAMPLE_DATE));
        assertEquals(10, Vba.datePart("n", SAMPLE_DATE));
        assertEquals(45, Vba.datePart("s", SAMPLE_DATE));
    }

    public void testDatePart3() {
        assertEquals(5, Vba.datePart("w", SAMPLE_DATE, Calendar.SUNDAY));
        assertEquals(4, Vba.datePart("w", SAMPLE_DATE, Calendar.MONDAY));
        assertEquals(17, Vba.datePart("ww", SAMPLE_DATE, Calendar.SUNDAY));
        assertEquals(18, Vba.datePart("ww", SAMPLE_DATE, Calendar.WEDNESDAY));
        assertEquals(18, Vba.datePart("ww", SAMPLE_DATE, Calendar.THURSDAY));
        assertEquals(17, Vba.datePart("ww", SAMPLE_DATE, Calendar.FRIDAY));
    }

    public void testDatePart4() {
        // 2008 starts on a Tuesday
        // 2008-04-29 is a Thursday
        // That puts it in week 17 by most ways of computing weeks
        assertEquals(17, Vba.datePart("ww", SAMPLE_DATE, Calendar.SUNDAY, 0));
        assertEquals(17, Vba.datePart("ww", SAMPLE_DATE, Calendar.SUNDAY, 1));
        assertEquals(17, Vba.datePart("ww", SAMPLE_DATE, Calendar.SUNDAY, 2));
        assertEquals(16, Vba.datePart("ww", SAMPLE_DATE, Calendar.SUNDAY, 3));
        assertEquals(17, Vba.datePart("ww", SAMPLE_DATE, Calendar.MONDAY, 0));
        assertEquals(17, Vba.datePart("ww", SAMPLE_DATE, Calendar.MONDAY, 1));
        assertEquals(17, Vba.datePart("ww", SAMPLE_DATE, Calendar.MONDAY, 2));
        assertEquals(16, Vba.datePart("ww", SAMPLE_DATE, Calendar.MONDAY, 3));
        assertEquals(17, Vba.datePart("ww", SAMPLE_DATE, Calendar.TUESDAY, 0));
        assertEquals(17, Vba.datePart("ww", SAMPLE_DATE, Calendar.TUESDAY, 1));
        assertEquals(17, Vba.datePart("ww", SAMPLE_DATE, Calendar.TUESDAY, 2));
        assertEquals(17, Vba.datePart("ww", SAMPLE_DATE, Calendar.TUESDAY, 3));
        assertEquals(
            18, Vba.datePart("ww", SAMPLE_DATE, Calendar.WEDNESDAY, 0));
        assertEquals(
            18, Vba.datePart("ww", SAMPLE_DATE, Calendar.WEDNESDAY, 1));
        assertEquals(
            17, Vba.datePart("ww", SAMPLE_DATE, Calendar.WEDNESDAY, 2));
        assertEquals(
            17, Vba.datePart("ww", SAMPLE_DATE, Calendar.WEDNESDAY, 3));
        assertEquals(18, Vba.datePart("ww", SAMPLE_DATE, Calendar.THURSDAY, 0));
        assertEquals(18, Vba.datePart("ww", SAMPLE_DATE, Calendar.THURSDAY, 1));
        assertEquals(17, Vba.datePart("ww", SAMPLE_DATE, Calendar.THURSDAY, 2));
        assertEquals(17, Vba.datePart("ww", SAMPLE_DATE, Calendar.THURSDAY, 3));
        assertEquals(17, Vba.datePart("ww", SAMPLE_DATE, Calendar.FRIDAY, 0));
        assertEquals(17, Vba.datePart("ww", SAMPLE_DATE, Calendar.FRIDAY, 1));
        assertEquals(16, Vba.datePart("ww", SAMPLE_DATE, Calendar.FRIDAY, 2));
        assertEquals(16, Vba.datePart("ww", SAMPLE_DATE, Calendar.FRIDAY, 3));
        assertEquals(17, Vba.datePart("ww", SAMPLE_DATE, Calendar.SATURDAY, 0));
        assertEquals(17, Vba.datePart("ww", SAMPLE_DATE, Calendar.SATURDAY, 1));
        assertEquals(17, Vba.datePart("ww", SAMPLE_DATE, Calendar.SATURDAY, 2));
        assertEquals(16, Vba.datePart("ww", SAMPLE_DATE, Calendar.SATURDAY, 3));
        try {
            int i = Vba.datePart("ww", SAMPLE_DATE, Calendar.SUNDAY, 4);
            fail("expected error, got " + i);
        } catch (RuntimeException e) {
            assertMessage(e, "ArrayIndexOutOfBoundsException");
        }
    }

    public void testDate() {
        final Date date = Vba.date();
        assertNotNull(date);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        assertEquals(0, calendar.get(Calendar.HOUR_OF_DAY));
        assertEquals(0, calendar.get(Calendar.MILLISECOND));
    }

    public void testDateSerial() {
        final Date date = Vba.dateSerial(2008, 2, 1);
        assertEquals("2008/02/01 00:00:00", date);
    }

    private void assertEquals(
        String expected,
        Date date)
    {
        final SimpleDateFormat dateFormat =
            new SimpleDateFormat("yyyy/MM/dd HH:mm:ss", Locale.US);
        final String dateString = dateFormat.format(date);
        assertEquals(expected, dateString);
    }

    public void testFormatDateTime() {
        try {
            Date date = DateFormat.getDateTimeInstance().parse(
                "October 19, 1962 4:35:47 PM");
            assertEquals("Oct 19, 1962 4:35:47 PM", Vba.formatDateTime(date));
            assertEquals(
                "Oct 19, 1962 4:35:47 PM", Vba.formatDateTime(date, 0));
            assertEquals("October 19, 1962", Vba.formatDateTime(date, 1));
            assertEquals("10/19/62", Vba.formatDateTime(date, 2));
            String datestr = Vba.formatDateTime(date, 3);
            assertNotNull(datestr);
            // skip the timezone so this test runs everywhere
            // in EST, this string is "4:35:47 PM EST"
            assertTrue(datestr.startsWith("4:35:47 PM"));
            assertEquals("4:35 PM", Vba.formatDateTime(date, 4));
        } catch (ParseException e) {
            e.printStackTrace();
            fail();
        }
    }

    public void testDateValue() {
        Date date = new Date();
        final Date date1 = Vba.dateValue(date);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date1);
        assertEquals(0, calendar.get(Calendar.HOUR_OF_DAY));
        assertEquals(0, calendar.get(Calendar.MINUTE));
        assertEquals(0, calendar.get(Calendar.SECOND));
        assertEquals(0, calendar.get(Calendar.MILLISECOND));
    }

    private static Date sampleDate() {
        Calendar calendar = Calendar.getInstance();
        // Thursday 2008-04-24 7:10:45pm
        // Chose a Thursday because 2008 starts on a Tuesday - it makes weeks
        // interesting.
        calendar.set(2008, 3 /* 0-based! */, 24, 19, 10, 45);
        return calendar.getTime();
    }

    public void testDay() {
        assertEquals(24, Vba.day(SAMPLE_DATE));
    }

    public void testHour() {
        assertEquals(19, Vba.hour(SAMPLE_DATE));
    }

    public void testMinute() {
        assertEquals(10, Vba.minute(SAMPLE_DATE));
    }

    public void testMonth() {
        assertEquals(4, Vba.month(SAMPLE_DATE));
    }

    public void testNow() {
        final Date date = Vba.now();
        assertNotNull(date);
    }

    public void testSecond() {
        assertEquals(45, Vba.second(SAMPLE_DATE));
    }

    public void testTimeSerial() {
        final Date date = Vba.timeSerial(17, 42, 10);
        assertEquals("1970/01/01 17:42:10", date);
    }

    public void testTimeValue() {
        assertEquals("1970/01/01 19:10:45", Vba.timeValue(SAMPLE_DATE));
    }

    public void testTimer() {
        final float v = Vba.timer();
        assertTrue(v >= 0);
        assertTrue(v < 24 * 60 * 60);
    }

    public void testWeekday1() {
        if (Calendar.getInstance().getFirstDayOfWeek() == Calendar.SUNDAY) {
            assertEquals(Calendar.THURSDAY, Vba.weekday(SAMPLE_DATE));
        }
    }

    public void testWeekday2() {
        // 2008/4/24 falls on a Thursday.

        // If Sunday is the first day of the week, Thursday is day 5.
        assertEquals(5, Vba.weekday(SAMPLE_DATE, Calendar.SUNDAY));

        // If Monday is the first day of the week, then 2008/4/24 falls on the
        // 4th day of the week
        assertEquals(4, Vba.weekday(SAMPLE_DATE, Calendar.MONDAY));

        assertEquals(3, Vba.weekday(SAMPLE_DATE, Calendar.TUESDAY));
        assertEquals(2, Vba.weekday(SAMPLE_DATE, Calendar.WEDNESDAY));
        assertEquals(1, Vba.weekday(SAMPLE_DATE, Calendar.THURSDAY));
        assertEquals(7, Vba.weekday(SAMPLE_DATE, Calendar.FRIDAY));
        assertEquals(6, Vba.weekday(SAMPLE_DATE, Calendar.SATURDAY));
    }

    public void testYear() {
        assertEquals(2008, Vba.year(SAMPLE_DATE));
    }

    public void testFormatNumber() {
        assertEquals("1", Vba.formatNumber(1.0));
        assertEquals("1.0", Vba.formatNumber(1.0, 1));

        assertEquals("0.1", Vba.formatNumber(0.1, -1, -1));
        assertEquals(".1", Vba.formatNumber(0.1, -1, 0));
        assertEquals("0.1", Vba.formatNumber(0.1, -1, 1));

        assertEquals("-1",  Vba.formatNumber(-1, -1, 1, -1));
        assertEquals("-1",  Vba.formatNumber(-1, -1, 1,  0));
        assertEquals("(1)", Vba.formatNumber(-1, -1, 1,  1));

        assertEquals("1", Vba.formatNumber(1, -1, 1, -1));
        assertEquals("1", Vba.formatNumber(1, -1, 1,  0));
        assertEquals("1", Vba.formatNumber(1, -1, 1,  1));

        assertEquals("1,000",   Vba.formatNumber(1000.0, -1, -1, -1, -1));
        assertEquals("1000.0",  Vba.formatNumber(1000.0,  1, -1, -1,  0));
        assertEquals("1,000.0", Vba.formatNumber(1000.0,  1, -1, -1,  1));
    }

    public void testFormatPercent() {
        assertEquals("100%", Vba.formatPercent(1.0));
        assertEquals("100.0%", Vba.formatPercent(1.0, 1));

        assertEquals("0.1%", Vba.formatPercent(0.001,1, -1));
        assertEquals(".1%", Vba.formatPercent(0.001, 1, 0));
        assertEquals("0.1%", Vba.formatPercent(0.001, 1, 1));


        assertEquals("11%", Vba.formatPercent(0.111, -1));
        assertEquals("11%", Vba.formatPercent(0.111, 0));
        assertEquals("11.100%", Vba.formatPercent(0.111, 3));

        assertEquals("-100%",  Vba.formatPercent(-1, -1, 1, -1));
        assertEquals("-100%",  Vba.formatPercent(-1, -1, 1,  0));
        assertEquals("(100%)", Vba.formatPercent(-1, -1, 1,  1));

        assertEquals("100%", Vba.formatPercent(1, -1, 1, -1));
        assertEquals("100%", Vba.formatPercent(1, -1, 1,  0));
        assertEquals("100%", Vba.formatPercent(1, -1, 1,  1));

        assertEquals("100,000%",   Vba.formatPercent(1000.0, -1, -1, -1, -1));
        assertEquals("100000.0%",  Vba.formatPercent(1000.0,  1, -1, -1,  0));
        assertEquals("100,000.0%", Vba.formatPercent(1000.0,  1, -1, -1,  1));
    }

    public void testFormatCurrency() {
        assertEquals("$1.00", Vba.formatCurrency(1.0));
        assertEquals("$0.00", Vba.formatCurrency(0.0));
        assertEquals("$1.0", Vba.formatCurrency(1.0, 1));
        assertEquals("$1", Vba.formatCurrency(1.0, 0));
        assertEquals("$.10", Vba.formatCurrency(0.10, -1, 0));
        assertEquals("$0.10", Vba.formatCurrency(0.10, -1, -1));
        // todo: still need to implement parens customization
        // assertEquals("-$0.10", Vba.formatCurrency(-0.10, -1, -1, -1));
        assertEquals("($0.10)", Vba.formatCurrency(-0.10, -1, -1, 0));

        assertEquals("$1,000.00", Vba.formatCurrency(1000.0, -1, -1, 0, 0));
        assertEquals("$1000.00", Vba.formatCurrency(1000.0, -1, -1, 0, -1));
    }

    public void testTypeName() {
        assertEquals("Double", Vba.typeName(1.0));
        assertEquals("Integer", Vba.typeName(1));
        assertEquals("Float", Vba.typeName(1.0f));
        assertEquals("Byte", Vba.typeName((byte)1));
        assertEquals("NULL", Vba.typeName(null));
        assertEquals("String", Vba.typeName(""));
        assertEquals("Date", Vba.typeName(new Date()));
    }

    // Financial

    public void testFv() {
        double f, r, y, p, x;
        int n;
        boolean t;

        r = 0;
        n = 3;
        y = 2;
        p = 7;
        t = true;
        f = Vba.fV(r, n, y, p, t);
        x = -13;
        assertEquals(x, f);

        r = 1;
        n = 10;
        y = 100;
        p = 10000;
        t = false;
        f = Vba.fV(r, n, y, p, t);
        x = -10342300;
        assertEquals(x, f);

        r = 1;
        n = 10;
        y = 100;
        p = 10000;
        t = true;
        f = Vba.fV(r, n, y, p, t);
        x = -10444600;
        assertEquals(x, f);

        r = 2;
        n = 12;
        y = 120;
        p = 12000;
        t = false;
        f = Vba.fV(r, n, y, p, t);
        x = -6409178400d;
        assertEquals(x, f);

        r = 2;
        n = 12;
        y = 120;
        p = 12000;
        t = true;
        f = Vba.fV(r, n, y, p, t);
        x = -6472951200d;
        assertEquals(x, f);

        // cross tests with pv
        r = 2.95;
        n = 13;
        y = 13000;
        p = -4406.78544294496;
        t = false;
        f = Vba.fV(r, n, y, p, t);
        x = 333891.230010986; // as returned by excel
        assertEquals(x, f, 1e-2);

        r = 2.95;
        n = 13;
        y = 13000;
        p = -17406.7852148156;
        t = true;
        f = Vba.fV(r, n, y, p, t);
        x = 333891.230102539; // as returned by excel
        assertEquals(x, f, 1e-2);
    }

    public void testNpv() {
        double r, v[], npv, x;

        r = 1;
        v = new double[] {100, 200, 300, 400};
        npv = Vba.nPV(r, v);
        x = 162.5;
        assertEquals(x, npv);

        r = 2.5;
        v = new double[] {1000, 666.66666, 333.33, 12.2768416};
        npv = Vba.nPV(r, v);
        x = 347.99232604144827;
        assertEquals(x, npv, SMALL);

        r = 12.33333;
        v = new double[] {1000, 0, -900, -7777.5765};
        npv = Vba.nPV(r, v);
        x = 74.3742433377061;
        assertEquals(x, npv, 1e-12);

        r = 0.05;
        v = new double[] {
            200000, 300000.55, 400000, 1000000, 6000000, 7000000, -300000
        };
        npv = Vba.nPV(r, v);
        x = 11342283.4233124;
        assertEquals(x, npv, 1e-8);
    }

    public void testPmt() {
        double f, r, y, p, x;
        int n;
        boolean t;

        r = 0;
        n = 3;
        p = 2;
        f = 7;
        t = true;
        y = Vba.pmt(r, n, p, f, t);
        x = -3;
        assertEquals(x, y);

        // cross check with pv
        r = 1;
        n = 10;
        p = -109.66796875;
        f = 10000;
        t = false;
        y = Vba.pmt(r, n, p, f, t);
        x = 100;
        assertEquals(x, y);

        r = 1;
        n = 10;
        p = -209.5703125;
        f = 10000;
        t = true;
        y = Vba.pmt(r, n, p, f, t);
        x = 100;
        assertEquals(x, y);

        // cross check with fv
        r = 2;
        n = 12;
        f = -6409178400d;
        p = 12000;
        t = false;
        y = Vba.pmt(r, n, p, f, t);
        x = 120;
        assertEquals(x, y);

        r = 2;
        n = 12;
        f = -6472951200d;
        p = 12000;
        t = true;
        y = Vba.pmt(r, n, p, f, t);
        x = 120;
        assertEquals(x, y);
    }

    public void testPv() {
        double f, r, y, p, x;
        int n;
        boolean t;

        r = 0;
        n = 3;
        y = 2;
        f = 7;
        t = true;
        f = Vba.pV(r, n, y, f, t);
        x = -13;
        assertEquals(x, f);

        r = 1;
        n = 10;
        y = 100;
        f = 10000;
        t = false;
        p = Vba.pV(r, n, y, f, t);
        x = -109.66796875;
        assertEquals(x, p);

        r = 1;
        n = 10;
        y = 100;
        f = 10000;
        t = true;
        p = Vba.pV(r, n, y, f, t);
        x = -209.5703125;
        assertEquals(x, p);

        r = 2.95;
        n = 13;
        y = 13000;
        f = 333891.23;
        t = false;
        p = Vba.pV(r, n, y, f, t);
        x = -4406.78544294496;
        assertEquals(x, p, 1e-10);

        r = 2.95;
        n = 13;
        y = 13000;
        f = 333891.23;
        t = true;
        p = Vba.pV(r, n, y, f, t);
        x = -17406.7852148156;
        assertEquals(x, p, 1e-10);

        // cross tests with fv
        r = 2;
        n = 12;
        y = 120;
        f = -6409178400d;
        t = false;
        p = Vba.pV(r, n, y, f, t);
        x = 12000;
        assertEquals(x, p);

        r = 2;
        n = 12;
        y = 120;
        f = -6472951200d;
        t = true;
        p = Vba.pV(r, n, y, f, t);
        x = 12000;
        assertEquals(x, p);
    }

    public void testDdb() {
        double cost, salvage, life, period, factor, result;
        cost = 100;
        salvage = 0;
        life = 10;
        period = 1;
        factor = 2;
        result = Vba.dDB(cost, salvage, life, period, factor);
        assertEquals(20.0, result);
        result = Vba.dDB(cost, salvage, life, period + 1, factor);
        assertEquals(40.0, result);
        result = Vba.dDB(cost, salvage, life, period + 2, factor);
        assertEquals(60.0, result);
        result = Vba.dDB(cost, salvage, life, period + 3, factor);
        assertEquals(80.0, result);
    }

    public void testRate() {
        double nPer, pmt, PV, fv, guess, result;
        boolean type = false;
        nPer = 12 * 30;
        pmt = -877.57;
        PV = 100000;
        fv = 0;
        guess = 0.10 / 12;
        result = Vba.rate(nPer, pmt, PV, fv, type, guess);

        // compare rate to pV calculation
        double expRate = 0.0083333;
        double expPV = Vba.pV(expRate, 12 * 30, -877.57, 0, false);
        result = Vba.rate(12 * 30, -877.57, expPV, 0, false, 0.10 / 12);
        assertTrue(Math.abs(expRate - result) < 0.0000001);

        // compare rate to fV calculation
        double expFV = Vba.fV(expRate, 12, -100, 0, false);
        result = Vba.rate(12, -100, 0, expFV, false, 0.10 / 12);
        assertTrue(Math.abs(expRate - result) < 0.0000001);
    }

    public void testIRR() {
        double vals[] = {-1000, 50, 50, 50, 50, 50, 1050};
        assertTrue(Math.abs(0.05 - Vba.IRR(vals, 0.1)) < 0.0000001);

        vals = new double[] {-1000, 200, 200, 200, 200, 200, 200};
        assertTrue(Math.abs(0.05471796 - Vba.IRR(vals, 0.1)) < 0.0000001);

        // what happens if the numbers are inversed? this may not be
        // accurate

        vals = new double[] {1000, -200, -200, -200, -200, -200, -200};
        assertTrue(Math.abs(0.05471796 - Vba.IRR(vals, 0.1)) < 0.0000001);
    }

    public void testMIRR() {
        double vals[] = {-1000, 50, 50, 50, 50, 50, 1050};
        assertTrue(Math.abs(0.05 - Vba.MIRR(vals, 0.05, 0.05)) < 0.0000001);

        vals = new double[] {-1000, 200, 200, 200, 200, 200, 200};
        assertTrue(
            Math.abs(0.05263266 - Vba.MIRR(vals, 0.05, 0.05)) < 0.0000001);

        vals = new double[] {-1000, 200, 200, 200, 200, 200, 200};
        assertTrue(
            Math.abs(0.04490701 - Vba.MIRR(vals, 0.06, 0.04)) < 0.0000001);
    }

    public void testIPmt() {
        assertEquals(-10000.0, Vba.iPmt(0.10, 1, 30, 100000, 0, false));
        assertEquals(
            -2185.473324557822, Vba.iPmt(0.10, 15, 30, 100000, 0, false));
        assertEquals(
            -60.79248252633988, Vba.iPmt(0.10, 30, 30, 100000, 0, false));
    }

    public void testPPmt() {
        assertEquals(
            -607.9248252633897, Vba.pPmt(0.10, 1, 30, 100000, 0, false));
        assertEquals(
            -8422.451500705567, Vba.pPmt(0.10, 15, 30, 100000, 0, false));
        assertEquals(
            -10547.13234273705, Vba.pPmt(0.10, 30, 30, 100000, 0, false));

        // verify that pmt, ipmt, and ppmt add up
        double pmt = Vba.pmt(0.10, 30, 100000, 0, false);
        double ipmt = Vba.iPmt(0.10, 15, 30, 100000, 0, false);
        double ppmt = Vba.pPmt(0.10, 15, 30, 100000, 0, false);
        assertTrue(Math.abs(pmt - (ipmt + ppmt)) < 0.0000001);
    }

    public void testSLN() {
        assertEquals(18.0, Vba.sLN(100, 10, 5));
        assertEquals(Double.POSITIVE_INFINITY, Vba.sLN(100, 10, 0));
    }

    public void testSYD() {
        assertEquals(300.0, Vba.sYD(1000, 100, 5, 5));
        assertEquals(240.0, Vba.sYD(1000, 100, 4, 5));
        assertEquals(180.0, Vba.sYD(1000, 100, 3, 5));
        assertEquals(120.0, Vba.sYD(1000, 100, 2, 5));
        assertEquals(60.0, Vba.sYD(1000, 100, 1, 5));
    }

    public void testInStr() {
        assertEquals(
            1,
            Vba.inStr("the quick brown fox jumps over the lazy dog", "the"));
        assertEquals(
            32,
            Vba.inStr(
                16, "the quick brown fox jumps over the lazy dog", "the"));
        assertEquals(
            0,
            Vba.inStr(
                16, "the quick brown fox jumps over the lazy dog", "cat"));
        assertEquals(
            0,
            Vba.inStr(1, "the quick brown fox jumps over the lazy dog", "cat"));
        assertEquals(
            0,
            Vba.inStr(1, "", "cat"));
        assertEquals(
            0,
            Vba.inStr(100, "short string", "str"));
        try {
            Vba.inStr(0, "the quick brown fox jumps over the lazy dog", "the");
            fail();
        } catch (InvalidArgumentException e) {
            assertTrue(e.getMessage().indexOf("-1 or a location") >= 0);
        }
    }

    public void testInStrRev() {
        assertEquals(
            32,
            Vba.inStrRev("the quick brown fox jumps over the lazy dog", "the"));
        assertEquals(
            1,
            Vba.inStrRev(
                "the quick brown fox jumps over the lazy dog", "the", 16));
        try {
            Vba.inStrRev(
                "the quick brown fox jumps over the lazy dog", "the", 0);
            fail();
        } catch (InvalidArgumentException e) {
            assertTrue(e.getMessage().indexOf("-1 or a location") >= 0);
        }
    }

    public void testStrComp() {
        assertEquals(-1, Vba.strComp("a", "b", 0));
        assertEquals(0, Vba.strComp("a", "a", 0));
        assertEquals(1, Vba.strComp("b", "a", 0));
    }

    public void testNper() {
        double f, r, y, p, x, n;
        boolean t;

        r = 0;
        y = 7;
        p = 2;
        f = 3;
        t = false;
        n = Vba.nPer(r, y, p, f, t);
        // can you believe it? excel returns nper as a fraction!??
        x = -0.71428571429;
        assertEquals(x, n, 1e-10);

        // cross check with pv
        r = 1;
        y = 100;
        p = -109.66796875;
        f = 10000;
        t = false;
        n = Vba.nPer(r, y, p, f, t);
        x = 10;
        assertEquals(x, n, 1e-12);

        r = 1;
        y = 100;
        p = -209.5703125;
        f = 10000;
        t = true;
        n = Vba.nPer(r, y, p, f, t);
        x = 10;
        assertEquals(x, n, 1e-14);

        // cross check with fv
        r = 2;
        y = 120;
        f = -6409178400d;
        p = 12000;
        t = false;
        n = Vba.nPer(r, y, p, f, t);
        x = 12;
        assertEquals(x, n, SMALL);

        r = 2;
        y = 120;
        f = -6472951200d;
        p = 12000;
        t = true;
        n = Vba.nPer(r, y, p, f, t);
        x = 12;
        assertEquals(x, n, SMALL);
    }

    // String functions

    public void testAsc() {
        assertEquals(0x61, Vba.asc("abc"));
        assertEquals(0x1234, Vba.asc("\u1234abc"));
        try {
            Object o = Vba.asc("");
            fail("expected error, got " + o);
        } catch (RuntimeException e) {
            assertMessage(e, "StringIndexOutOfBoundsException");
        }
    }

    public void testAscB() {
        assertEquals(0x61, Vba.ascB("abc"));
        assertEquals(0x34, Vba.ascB("\u1234abc")); // not sure about this
        try {
            Object o = Vba.ascB("");
            fail("expected error, got " + o);
        } catch (RuntimeException e) {
            assertMessage(e, "StringIndexOutOfBoundsException");
        }
    }

    public void testAscW() {
        // ascW behaves identically to asc
        assertEquals(0x61, Vba.ascW("abc"));
        assertEquals(0x1234, Vba.ascW("\u1234abc"));
        try {
            Object o = Vba.ascW("");
            fail("expected error, got " + o);
        } catch (RuntimeException e) {
            assertMessage(e, "StringIndexOutOfBoundsException");
        }
    }

    public void testChr() {
        assertEquals("a", Vba.chr(0x61));
        assertEquals("\u1234", Vba.chr(0x1234));
    }

    public void testChrB() {
        assertEquals("a", Vba.chrB(0x61));
        assertEquals("\u0034", Vba.chrB(0x1234));
    }

    public void testChrW() {
        assertEquals("a", Vba.chrW(0x61));
        assertEquals("\u1234", Vba.chrW(0x1234));
    }

    public void testLCase() {
        assertEquals("", Vba.lCase(""));
        assertEquals("abc", Vba.lCase("AbC"));
    }

    // NOTE: BuiltinFunTable already implements Left; todo: use this
    public void testLeft() {
        assertEquals("abc", Vba.left("abcxyz", 3));
        // length=0 is OK
        assertEquals("", Vba.left("abcxyz", 0));
        // Spec says: "If greater than or equal to the number of characters in
        // string, the entire string is returned."
        assertEquals("abcxyz", Vba.left("abcxyz", 8));
        assertEquals("", Vba.left("", 3));

        // Length<0 is illegal.
        // Note: SSAS 2005 allows length<0, giving the same result as length=0.
        // We favor the VBA spec over SSAS 2005.
        if (Bug.Ssas2005Compatible) {
            assertEquals("", Vba.left("xyz", -2));
        } else {
            try {
                String s = Vba.left("xyz", -2);
                fail("expected error, got " + s);
            } catch (RuntimeException e) {
                assertMessage(e, "StringIndexOutOfBoundsException");
            }
        }

        assertEquals("Hello", Vba.left("Hello World!", 5));
    }

    public void testLTrim() {
        assertEquals("", Vba.lTrim(""));
        assertEquals("", Vba.lTrim("  "));
        assertEquals("abc  \r", Vba.lTrim(" \n\tabc  \r"));
    }

    public void testMid() {
        String testString = "Mid Function Demo";
        assertEquals("Mid", Vba.mid(testString, 1, 3));
        assertEquals("Demo", Vba.mid(testString, 14, 4));
        // It's OK if start+length = string.length
        assertEquals("Demo", Vba.mid(testString, 14, 5));
        // It's OK if start+length > string.length
        assertEquals("Demo", Vba.mid(testString, 14, 500));
        assertEquals("Function Demo", Vba.mid(testString, 5));
        assertEquals("o", Vba.mid("yahoo", 5, 1));

        // Start=0 illegal
        // Note: SSAS 2005 accepts start<=0, treating it as 1, therefore gives
        // different results. We favor the VBA spec over SSAS 2005.
        if (Bug.Ssas2005Compatible) {
            assertEquals("Mid Function Demo", Vba.mid(testString, 0));
            assertEquals("Mid Function Demo", Vba.mid(testString, -2));
            assertEquals("Mid Function Demo", Vba.mid(testString, -2, 5));
        } else {
            try {
                String s = Vba.mid(testString, 0);
                fail("expected error, got " + s);
            } catch (RuntimeException e) {
                assertMessage(
                    e,
                    "Invalid parameter. Start parameter of Mid function must "
                    + "be positive");
            }
            // Start<0 illegal
            try {
                String s = Vba.mid(testString, -2);
                fail("expected error, got " + s);
            } catch (RuntimeException e) {
                assertMessage(
                    e,
                    "Invalid parameter. Start parameter of Mid function must "
                    + "be positive");
            }
            // Start<0 illegal to 3 args version
            try {
                String s = Vba.mid(testString, -2, 5);
                fail("expected error, got " + s);
            } catch (RuntimeException e) {
                assertMessage(
                    e,
                    "Invalid parameter. Start parameter of Mid function must "
                    + "be positive");
            }
        }

        // Length=0 OK
        assertEquals("", Vba.mid(testString, 14, 0));

        // Length<0 illegal
        // Note: SSAS 2005 accepts length<0, treating it as 0, therefore gives
        // different results. We favor the VBA spec over SSAS 2005.
        if (Bug.Ssas2005Compatible) {
            assertEquals("", Vba.mid(testString, 14, -1));
        } else {
            try {
                String s = Vba.mid(testString, 14, -1);
                fail("expected error, got " + s);
            } catch (RuntimeException e) {
                assertMessage(
                    e,
                    "Invalid parameter. Length parameter of Mid function must "
                    + "be non-negative");
            }
        }
    }

    public void testMonthName() {
        assertEquals("January", Vba.monthName(1, false));
        assertEquals("Jan", Vba.monthName(1, true));
        assertEquals("Dec", Vba.monthName(12, true));
        try {
            String s = Vba.monthName(0, true);
            fail("expected error, got " + s);
        } catch (RuntimeException e) {
            assertMessage(e, "ArrayIndexOutOfBoundsException");
        }
    }

    public void testReplace3() {
        // replace with longer string
        assertEquals("abczabcz", Vba.replace("xyzxyz", "xy", "abc"));
        // replace with shorter string
        assertEquals("wazwaz", Vba.replace("wxyzwxyz", "xy", "a"));
        // replace with string which contains seek
        assertEquals("wxyz", Vba.replace("xyz", "xy", "wxy"));
        // replace with string which combines with following char to make seek
        assertEquals("wxyzwx", Vba.replace("xyyzxy", "xy", "wx"));
        // replace with empty string
        assertEquals("wxyza", Vba.replace("wxxyyzxya", "xy", ""));
    }

    public void testReplace4() {
        assertEquals("azaz", Vba.replace("xyzxyz", "xy", "a", 1));
        assertEquals("xyzaz", Vba.replace("xyzxyz", "xy", "a", 2));
        assertEquals("xyzxyz", Vba.replace("xyzxyz", "xy", "a", 30));
        // spec doesn't say, but assume starting before start of string is ok
        assertEquals("azaz", Vba.replace("xyzxyz", "xy", "a", 0));
        assertEquals("azaz", Vba.replace("xyzxyz", "xy", "a", -5));
    }

    public void testReplace5() {
        assertEquals("azaz", Vba.replace("xyzxyz", "xy", "a", 1, -1));
        assertEquals("azxyz", Vba.replace("xyzxyz", "xy", "a", 1, 1));
        assertEquals("azaz", Vba.replace("xyzxyz", "xy", "a", 1, 2));
        assertEquals("xyzazxyz", Vba.replace("xyzxyzxyz", "xy", "a", 2, 1));
    }

    public void testReplace6() {
        // compare is currently ignored
        assertEquals("azaz", Vba.replace("xyzxyz", "xy", "a", 1, -1, 1000));
        assertEquals("azxyz", Vba.replace("xyzxyz", "xy", "a", 1, 1, 0));
        assertEquals("azaz", Vba.replace("xyzxyz", "xy", "a", 1, 2, -6));
        assertEquals(
            "xyzazxyz", Vba.replace("xyzxyzxyz", "xy", "a", 2, 1, 11));
    }

    public void testRight() {
        assertEquals("xyz", Vba.right("abcxyz", 3));
        // length=0 is OK
        assertEquals("", Vba.right("abcxyz", 0));
        // Spec says: "If greater than or equal to the number of characters in
        // string, the entire string is returned."
        assertEquals("abcxyz", Vba.right("abcxyz", 8));
        assertEquals("", Vba.right("", 3));

        // The VBA spec says that length<0 is error.
        // Note: SSAS 2005 allows length<0, giving the same result as length=0.
        // We favor the VBA spec over SSAS 2005.
        if (Bug.Ssas2005Compatible) {
            assertEquals("", Vba.right("xyz", -2));
        } else {
            try {
                String s = Vba.right("xyz", -2);
                fail("expected error, got " + s);
            } catch (RuntimeException e) {
                assertMessage(e, "StringIndexOutOfBoundsException");
            }
        }

        assertEquals("World!", Vba.right("Hello World!", 6));
    }

    public void testRTrim() {
        assertEquals("", Vba.rTrim(""));
        assertEquals("", Vba.rTrim("  "));
        assertEquals(" \n\tabc", Vba.rTrim(" \n\tabc"));
        assertEquals(" \n\tabc", Vba.rTrim(" \n\tabc  \r"));
    }

    public void testSpace() {
        assertEquals("   ", Vba.space(3));
        assertEquals("", Vba.space(0));
        try {
            String s = Vba.space(-2);
            fail("expected error, got " + s);
        } catch (RuntimeException e) {
            assertMessage(e, "NegativeArraySizeException");
        }
    }

    public void testString() {
        assertEquals("xxx", Vba.string(3, 'x'));
        assertEquals("", Vba.string(0, 'y'));
        try {
            String s = Vba.string(-2, 'z');
            fail("expected error, got " + s);
        } catch (RuntimeException e) {
            assertMessage(e, "NegativeArraySizeException");
        }
        assertEquals("", Vba.string(100, '\0'));
    }

    public void testStrReverse() {
        // odd length
        assertEquals("cba", Vba.strReverse("abc"));
        // even length
        assertEquals("wxyz", Vba.strReverse("zyxw"));
        // zero length
        assertEquals("", Vba.strReverse(""));
    }

    public void testTrim() {
        assertEquals("", Vba.trim(""));
        assertEquals("", Vba.trim("  "));
        assertEquals("abc", Vba.trim("abc"));
        assertEquals("abc", Vba.trim(" \n\tabc  \r"));
    }

    public void testWeekdayName() {
        // If Sunday (1) is the first day of the week
        // then day 1 is Sunday,
        // then day 2 is Monday,
        // and day 7 is Saturday
        assertEquals("Sunday", Vba.weekdayName(1, false, 1));
        assertEquals("Monday", Vba.weekdayName(2, false, 1));
        assertEquals("Saturday", Vba.weekdayName(7, false, 1));
        assertEquals("Sat", Vba.weekdayName(7, true, 1));

        // If Monday (2) is the first day of the week
        // then day 1 is Monday,
        // and day 7 is Sunday
        assertEquals("Monday", Vba.weekdayName(1, false, 2));
        assertEquals("Sunday", Vba.weekdayName(7, false, 2));

        // Use weekday start from locale. Test for the 2 most common.
        switch (Calendar.getInstance().getFirstDayOfWeek()) {
        case Calendar.SUNDAY:
            assertEquals("Sunday", Vba.weekdayName(1, false, 0));
            assertEquals("Monday", Vba.weekdayName(2, false, 0));
            assertEquals("Saturday", Vba.weekdayName(7, false, 0));
            assertEquals("Sat", Vba.weekdayName(7, true, 0));
            break;
        case Calendar.MONDAY:
            assertEquals("Monday", Vba.weekdayName(1, false, 0));
            assertEquals("Tuesday", Vba.weekdayName(2, false, 0));
            assertEquals("Sunday", Vba.weekdayName(7, false, 0));
            assertEquals("Sun", Vba.weekdayName(7, true, 0));
            break;
        }
    }

    // Mathematical

    public void testAbs() {
        assertEquals(Vba.abs(-1.7d), 1.7d);
    }

    public void testAtn() {
        assertEquals(0d, Vba.atn(0), SMALL);
        assertEquals(Math.PI / 4d, Vba.atn(1), SMALL);
    }

    public void testCos() {
        assertEquals(1d, Vba.cos(0), 0d);
        assertEquals(Vba.sqr(0.5d), Vba.cos(Math.PI / 4d), 0d);
        assertEquals(0d, Vba.cos(Math.PI / 2d), SMALL);
        assertEquals(-1d, Vba.cos(Math.PI), 0d);
    }

    public void testExp() {
        assertEquals(1d, Vba.exp(0));
        assertEquals(Math.E, Vba.exp(1), 1e-10);
    }

    public void testRound() {
        assertEquals(123d, Vba.round(123.4567d), SMALL);
    }

    public void testRound2() {
        assertEquals(123d, Vba.round(123.4567d, 0), SMALL);
        assertEquals(123.46d, Vba.round(123.4567d, 2), SMALL);
        assertEquals(120d, Vba.round(123.45d, -1), SMALL);
        assertEquals(-123.46d, Vba.round(-123.4567d, 2), SMALL);
    }

    public void testSgn() {
        assertEquals(1, Vba.sgn(3.11111d), 0d);
        assertEquals(-1, Vba.sgn(-Math.PI), 0d);
        assertTrue(0 == Vba.sgn(-0d));
        assertTrue(0 == Vba.sgn(0d));
    }

    public void testSin() {
        assertEquals(Vba.sqr(0.5d), Vba.sin(Math.PI / 4d), SMALL);
    }

    public void testSqr() {
        assertEquals(2d, Vba.sqr(4d), 0d);
        assertEquals(0d, Vba.sqr(0d), 0d);
        assertTrue(Double.isNaN(Vba.sqr(-4)));
    }

    public void testTan() {
        assertEquals(1d, Vba.tan(Math.PI / 4d), SMALL);
    }
}

// End VbaTest.java
