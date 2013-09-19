/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.olap.fun.vba;

import mondrian.olap.InvalidArgumentException;
import mondrian.olap.Util;

import java.text.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static mondrian.olap.fun.JavaFunDef.*;

/**
 * Implementations of functions in the Visual Basic for Applications (VBA)
 * specification.
 *
 * <p>The functions are defined in
 * <a href="http://msdn.microsoft.com/en-us/library/32s6akha(VS.80).aspx">MSDN
 * </a>.
 *
 * @author jhyde
 * @since Dec 31, 2007
 */
public class Vba {
    private static final long MILLIS_IN_A_DAY = 24 * 60 * 60 * 1000;

    private static final DateFormatSymbols DATE_FORMAT_SYMBOLS =
        new DateFormatSymbols(Locale.getDefault());

    // Conversion

    @FunctionName("CBool")
    @Signature("CBool(expression)")
    @Description(
        "Returns an expression that has been converted to a Variant of subtype "
        + "Boolean.")
    public static boolean cBool(Object expression) {
        if (expression instanceof Boolean) {
            return (Boolean) expression;
        } else {
            int i = cInt(expression);
            return i != 0;
        }
    }

    // Conversion functions

    @FunctionName("CByte")
    @Signature("CByte(expression)")
    @Description(
        "Returns an expression that has been converted to a Variant of subtype "
        + "Byte.")
    public static byte cByte(Object expression) {
        if (expression instanceof Byte) {
            return (Byte) expression;
        } else {
            int i = cInt(expression);
            return (byte) i;
        }
    }

    // public Currency cCur(Object expression)

    @FunctionName("CDate")
    @Signature("CDate(date)")
    @Description(
        "Returns an expression that has been converted to a Variant of subtype "
        + "Date.")
    public static Date cDate(Object expression) {
        String str = String.valueOf(expression);
        if (expression instanceof Date) {
            return (Date) expression;
        } else if (expression == null) {
            return null;
        } else {
            // note that this currently only supports a limited set of dates and
            // times
            // "October 19, 1962"
            // "4:35:47 PM"
            try {
                return DateFormat.getTimeInstance().parse(str);
            } catch (ParseException ex0) {
                try {
                    return DateFormat.getDateTimeInstance().parse(str);
                } catch (ParseException ex1) {
                    try {
                        return DateFormat.getDateInstance().parse(str);
                    } catch (ParseException ex2) {
                        throw new InvalidArgumentException(
                            "Invalid parameter. "
                            + "expression parameter of CDate function must be "
                            + "formatted correctly ("
                            + String.valueOf(expression) + ")");
                    }
                }
            }
        }
    }

    @FunctionName("CDbl")
    @Signature("CDbl(expression)")
    @Description(
        "Returns an expression that has been converted to a Variant of subtype "
        + "Double.")
    public static double cDbl(Object expression) {
        if (expression instanceof Number) {
            Number number = (Number) expression;
            return number.doubleValue();
        } else {
            final String s = String.valueOf(expression);
            return new Double(s).intValue();
        }
    }

    @FunctionName("CInt")
    @Signature("CInt(expression)")
    @Description(
        "Returns an expression that has been converted to a Variant of subtype "
        + "Integer.")
    public static int cInt(Object expression) {
        if (expression instanceof Number) {
            Number number = (Number) expression;
            final int intValue = number.intValue();
            if (number instanceof Float || number instanceof Double) {
                final double doubleValue = number.doubleValue();
                if (doubleValue == (double) intValue) {
                    // Number is already an integer
                    return intValue;
                }
                final double doubleDouble = doubleValue * 2d;
                if (doubleDouble == Math.floor(doubleDouble)) {
                    // Number ends in .5 - round towards even required
                    return (int) Math.round(doubleValue / 2d) * 2;
                }
                return (int) Math.round(doubleValue);
            }
            return intValue;
        } else {
            // Try to parse as integer before parsing as double. More
            // efficient, and avoids loss of precision.
            final String s = String.valueOf(expression);
            try {
                return Integer.parseInt(s);
            } catch (NumberFormatException e) {
                return new Double(s).intValue();
            }
        }
    }

    // public int cLng(Object expression)
    // public float cSng(Object expression)
    // public String cStr(Object expression)
    // public Object cVDate(Object expression)
    // public Object cVErr(Object expression)
    // public Object cVar(Object expression)
    // public String error$(Object errorNumber)
    // public Object error(Object errorNumber)

    @FunctionName("Fix")
    @Signature("Fix(number)")
    @Description(
        "Returns the integer portion of a number. If negative, returns the "
        + "negative number greater than or equal to the number.")
    public static int fix(Object number) {
        if (number instanceof Number) {
            int v = ((Number) number).intValue();
            double dv = ((Number) number).doubleValue();
            if (v < 0 && v < dv) {
                v++;
            }
            return v;
        } else {
            throw new InvalidArgumentException(
                "Invalid parameter. "
                + "number parameter " + number
                + " of Int function must be " + "of type number");
        }
    }

    @FunctionName("Hex")
    @Signature("Hex(number)")
    @Description(
        "Returns a String representing the hexadecimal value of a number.")
    public static String hex(Object number) {
        if (number instanceof Number) {
            return Integer.toHexString(((Number) number).intValue())
                    .toUpperCase();
        } else {
            throw new InvalidArgumentException(
                "Invalid parameter. "
                + "number parameter " + number
                + " of Hex function must be " + "of type number");
        }
    }

    @FunctionName("Int")
    @Signature("Int(number)")
    @Description(
        "Returns the integer portion of a number. If negative, returns the "
        + "negative number less than or equal to the number.")
    public static int int_(Object number) {
        if (number instanceof Number) {
            int v = ((Number) number).intValue();
            double dv = ((Number) number).doubleValue();
            if (v < 0 && v > dv) {
                v--;
            }
            return v;
        } else {
            throw new InvalidArgumentException(
                "Invalid parameter. "
                + "number parameter " + number
                + " of Int function must be " + "of type number");
        }
    }

    /**
     * Equivalent of the {@link #int_} function on the native 'double' type.
     * Not an MDX function.
     *
     * @param dv Double value
     * @return Value rounded towards negative infinity
     */
    static int intNative(double dv) {
        int v = (int) dv;
        if (v < 0 && v > dv) {
            v--;
        }
        return v;
    }

    // public String oct$(Object number)

    @FunctionName("Oct")
    @Signature("Oct(number)")
    @Description(
        "Returns a Variant (String) representing the octal value of a number.")
    public static String oct(Object number) {
        if (number instanceof Number) {
            return Integer.toOctalString(((Number) number).intValue());
        } else {
            throw new InvalidArgumentException(
                "Invalid parameter. "
                + "number parameter " + number
                + " of Oct function must be " + "of type number");
        }
    }

    // public String str$(Object number)

    @FunctionName("Str")
    @Signature("Str(number)")
    @Description("Returns a Variant (String) representation of a number.")
    public static String str(Object number) {
        // When numbers are converted to strings, a leading space is always
        // reserved for the sign of number. If number is positive, the returned
        // string contains a leading space and the plus sign is implied.
        //
        // Use the Format function to convert numeric values you want formatted
        // as dates, times, or currency or in other user-defined formats.
        // Unlike Str, the Format function doesn't include a leading space for
        // the sign of number.
        //
        // Note The Str function recognizes only the period (.) as a valid
        // decimal separator. When different decimal separators may be used
        // (for example, in international applications), use CStr to convert a
        // number to a string.
        if (number instanceof Number) {
            if (((Number) number).doubleValue() >= 0) {
                return " " + number.toString();
            } else {
                return number.toString();
            }
        } else {
            throw new InvalidArgumentException(
                "Invalid parameter. "
                + "number parameter " + number
                + " of Str function must be " + "of type number");
        }
    }

    @FunctionName("Val")
    @Signature("Val(string)")
    @Description(
        "Returns the numbers contained in a string as a numeric value of "
        + "appropriate type.")
    public static double val(String string) {
        // The Val function stops reading the string at the first character it
        // can't recognize as part of a number. Symbols and characters that are
        // often considered parts of numeric values, such as dollar signs and
        // commas, are not recognized. However, the function recognizes the
        // radix prefixes &O (for octal) and &H (for hexadecimal). Blanks,
        // tabs, and linefeed characters are stripped from the argument.
        //
        // The following returns the value 1615198:
        //
        // Val(" 1615 198th Street N.E.")
        // In the code below, Val returns the decimal value -1 for the
        // hexadecimal value shown:
        //
        // Val("&HFFFF")
        // Note The Val function recognizes only the period (.) as a valid
        // decimal separator. When different decimal separators are used, as in
        // international applications, use CDbl instead to convert a string to
        // a number.

        string = string.replaceAll("\\s", ""); // remove all whitespace
        if (string.startsWith("&H")) {
            string = string.substring(2);
            Pattern p = Pattern.compile("[0-9a-fA-F]*");
            Matcher m = p.matcher(string);
            m.find();
            return Integer.parseInt(m.group(), 16);
        } else if (string.startsWith("&O")) {
            string = string.substring(2);
            Pattern p = Pattern.compile("[0-7]*");
            Matcher m = p.matcher(string);
            m.find();
            return Integer.parseInt(m.group(), 8);
        } else {
            // find the first number
            Pattern p = Pattern.compile("-?[0-9]*[.]?[0-9]*");
            Matcher m = p.matcher(string);
            m.find();
            return Double.parseDouble(m.group());
        }
    }

    // DateTime

    // public Calendar calendar()
    // public void calendar(Calendar val)
    // public String date$()
    // public void date$(String val)

    @FunctionName("DateAdd")
    @Signature("DateAdd(interval, number, date)")
    @Description(
        "Returns a Variant (Date) containing a date to which a specified time "
        + "interval has been added.")
    public static Date dateAdd(String intervalName, double number, Date date) {
        Interval interval = Interval.valueOf(intervalName);
        final double floor = Math.floor(number);

        // We use the local calendar here. This method will therefore return
        // different results in different locales: it depends whether the
        // initial date and the final date are in DST.
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        if (floor != number) {
            final double ceil = Math.ceil(number);
            interval.add(calendar, (int) ceil);
            final long ceilMillis = calendar.getTimeInMillis();

            calendar.setTime(date);
            interval.add(calendar, (int) floor);
            final long floorMillis = calendar.getTimeInMillis();

            final long amount =
                (long)
                    (((double) (ceilMillis - floorMillis)) * (number - floor));
            calendar.add(
                Calendar.DAY_OF_YEAR,
                (int) (amount / MILLIS_IN_A_DAY));
            calendar.add(
                Calendar.MILLISECOND, (int)
                (amount % MILLIS_IN_A_DAY));
        } else {
            interval.add(calendar, (int) floor);
        }
        return calendar.getTime();
    }

    @FunctionName("DateDiff")
    @Signature(
        "DateDiff(interval, date1, date2[, firstdayofweek[, firstweekofyear]])")
    @Description(
        "Returns a Variant (Long) specifying the number of time intervals "
        + "between two specified dates.")
    public static long dateDiff(String interval, Date date1, Date date2) {
        return _dateDiff(
            interval, date1, date2, Calendar.SUNDAY,
            FirstWeekOfYear.vbFirstJan1);
    }

    @FunctionName("DateDiff")
    @Signature(
        "DateDiff(interval, date1, date2[, firstdayofweek[, firstweekofyear]])")
    @Description(
        "Returns a Variant (Long) specifying the number of time intervals "
        + "between two specified dates.")
    public static long dateDiff(
        String interval, Date date1, Date date2, int firstDayOfWeek)
    {
        return _dateDiff(
            interval, date1, date2, firstDayOfWeek,
            FirstWeekOfYear.vbFirstJan1);
    }

    @FunctionName("DateDiff")
    @Signature(
        "DateDiff(interval, date1, date2[, firstdayofweek[, firstweekofyear]])")
    @Description(
        "Returns a Variant (Long) specifying the number of time intervals "
        + "between two specified dates.")
    public static long dateDiff(
        String interval, Date date1, Date date2,
        int firstDayOfWeek, int firstWeekOfYear)
    {
        return _dateDiff(
            interval, date1, date2, firstDayOfWeek,
            FirstWeekOfYear.values()[firstWeekOfYear]);
    }

    private static long _dateDiff(
        String intervalName, Date date1, Date date2,
        int firstDayOfWeek, FirstWeekOfYear firstWeekOfYear)
    {
        Interval interval = Interval.valueOf(intervalName);
        Calendar calendar1 = Calendar.getInstance();
        firstWeekOfYear.apply(calendar1);
        calendar1.setTime(date1);
        Calendar calendar2 = Calendar.getInstance();
        firstWeekOfYear.apply(calendar2);
        calendar2.setTime(date2);
        return interval.diff(calendar1, calendar2, firstDayOfWeek);
    }

    @FunctionName("DatePart")
    @Signature("DatePart(interval, date[,firstdayofweek[, firstweekofyear]])")
    @Description(
        "Returns a Variant (Integer) containing the specified part of a given "
        + "date.")
    public static int datePart(String interval, Date date) {
        return _datePart(
            interval, date, Calendar.SUNDAY,
            FirstWeekOfYear.vbFirstJan1);
    }

    @FunctionName("DatePart")
    @Signature("DatePart(interval, date[,firstdayofweek[, firstweekofyear]])")
    @Description(
        "Returns a Variant (Integer) containing the specified part of a given "
        + "date.")
    public static int datePart(String interval, Date date, int firstDayOfWeek) {
        return _datePart(
            interval, date, firstDayOfWeek,
            FirstWeekOfYear.vbFirstJan1);
    }

    @FunctionName("DatePart")
    @Signature("DatePart(interval, date[,firstdayofweek[, firstweekofyear]])")
    @Description(
        "Returns a Variant (Integer) containing the specified part of a given "
        + "date.")
    public static int datePart(
        String interval, Date date, int firstDayOfWeek,
        int firstWeekOfYear)
    {
        return _datePart(
            interval, date, firstDayOfWeek,
            FirstWeekOfYear.values()[firstWeekOfYear]);
    }

    private static int _datePart(
        String intervalName,
        Date date,
        int firstDayOfWeek,
        FirstWeekOfYear firstWeekOfYear)
    {
        Interval interval = Interval.valueOf(intervalName);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        switch (interval) {
        case w:
        case ww:
            // firstWeekOfYear and firstDayOfWeek only matter for 'w' and 'ww'
            firstWeekOfYear.apply(calendar);
            calendar.setFirstDayOfWeek(firstDayOfWeek);
            break;
        }
        return interval.datePart(calendar);
    }

    @FunctionName("Date")
    @Signature("Date")
    @Description("Returns a Variant (Date) containing the current system date.")
    public static Date date() {
        Calendar calendar = Calendar.getInstance();
        calendar.clear();
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar.getTime();
    }

    // public void date(Object val)

    @FunctionName("DateSerial")
    @Signature("DateSerial(year, month, day)")
    @Description(
        "Returns a Variant (Date) for a specified year, month, and day.")
    public static Date dateSerial(int year, int month, int day) {
        Calendar calendar = Calendar.getInstance();
        calendar.clear();
        calendar.set(year, month - 1, day);
        return calendar.getTime();
    }

    @FunctionName("DateValue")
    @Signature("DateValue(date)")
    @Description("Returns a Variant (Date).")
    public static Date dateValue(Date date) {
        final Calendar calendar = Calendar.getInstance();
        calendar.clear();
        calendar.setTime(date);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar.getTime();
    }

    @FunctionName("Day")
    @Signature("Day(date)")
    @Description(
        "Returns a Variant (Integer) specifying a whole number between 1 and "
        + "31, inclusive, representing the day of the month.")
    public static int day(Date date) {
        final Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar.get(Calendar.DAY_OF_MONTH);
    }

    @FunctionName("Hour")
    @Signature("Hour(time)")
    @Description(
        "Returns a Variant (Integer) specifying a whole number between 0 and "
        + "23, inclusive, representing the hour of the day.")
    public static int hour(Date time) {
        final Calendar calendar = Calendar.getInstance();
        calendar.setTime(time);
        return calendar.get(Calendar.HOUR_OF_DAY);
    }

    @FunctionName("Minute")
    @Signature("Minute(time)")
    @Description(
        "Returns a Variant (Integer) specifying a whole number between 0 and "
        + "59, inclusive, representing the minute of the hour.")
    public static int minute(Date time) {
        final Calendar calendar = Calendar.getInstance();
        calendar.setTime(time);
        return calendar.get(Calendar.MINUTE);
    }

    @FunctionName("Month")
    @Signature("Month(date)")
    @Description(
        "Returns a Variant (Integer) specifying a whole number between 1 and "
        + "12, inclusive, representing the month of the year.")
    public static int month(Date date) {
        final Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        final int month = calendar.get(Calendar.MONTH);
        return month + 1; // convert from 0- to 1-based
    }

    @FunctionName("Now")
    @Signature("Now()")
    @Description(
        "Returns a Variant (Date) specifying the current date and time "
        + "according your computer's system date and time.")
    public static Date now() {
        return new Date();
    }

    @FunctionName("Second")
    @Signature("Second(time)")
    @Description(
        "Returns a Variant (Integer) specifying a whole number between 0 and "
        + "59, inclusive, representing the second of the minute.")
    public static int second(Date time) {
        final Calendar calendar = Calendar.getInstance();
        calendar.setTime(time);
        return calendar.get(Calendar.SECOND);
    }

    // public String time$()
    // public void time$(String val)

    @FunctionName("Time")
    @Signature("Time()")
    @Description("Returns a Variant (Date) indicating the current system time.")
    public static Date time() {
        return new Date();
    }

    // public void time(Object val)

    @FunctionName("TimeSerial")
    @Signature("TimeSerial(hour, minute, second)")
    @Description(
        "Returns a Variant (Date) containing the time for a specific hour, "
        + "minute, and second.")
    public static Date timeSerial(int hour, int minute, int second) {
        final Calendar calendar = Calendar.getInstance();
        calendar.clear();
        calendar.set(Calendar.HOUR_OF_DAY, hour);
        calendar.set(Calendar.MINUTE, minute);
        calendar.set(Calendar.SECOND, second);
        return calendar.getTime();
    }

    @FunctionName("TimeValue")
    @Signature("TimeValue(time)")
    @Description("Returns a Variant (Date) containing the time.")
    public static Date timeValue(Date time) {
        final Calendar calendar = Calendar.getInstance();
        calendar.clear();
        calendar.setTime(time);
        calendar.set(1970, 0, 1);
        return calendar.getTime();
    }

    @FunctionName("Timer")
    @Signature("Timer()")
    @Description(
        "Returns a Single representing the number of seconds elapsed since "
        + "midnight.")
    public static float timer() {
        final Calendar calendar = Calendar.getInstance();
        final long now = calendar.getTimeInMillis();
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        final long midnight = calendar.getTimeInMillis();
        return ((float) (now - midnight)) / 1000f;
    }

    @FunctionName("Weekday")
    @Signature("Weekday(date[, firstDayOfWeek])")
    @Description(
        "Returns a Variant (Integer) containing a whole number representing "
        + "the day of the week.")
    public static int weekday(Date date) {
        return weekday(date, Calendar.SUNDAY);
    }

    @FunctionName("Weekday")
    @Signature("Weekday(date[, firstDayOfWeek])")
    @Description(
        "Returns a Variant (Integer) containing a whole number representing "
        + "the day of the week.")
    public static int weekday(Date date, int firstDayOfWeek) {
        final Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        int weekday = calendar.get(Calendar.DAY_OF_WEEK);
        // adjust for start of week
        weekday -= (firstDayOfWeek - 1);
        // bring into range 1..7
        weekday = (weekday + 6) % 7 + 1;
        return weekday;
    }

    @FunctionName("Year")
    @Signature("Year(date)")
    @Description(
        "Returns a Variant (Integer) containing a whole number representing "
        + "the year.")
    public static int year(Date date) {
        final Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar.get(Calendar.YEAR);
    }

    //
    // /* FileSystem */
    // public void chDir(String path)
    // public void chDrive(String drive)
    // public String curDir$(Object drive)
    // public Object curDir(Object drive)
    // public String dir(Object pathName, FileAttribute attributes /* default
    // FileAttribute.Normal */)
    // public boolean EOF(int fileNumber)
    // public int fileAttr(int fileNumber, int returnType /* default 1 */)
    // public void fileCopy(String source, String destination)
    // public Object fileDateTime(String pathName)
    // public int fileLen(String pathName)
    // public int freeFile(Object rangeNumber)
    // public FileAttribute getAttr(String pathName)
    // public void kill(Object pathName)
    // public int LOF(int fileNumber)
    // public int loc(int fileNumber)
    // public void mkDir(String path)
    // public void reset()
    // public void rmDir(String path)
    // public int seek(int fileNumber)
    // public void setAttr(String pathName, FileAttribute attributes)
    //
    // Financial

    @FunctionName("DDB")
    @Signature("DDB(cost, salvage, life, period[, factor])")
    @Description(
        "Returns a Double specifying the depreciation of an asset for a "
        + "specific time period using the double-declining balance method or "
        + "some other method you specify.")
    public static double dDB(
        double cost,
        double salvage,
        double life,
        double period)
    {
        return dDB(cost, salvage, life, period, 2.0);
    }

    @FunctionName("DDB")
    @Signature("DDB(cost, salvage, life, period[, factor])")
    @Description(
        "Returns a Double specifying the depreciation of an asset for a "
        + "specific time period using the double-declining balance method or "
        + "some other method you specify.")
    public static double dDB(
        double cost,
        double salvage,
        double life,
        double period,
        double factor)
    {
        return (((cost - salvage) * factor) / life) * period;
    }

    @FunctionName("FV")
    @Signature("FV(rate, nper, pmt[, pv[, type]])")
    @Description(
        "Returns a Double specifying the future value of an annuity based on "
        + "periodic, fixed payments and a fixed interest rate.")
    public static double fV(double rate, double nPer, double pmt) {
        return fV(rate, nPer, pmt, 0d, false);
    }

    @FunctionName("FV")
    @Signature("FV(rate, nper, pmt[, pv[, type]])")
    @Description(
        "Returns a Double specifying the future value of an annuity based on "
        + "periodic, fixed payments and a fixed interest rate.")
    public static double fV(double rate, double nPer, double pmt, double pv) {
        return fV(rate, nPer, pmt, pv, false);
    }

    @FunctionName("FV")
    @Signature("FV(rate, nper, pmt[, pv[, type]])")
    @Description(
        "Returns a Double specifying the future value of an annuity based on "
        + "periodic, fixed payments and a fixed interest rate.")
    public static double fV(
        double rate,
        double nPer,
        double pmt,
        double pv,
        boolean type)
    {
        if (rate == 0) {
            return -(pv + (nPer * pmt));
        } else {
            double r1 = rate + 1;
            return ((1 - Math.pow(r1, nPer)) * (type ? r1 : 1) * pmt) / rate
                    - pv * Math.pow(r1, nPer);
        }
    }

    @FunctionName("IPmt")
    @Signature("IPmt(rate, per, nper, pv[, fv[, type]])")
    @Description(
        "Returns a Double specifying the interest payment for a given period "
        + "of an annuity based on periodic, fixed payments and a fixed "
        + "interest rate.")
    public static double iPmt(double rate, double per, double nPer, double PV) {
        return iPmt(rate, per, nPer, PV, 0);
    }


    @FunctionName("IPmt")
    @Signature("IPmt(rate, per, nper, pv[, fv[, type]])")
    @Description(
        "Returns a Double specifying the interest payment for a given period "
        + "of an annuity based on periodic, fixed payments and a fixed "
        + "interest rate.")
    public static double iPmt(
        double rate,
        double per,
        double nPer,
        double PV,
        double fv)
    {
        return iPmt(rate, per, nPer, PV, fv, false);
    }


    @FunctionName("IPmt")
    @Signature("IPmt(rate, per, nper, pv[, fv[, type]])")
    @Description(
        "Returns a Double specifying the interest payment for a given period "
        + "of an annuity based on periodic, fixed payments and a fixed "
        + "interest rate.")
    public static double iPmt(
        double rate,
        double per,
        double nPer,
        double PV,
        double fv,
        boolean due)
    {
        double pmtVal = pmt(rate, nPer, PV, fv, due);
        double pValm1 = PV - pV(rate, per - 1, pmtVal, fv, due);
        return - pValm1 * rate;
    }

    @FunctionName("IRR")
    @Signature("IRR(values()[, guess])")
    @Description(
        "Returns a Double specifying the internal rate of return for a series "
        + "of periodic cash flows (payments and receipts).")
    public static double IRR(double[] valueArray) {
        return IRR(valueArray, 0.10);
    }


    @FunctionName("IRR")
    @Signature("IRR(values()[, guess])")
    @Description(
        "Returns a Double specifying the internal rate of return for a series "
        + "of periodic cash flows (payments and receipts).")
    public static double IRR(double[] valueArray, double guess) {
        // calc pV of stream (sum of pV's for valueArray) ((1 + guess) ^ index)
        double minGuess = 0.0;
        double maxGuess = 1.0;

        // i'm not certain
        int r = 1;
        if (valueArray[0] > 0) {
            r = -1;
        }

        for (int i = 0; i < 30; i++) {
            // first calculate overall return based on guess
            double totalPv = 0;
            for (int j = 0; j < valueArray.length; j++) {
                totalPv += valueArray[j] / Math.pow(1.0 + guess, j);
            }
            if ((maxGuess - minGuess) < 0.0000001) {
                return guess;
            } else if (totalPv * r < 0) {
                maxGuess = guess;
            } else {
                minGuess = guess;
            }
            // avg max min to determine next step
            guess = (maxGuess + minGuess) / 2;
        }
        // unable to find a match
        return -1;
    }

    @FunctionName("MIRR")
    @Signature("MIRR(values(), finance_rate, reinvest_rate)")
    @Description(
        "Returns a Double specifying the modified internal rate of return for "
        + "a series of periodic cash flows (payments and receipts).")
    public static double MIRR(
        double valueArray[],
        double financeRate,
        double reinvestRate)
    {
        // based on
        // http://en.wikipedia.org/wiki/Modified_Internal_Rate_of_Return
        double reNPV = 0.0;
        double fiNPV = 0.0;
        for (int j = 0; j < valueArray.length; j++) {
            if (valueArray[j] > 0) {
                reNPV += valueArray[j] / Math.pow(1.0 + reinvestRate, j);
            } else {
                fiNPV += valueArray[j] / Math.pow(1.0 + financeRate, j);
            }
        }

        double ratio =
            (- reNPV * Math.pow(1 + reinvestRate, valueArray.length))
            / (fiNPV * (1 + financeRate));

        return Math.pow(ratio, 1.0 / (valueArray.length - 1)) - 1.0;
    }

    @FunctionName("NPer")
    @Signature("NPer(rate, pmt, pv[, fv[, type]])")
    @Description(
        "Returns a Double specifying the number of periods for an annuity "
        + "based on periodic, fixed payments and a fixed interest rate.")
    public static double nPer(
        double rate,
        double pmt,
        double pv,
        double fv,
        boolean due)
    {
        if (rate == 0) {
            return -(fv + pv) / pmt;
        } else {
            double r1 = rate + 1;
            double ryr = (due ? r1 : 1) * pmt / rate;
            double a1 =
                ((ryr - fv) < 0)
                ? Math.log(fv - ryr)
                : Math.log(ryr - fv);
            double a2 =
                ((ryr - fv) < 0)
                ? Math.log(-pv - ryr)
                : Math.log(pv + ryr);
            double a3 = Math.log(r1);
            return (a1 - a2) / a3;
        }
    }

    @FunctionName("NPV")
    @Signature("NPV(rate, values())")
    @Description(
        "Returns a Double specifying the net present value of an investment "
        + "based on a series of periodic cash flows (payments and receipts) "
        + "and a discount rate.")
    public static double nPV(double r, double[] cfs) {
        double npv = 0;
        double r1 = r + 1;
        double trate = r1;
        for (int i = 0, iSize = cfs.length; i < iSize; i++) {
            npv += cfs[i] / trate;
            trate *= r1;
        }
        return npv;
    }

    @FunctionName("PPmt")
    @Signature("PPmt(rate, per, nper, pv[, fv[, type]])")
    @Description(
        "Returns a Double specifying the principal payment for a given period "
        + "of an annuity based on periodic, fixed payments and a fixed "
        + "interest rate.")
    public static double pPmt(double rate, double per, double nPer, double PV) {
        return pPmt(rate, per, nPer, PV, 0);
    }

    @FunctionName("PPmt")
    @Signature("PPmt(rate, per, nper, pv[, fv[, type]])")
    @Description(
        "Returns a Double specifying the principal payment for a given period "
        + "of an annuity based on periodic, fixed payments and a fixed "
        + "interest rate.")
    public static double pPmt(
        double rate,
        double per,
        double nPer,
        double PV,
        double fv)
    {
        return pPmt(rate, per, nPer, PV, fv, false);
    }

    @FunctionName("PPmt")
    @Signature("PPmt(rate, per, nper, pv[, fv[, type]])")
    @Description(
        "Returns a Double specifying the principal payment for a given period "
        + "of an annuity based on periodic, fixed payments and a fixed "
        + "interest rate.")
    public static double pPmt(
        double rate,
        double per,
        double nPer,
        double PV,
        double fv,
        boolean due)
    {
        return pmt(rate, nPer, PV, fv, due)
            - iPmt(rate, per, nPer, PV, fv, due);
    }

    @FunctionName("Pmt")
    @Signature("Pmt(rate, nper, pv[, fv[, type]])")
    @Description(
        "Returns a Double specifying the payment for an annuity based on "
        + "periodic, fixed payments and a fixed interest rate.")
    public static double pmt(
        double rate,
        double nPer,
        double pv,
        double fv,
        boolean due)
    {
        if (rate == 0) {
            return -(fv + pv) / nPer;
        } else {
            double r1 = rate + 1;
            return
                (fv + pv * Math.pow(r1, nPer))
                * rate
                / ((due ? r1 : 1) * (1 - Math.pow(r1, nPer)));
        }
    }

    @FunctionName("PV")
    @Signature("PV(rate, nper, pmt[, fv[, type]])")
    @Description(
        "Returns a Double specifying the present value of an annuity based on "
        + "periodic, fixed payments to be paid in the future and a fixed "
        + "interest rate.")
    public static double pV(
        double rate,
        double nper,
        double pmt,
        double fv,
        boolean due)
    {
        if (rate == 0) {
            return -((nper * pmt) + fv);
        } else {
            double r1 = rate + 1;
            return
                (((1 - Math.pow(r1, nper)) / rate) * (due ? r1 : 1) * pmt - fv)
                    / Math.pow(r1, nper);
        }
    }

    @FunctionName("Rate")
    @Signature("Rate(nper, pmt, pv[, fv[, type[, guess]]])")
    @Description(
        "Returns a Double specifying the interest rate per period for an "
        + "annuity.")
    public static double rate(
        double nPer,
        double pmt,
        double PV)
    {
        return rate(nPer, pmt, PV, 0);
    }

    @FunctionName("Rate")
    @Signature("Rate(nper, pmt, pv[, fv[, type[, guess]]])")
    @Description(
        "Returns a Double specifying the interest rate per period for an "
        + "annuity.")
    public static double rate(
        double nPer,
        double pmt,
        double PV,
        double fv)
    {
        return rate(nPer, pmt, PV, fv, false);
    }

    @FunctionName("Rate")
    @Signature("Rate(nper, pmt, pv[, fv[, type[, guess]]])")
    @Description(
        "Returns a Double specifying the interest rate per period for an "
        + "annuity.")
    public static double rate(
        double nPer,
        double pmt,
        double PV,
        double fv,
        boolean type)
    {
        return rate(nPer, pmt, PV, fv, type, 0.1);
    }

    @FunctionName("Rate")
    @Signature("Rate(nper, pmt, pv[, fv[, type[, guess]]])")
    @Description(
        "Returns a Double specifying the interest rate per period for an "
        + "annuity.")
    public static double rate(
        double nPer, // specifies the number of payment periods
        double pmt, // payment per period of annuity
        double PV, // the present value of the annuity (0 if a loan)
        double fv, // the future value of the annuity ($ if savings)
        boolean due,
        double guess)
    {
        if (nPer <= 0) {
            throw new InvalidArgumentException(
                "number of payment periods must be larger than 0");
        }
        double minGuess = 0.0;
        double maxGuess = 1.0;

        // converge on the correct answer should use Newton's Method
        // for now use a binary search
        int r = 1;
        if (PV < fv) {
            r = -1;
        }

        // the vb method uses 20 iterations, but they also probably use newton's
        // method,
        // so i've bumped it up to 30 iterations.
        for (int n = 0; n < 30; n++) {
            double gFV = fV(guess, nPer, pmt, PV, due);
            double diff = gFV - fv;
            if ((maxGuess - minGuess) < 0.0000001) {
                return guess;
            } else {
                if (diff * r < 0) {
                    maxGuess = guess;
                } else {
                    minGuess = guess;
                }
                guess = (maxGuess + minGuess) / 2;
            }
        }
        // fail, not sure how VB fails
        return -1;
    }

    @FunctionName("SLN")
    @Signature("SLN(cost, salvage, life)")
    @Description(
        "Returns a Double specifying the straight-line depreciation of an "
        + "asset for a single period.")
    public static double sLN(double cost, double salvage, double life) {
        return (cost - salvage) / life;
    }

    @FunctionName("SYD")
    @Signature("SYD(cost, salvage, life, period)")
    @Description(
        "Returns a Double specifying the sum-of-years' digits depreciation of "
        + "an asset for a specified period.")
    public static double sYD(
        double cost,
        double salvage,
        double life,
        double period)
    {
        return (cost - salvage) * (life / (period * (period + 1) / 2));
    }

    // Information

    // public Throwable err()
    // public Object iMEStatus()

    @FunctionName("IsArray")
    @Signature("IsArray(varname)")
    @Description(
        "Returns a Boolean value indicating whether a variable is an array.")
    public boolean isArray(Object varName) {
        // arrays are not supported at present
        return false;
    }

    @FunctionName("IsDate")
    @Signature("IsDate(varname)")
    @Description(
        "Returns a Boolean value indicating whether an expression can be "
        + "converted to a date.")
    public static boolean isDate(Object expression) {
        // IsDate returns True if Expression represents a valid date, a valid
        // time, or a valid date and time.
        try {
            Date val = cDate(expression);
            return (val != null);
        } catch (InvalidArgumentException e) {
            return false;
        }
    }

    // use mondrian's implementation of IsEmpty
    // public boolean isEmpty(Object expression)

    @FunctionName("IsError")
    @Signature("IsError(varname)")
    @Description(
        "Returns a Boolean value indicating whether an expression is an error "
        + "value.")
    public boolean isError(Object expression) {
        return expression instanceof Throwable;
    }

    @FunctionName("IsMissing")
    @Signature("IsMissing(varname)")
    @Description(
        "Returns a Boolean value indicating whether an optional Variant "
        + "argument has been passed to a procedure.")
    public boolean isMissing(Object argName) {
        // We have no way to detect missing arguments.
        return false;
    }

    @FunctionName("IsNull")
    @Signature("IsNull(varname)")
    @Description(
        "Returns a Boolean value that indicates whether an expression "
        + "contains no valid data (Null).")
    public boolean isNull(Object expression) {
        return expression == null;
    }

    @FunctionName("IsNumeric")
    @Signature("IsNumeric(varname)")
    @Description(
        "Returns a Boolean value indicating whether an expression can be "
        + "evaluated as a number.")
    public boolean isNumeric(Object expression) {
        return expression instanceof Number;
    }

    @FunctionName("IsObject")
    @Signature("IsObject(varname)")
    @Description(
        "Returns a Boolean value indicating whether an identifier represents "
        + "an object variable.")
    public boolean isObject(Object expression) {
        return false;
    }

    // public int qBColor(int color)
    // public int RGB(int red, int green, int blue)

    @FunctionName("TypeName")
    @Signature("TypeName(varname)")
    @Description("Returns a String that provides information about a variable.")
    public static String typeName(Object varName) {
        // The string returned by TypeName can be any one of the following:
        //
        // String returned Variable
        // object type An object whose type is objecttype
        // Byte Byte value
        // Integer Integer
        // Long Long integer
        // Single Single-precision floating-point number
        // Double Double-precision floating-point number
        // Currency Currency value
        // Decimal Decimal value
        // Date Date value
        // String String
        // Boolean Boolean value
        // Error An error value
        // Empty Uninitialized
        // Null No valid data
        // Object An object
        // Unknown An object whose type is unknown
        // Nothing Object variable that doesn't refer to an object

        if (varName == null) {
            return "NULL";
        } else {
            // strip off the package information
            String name = varName.getClass().getName();
            if (name.lastIndexOf(".") >= 0) {
                name = name.substring(name.lastIndexOf(".") + 1);
            }
            return name;
        }
    }

    // public VarType varType(Object varName)

    // Interaction

    // public void appActivate(Object title, Object wait)
    // public void beep()
    // public Object callByName(Object object, String procName, CallType
    // callType, Object args, int lcid)
    // public Object choose(float index, Object choice)
    // public String command$()
    // public Object command()
    // public Object createObject(String Class, String serverName)
    // public int doEvents()
    // public String environ$(Object expression)
    // public Object environ(Object expression)
    // public Object getAllSettings(String appName, String section)
    // public Object getObject(Object pathName, Object Class)
    // public String getSetting(String appName, String section, String key,
    // Object Default)
    // public Object iIf(Object expression, Object truePart, Object falsePart)
    // public String inputBox(Object prompt, Object title, Object Default,
    // Object xPos, Object yPos, Object helpFile, Object context)
    // public String macScript(String script)
    // public MsgBoxResult msgBox(Object prompt, MsgBoxStyle buttons /* default
    // MsgBoxStyle.OKOnly */, Object title, Object helpFile, Object context)
    // public Object partition(Object number, Object start, Object stop, Object
    // interval)
    // public void saveSetting(String appName, String section, String key,
    // String setting)
    // public void sendKeys(String string, Object wait)
    // public double shell(Object pathName, AppWinStyle windowStyle /* default
    // AppWinStyle.MinimizedFocus */)
    // public Object Switch(Object varExpr)

    // Mathematical

    @FunctionName("Abs")
    @Signature("Abs(number)")
    @Description(
        "Returns a value of the same type that is passed to it specifying the "
        + "absolute value of a number.")
    public static double abs(double number) {
        return Math.abs(number);
    }

    @FunctionName("Atn")
    @Signature("Atn(number)")
    @Description("Returns a Double specifying the arctangent of a number.")
    public static double atn(double number) {
        return Math.atan(number);
    }

    @FunctionName("Cos")
    @Signature("Cos(number)")
    @Description("Returns a Double specifying the cosine of an angle.")
    public static double cos(double number) {
        return Math.cos(number);
    }

    @FunctionName("Exp")
    @Signature("Exp(number)")
    @Description(
        "Returns a Double specifying e (the base of natural logarithms) "
        + "raised to a power.")
    public static double exp(double number) {
        return Math.exp(number);
    }

    @FunctionName("Log")
    @Signature("Log(number)")
    @Description(
        "Returns a Double specifying the natural logarithm of a number.")
    public static double log(double number) {
        return Math.log(number);
    }

    // Cannot implement randomize and rnd - we require context to hold the
    // seed

    // public void randomize(Object number)
    // public float rnd(Object number)

    @FunctionName("Round")
    @Signature("Round(number[, numDigitsAfterDecimal])")
    @Description(
        "Returns a number rounded to a specified number of decimal places.")
    public static double round(double number) {
        return Math.round(number);
    }

    @FunctionName("Round")
    @Signature("Round(number[, numDigitsAfterDecimal])")
    @Description(
        "Returns a number rounded to a specified number of decimal places.")
    public static double round(double number, int numDigitsAfterDecimal) {
        if (numDigitsAfterDecimal == 0) {
            return Math.round(number);
        }
        final double shift = Math.pow(10d, numDigitsAfterDecimal);
        double numberScaled = number * shift;
        double resultScaled = Math.round(numberScaled);
        return resultScaled / shift;
    }

    @FunctionName("Sgn")
    @Signature("Sgn(number)")
    @Description("Returns a Variant (Integer) indicating the sign of a number.")
    public static int sgn(double number) {
        // We could use Math.signum(double) from JDK 1.5 onwards.
        return number < 0.0d ? -1 : number > 0.0d ? 1 : 0;
    }

    @FunctionName("Sin")
    @Signature("Sin(number)")
    @Description("Returns a Double specifying the sine of an angle.")
    public static double sin(double number) {
        return Math.sin(number);
    }

    @FunctionName("Sqr")
    @Signature("Sqr(number)")
    @Description("Returns a Double specifying the square root of a number.")
    public static double sqr(double number) {
        return Math.sqrt(number);
    }

    @FunctionName("Tan")
    @Signature("Tan(number)")
    @Description("Returns a Double specifying the tangent of an angle.")
    public static double tan(double number) {
        return Math.tan(number);
    }

    // Strings

    @FunctionName("Asc")
    @Signature("Asc(string)")
    @Description(
        "Returns an Integer representing the character code corresponding to "
        + "the first letter in a string.")
    public static int asc(String string) {
        return string.charAt(0);
    }

    @FunctionName("AscB")
    @Signature("AscB(string)")
    @Description("See Asc.")
    public static int ascB(String string) {
        return (byte) string.charAt(0);
    }

    @FunctionName("AscW")
    @Signature("AscW(string)")
    @Description("See Asc.")
    public static int ascW(String string) {
        return asc(string);
    }

    // public String chr$(int charCode)
    // public String chrB$(int charCode)

    @FunctionName("Chr")
    @Signature("Chr(charcode)")
    @Description(
        "Returns a String containing the character associated with the "
        + "specified character code.")
    public static String chr(int charCode) {
        return new String(new char[] { (char) charCode });
    }

    @FunctionName("ChrB")
    @Signature("ChrB(charcode)")
    @Description("See Chr.")
    public static String chrB(int charCode) {
        return new String(new byte[] { (byte) charCode });
    }

    // public String chrW$(int charCode)

    @FunctionName("ChrW")
    @Signature("ChrW(charcode)")
    @Description("See Chr.")
    public static String chrW(int charCode) {
        return new String(new char[] { (char) charCode });
    }

    // public Object filter(Object sourceArray, String match, boolean include /*
    // default 1 */, int compare /* default BinaryCompare */)
    // public String format$(Object expression, Object format, int
    // firstDayOfWeek /* default Sunday */, int firstWeekOfYear /* default
    // FirstJan1 */)

    @FunctionName("FormatCurrency")
    @Signature(
        "FormatCurrency(Expression[,NumDigitsAfterDecimal "
        + "[,IncludeLeadingDigit [,UseParensForNegativeNumbers "
        + "[,GroupDigits]]]])")
    @Description(
        "Returns an expression formatted as a currency value using the "
        + "currency symbol defined in the system control panel.")
    public static String formatCurrency(Object expression) {
        return formatCurrency(expression, -1, -2, -2, -2);
    }

    @FunctionName("FormatCurrency")
    @Signature(
        "FormatCurrency(Expression[,NumDigitsAfterDecimal "
        + "[,IncludeLeadingDigit [,UseParensForNegativeNumbers "
        + "[,GroupDigits]]]])")
    @Description(
        "Returns an expression formatted as a currency value using the "
        + "currency symbol defined in the system control panel.")
    public static String formatCurrency(
        Object expression,
        int numDigitsAfterDecimal)
    {
        return formatCurrency(expression, numDigitsAfterDecimal, -2, -2, -2);
    }

    @FunctionName("FormatCurrency")
    @Signature(
        "FormatCurrency(Expression[,NumDigitsAfterDecimal "
        + "[,IncludeLeadingDigit [,UseParensForNegativeNumbers "
        + "[,GroupDigits]]]])")
    @Description(
        "Returns an expression formatted as a currency value using the "
        + "currency symbol defined in the system control panel.")
    public static String formatCurrency(
        Object expression,
        int numDigitsAfterDecimal,
        int includeLeadingDigit)
    {
        return formatCurrency(
            expression, numDigitsAfterDecimal,
            includeLeadingDigit, -2, -2);
    }

    @FunctionName("FormatCurrency")
    @Signature(
        "FormatCurrency(Expression[,NumDigitsAfterDecimal "
        + "[,IncludeLeadingDigit [,UseParensForNegativeNumbers "
        + "[,GroupDigits]]]])")
    @Description(
        "Returns an expression formatted as a currency value using the "
        + "currency symbol defined in the system control panel.")
    public static String formatCurrency(
        Object expression,
        int numDigitsAfterDecimal,
        int includeLeadingDigit,
        int useParensForNegativeNumbers)
    {
        return formatCurrency(
            expression,
            numDigitsAfterDecimal,
            includeLeadingDigit,
            useParensForNegativeNumbers,
            -2);
    }

    @FunctionName("FormatCurrency")
    @Signature(
        "FormatCurrency(Expression[,NumDigitsAfterDecimal "
        + "[,IncludeLeadingDigit [,UseParensForNegativeNumbers "
        + "[,GroupDigits]]]])")
    @Description(
        "Returns an expression formatted as a currency value using the "
        + "currency symbol defined in the system control panel.")
    public static String formatCurrency(
        Object expression,
        int numDigitsAfterDecimal,
        int includeLeadingDigit,
        int useParensForNegativeNumbers,
        int groupDigits)
    {
        DecimalFormat format =
            (DecimalFormat) NumberFormat.getCurrencyInstance();
        if (numDigitsAfterDecimal != -1) {
            format.setMaximumFractionDigits(numDigitsAfterDecimal);
            format.setMinimumFractionDigits(numDigitsAfterDecimal);
        }
        if (includeLeadingDigit != -2) {
            if (includeLeadingDigit != 0) {
                format.setMinimumIntegerDigits(1);
            } else {
                format.setMinimumIntegerDigits(0);
            }
        }
        if (useParensForNegativeNumbers != -2) {
            // todo: implement.
            // This will require tweaking of the currency expression
        }
        if (groupDigits != -2) {
            if (groupDigits != 0) {
                format.setGroupingUsed(false);
            } else {
                format.setGroupingUsed(true);
            }
        }
        return format.format(expression);
    }

    @FunctionName("FormatDateTime")
    @Signature("FormatDateTime(Date[,NamedFormat])")
    @Description("Returns an expression formatted as a date or time.")
    public static String formatDateTime(Date date) {
        return formatDateTime(date, 0);
    }

    @FunctionName("FormatDateTime")
    @Signature("FormatDateTime(Date[,NamedFormat])")
    @Description("Returns an expression formatted as a date or time.")
    public static String formatDateTime(
        Date date,
        int namedFormat /* default 0, GeneralDate */)
    {
        // todo: test
        // todo: how do we support VB Constants? Strings or Ints?
        switch (namedFormat) {
            // vbLongDate, 1
            // Display a date using the long date format specified in your
            // computer's regional settings.

        case 1:
            return DateFormat.getDateInstance(DateFormat.LONG).format(date);

            // vbShortDate, 2
            // Display a date using the short date format specified in your
            // computer's regional settings.
        case 2:
            return DateFormat.getDateInstance(DateFormat.SHORT).format(date);

            // vbLongTime, 3
            // Display a time using the time format specified in your computer's
            // regional settings.
        case 3:
            return DateFormat.getTimeInstance(DateFormat.LONG).format(date);

            // vbShortTime, 4
            // Display a time using the 24-hour format (hh:mm).
        case 4:
            return DateFormat.getTimeInstance(DateFormat.SHORT).format(date);

            // vbGeneralDate, 0
            // Display a date and/or time. If there is a date part,
            // display it as a short date. If there is a time part,
            // display it as a long time. If present, both parts are
            // displayed.
            //
            // todo: how do we determine if there is a "time part" in java?
        case 0:
        default:
            return DateFormat.getDateTimeInstance().format(date);
        }
    }

    // Format is implemented with FormatFunDef, third and fourth params are not
    // supported
    // @FunctionName("Format")
    // @Signature("Format(expression[, format[, firstdayofweek[,
    // firstweekofyear]]])")
    // @Description("Returns a Variant (String) containing an expression
    // formatted according to instructions contained in a format expression.")


    @FunctionName("FormatNumber")
    @Signature(
        "FormatNumber(Expression[,NumDigitsAfterDecimal [,IncludeLeadingDigit "
        + "[,UseParensForNegativeNumbers [,GroupDigits]]]])")
    @Description("Returns an expression formatted as a number.")
    public static String formatNumber(Object expression) {
        return formatNumber(expression, -1);
    }

    @FunctionName("FormatNumber")
    @Signature(
        "FormatNumber(Expression[,NumDigitsAfterDecimal [,IncludeLeadingDigit "
        + "[,UseParensForNegativeNumbers [,GroupDigits]]]])")
    @Description("Returns an expression formatted as a number.")
    public static String formatNumber(
        Object expression,
        int numDigitsAfterDecimal)
    {
        return formatNumber(expression, numDigitsAfterDecimal, -1);
    }

    @FunctionName("FormatNumber")
    @Signature(
        "FormatNumber(Expression[,NumDigitsAfterDecimal [,IncludeLeadingDigit "
        + "[,UseParensForNegativeNumbers [,GroupDigits]]]])")
    @Description("Returns an expression formatted as a number.")
    public static String formatNumber(
        Object expression,
        int numDigitsAfterDecimal,
        int includeLeadingDigit)
    {
        return formatNumber(
            expression,
            numDigitsAfterDecimal,
            includeLeadingDigit,
            -1);
    }

    @FunctionName("FormatNumber")
    @Signature(
        "FormatNumber(Expression[,NumDigitsAfterDecimal [,IncludeLeadingDigit "
        + "[,UseParensForNegativeNumbers [,GroupDigits]]]])")
    @Description("Returns an expression formatted as a number.")
    public static String formatNumber(
        Object expression,
        int numDigitsAfterDecimal,
        int includeLeadingDigit,
        int useParensForNegativeNumbers)
    {
        return formatNumber(
            expression,
            numDigitsAfterDecimal,
            includeLeadingDigit,
            useParensForNegativeNumbers, -1);
    }

    @FunctionName("FormatNumber")
    @Signature(
        "FormatNumber(Expression[,NumDigitsAfterDecimal [,IncludeLeadingDigit "
        + "[,UseParensForNegativeNumbers [,GroupDigits]]]])")
    @Description("Returns an expression formatted as a number.")
    public static String formatNumber(
        Object expression,
        int numDigitsAfterDecimal /* default -1 */,
        int includeLeadingDigit /* default usedefault */,
        int useParensForNegativeNumbers /* default UseDefault */,
        int groupDigits /* default UseDefault */)
    {
        NumberFormat format = NumberFormat.getNumberInstance();
        if (numDigitsAfterDecimal != -1) {
            format.setMaximumFractionDigits(numDigitsAfterDecimal);
            format.setMinimumFractionDigits(numDigitsAfterDecimal);
        }

        if (includeLeadingDigit != -1) {
            if (includeLeadingDigit != 0) {
                // true
                format.setMinimumIntegerDigits(1);
            } else {
                format.setMinimumIntegerDigits(0);
            }
        }

        if (useParensForNegativeNumbers != -1) {
            if (useParensForNegativeNumbers != 0) {
                DecimalFormat dformat = (DecimalFormat)format;
                dformat.setNegativePrefix("(");
                dformat.setNegativeSuffix(")");
            } else {
                DecimalFormat dformat = (DecimalFormat)format;
                dformat.setNegativePrefix(
                    "" + dformat.getDecimalFormatSymbols().getMinusSign());
                dformat.setNegativeSuffix("");
            }
        }

        if (groupDigits != -1) {
            format.setGroupingUsed(groupDigits != 0);
        }

        return format.format(expression);
    }

    @FunctionName("FormatPercent")
    @Signature(
        "FormatPercent(Expression[,NumDigitsAfterDecimal "
        + "[,IncludeLeadingDigit [,UseParensForNegativeNumbers "
        + "[,GroupDigits]]]])")
    @Description(
        "Returns an expression formatted as a percentage (multipled by 100) "
        + "with a trailing % character.")
    public static String formatPercent(Object expression) {
        return formatPercent(expression, -1);
    }

    @FunctionName("FormatPercent")
    @Signature(
        "FormatPercent(Expression[,NumDigitsAfterDecimal "
        + "[,IncludeLeadingDigit [,UseParensForNegativeNumbers "
        + "[,GroupDigits]]]])")
    @Description(
        "Returns an expression formatted as a percentage (multipled by 100) "
        + "with a trailing % character.")
    public static String formatPercent(
        // todo: impl & test
        Object expression, int numDigitsAfterDecimal /* default -1 */)
    {
        return formatPercent(expression, numDigitsAfterDecimal, -1);
    }

    @FunctionName("FormatPercent")
    @Signature(
        "FormatPercent(Expression[,NumDigitsAfterDecimal "
        + "[,IncludeLeadingDigit [,UseParensForNegativeNumbers "
        + "[,GroupDigits]]]])")
    @Description(
        "Returns an expression formatted as a percentage (multipled by 100) "
        + "with a trailing % character.")
    public static String formatPercent(
        // todo: impl & test
        Object expression,
        int numDigitsAfterDecimal /* default -1 */,
        int includeLeadingDigit /* default UseDefault */)
    {
        return formatPercent(
            expression,
            numDigitsAfterDecimal,
            includeLeadingDigit,
            -1);
    }

    @FunctionName("FormatPercent")
    @Signature(
        "FormatPercent(Expression[,NumDigitsAfterDecimal "
        + "[,IncludeLeadingDigit [,UseParensForNegativeNumbers "
        + "[,GroupDigits]]]])")
    @Description(
        "Returns an expression formatted as a percentage (multipled by 100) "
        + "with a trailing % character.")
    public static String formatPercent(
        // todo: impl & test
        Object expression,
        int numDigitsAfterDecimal /* default -1 */,
        int includeLeadingDigit /* default UseDefault */,
        int useParensForNegativeNumbers /* default UseDefault */)
    {
        return formatPercent(
            expression, numDigitsAfterDecimal,
            includeLeadingDigit, useParensForNegativeNumbers, -1);
    }

    @FunctionName("FormatPercent")
    @Signature(
        "FormatPercent(Expression[,NumDigitsAfterDecimal "
        + "[,IncludeLeadingDigit [,UseParensForNegativeNumbers "
        + "[,GroupDigits]]]])")
    @Description(
        "Returns an expression formatted as a percentage (multipled by 100) "
        + "with a trailing % character.")
    public static String formatPercent(
        Object expression,
        int numDigitsAfterDecimal /* default -1 */,
        int includeLeadingDigit /* default UseDefault */,
        int useParensForNegativeNumbers /* default UseDefault */,
        int groupDigits /* default UseDefault */)
    {
        NumberFormat format = NumberFormat.getPercentInstance();
        if (numDigitsAfterDecimal != -1) {
            format.setMaximumFractionDigits(numDigitsAfterDecimal);
            format.setMinimumFractionDigits(numDigitsAfterDecimal);
        }

        if (includeLeadingDigit != -1) {
            if (includeLeadingDigit != 0) {
                // true
                format.setMinimumIntegerDigits(1);
            } else {
                format.setMinimumIntegerDigits(0);
            }
        }

        if (useParensForNegativeNumbers != -1) {
            if (useParensForNegativeNumbers != 0) {
                DecimalFormat dformat = (DecimalFormat)format;
                dformat.setNegativePrefix("(");
                dformat.setNegativeSuffix(
                    "" + dformat.getDecimalFormatSymbols().getPercent() +  ")");
            } else {
                DecimalFormat dformat = (DecimalFormat)format;
                dformat.setNegativePrefix(
                    "" + dformat.getDecimalFormatSymbols().getMinusSign());
                dformat.setNegativeSuffix(
                    "" + dformat.getDecimalFormatSymbols().getPercent());
            }
        }

        if (groupDigits != -1) {
            format.setGroupingUsed(groupDigits != 0);
        }

        return format.format(expression);
    }

    // public Object inStrB(Object start, Object string1, Object string2, int
    // compare /* default BinaryCompare */)

    @FunctionName("InStr")
    @Signature("InStr([start, ]stringcheck, stringmatch[, compare])")
    @Description(
        "Returns a Variant (Long) specifying the position of the first "
        + "occurrence of one string within another.")
    public static int inStr(String stringCheck, String stringMatch) {
        return inStr(1, stringCheck, stringMatch, 0);
    }

    @FunctionName("InStr")
    @Signature("InStr([start, ]stringcheck, stringmatch[, compare])")
    @Description(
        "Returns the position of an occurrence of one string within "
        + "another.")
    public static int inStr(
        int start /* default 1 */,
        String stringCheck,
        String stringMatch)
    {
        return inStr(start, stringCheck, stringMatch, 0);
    }

    @FunctionName("InStr")
    @Signature("InStr([start, ]stringcheck, stringmatch[, compare])")
    @Description(
        "Returns the position of an occurrence of one string within "
        + "another.")
    public static int inStr(
        int start /* default 1 */,
        String stringCheck,
        String stringMatch,
        int compare /* default BinaryCompare */)
    {
        // todo: implement binary vs. text compare
        if (start == 0 || start < -1) {
            throw new InvalidArgumentException(
                "start must be -1 or a location in the string to start");
        }
        if (start != -1) {
            return stringCheck.indexOf(stringMatch, start - 1) + 1;
        } else {
            return stringCheck.indexOf(stringMatch) + 1;
        }
    }

    @FunctionName("InStrRev")
    @Signature("InStrRev(stringcheck, stringmatch[, start[, compare]])")
    @Description(
        "Returns the position of an occurrence of one string within another, "
        + "from the end of string.")
    public static int inStrRev(String stringCheck, String stringMatch) {
        return inStrRev(stringCheck, stringMatch, -1);
    }

    @FunctionName("InStrRev")
    @Signature("InStrRev(stringcheck, stringmatch[, start[, compare]])")
    @Description(
        "Returns the position of an occurrence of one string within another, "
        + "from the end of string.")
    public static int inStrRev(
        String stringCheck,
        String stringMatch,
        int start /* default -1 */)
    {
        return inStrRev(stringCheck, stringMatch, start, 0);
    }

    @FunctionName("InStrRev")
    @Signature("InStrRev(stringcheck, stringmatch[, start[, compare]])")
    @Description(
        "Returns the position of an occurrence of one string within another, "
        + "from the end of string.")
    public static int inStrRev(
        String stringCheck,
        String stringMatch,
        int start /* default -1 */,
        int compare /* default BinaryCompare */)
    {
        // todo: implement binary vs. text compare
        if (start == 0 || start < -1) {
            throw new InvalidArgumentException(
                "start must be -1 or a location in the string to start");
        }
        if (start != -1) {
            return stringCheck.lastIndexOf(stringMatch, start - 1) + 1;
        } else {
            return stringCheck.lastIndexOf(stringMatch) + 1;
        }
    }

    // public String join(Object sourceArray, Object delimiter)

    @FunctionName("LCase")
    @Signature("LCase(string)")
    @Description("Returns a String that has been converted to lowercase.")
    public static String lCase(String string) {
        return string.toLowerCase();
    }

    // public Object lCase$(Object string)
    // public String lTrim$(String string)

    @FunctionName("LTrim")
    @Signature("LTrim(string)")
    @Description(
        "Returns a Variant (String) containing a copy of a specified string "
        + "without leading spaces.")
    public static String lTrim(String string) {
        int i = 0, n = string.length();
        while (i < n) {
            if (string.charAt(i) > ' ') {
                break;
            }
            i++;
        }
        return string.substring(i);
    }

    // public String left$(String string, int length)
    // public String leftB$(String string, int length)
    // public Object leftB(Object string, int length)

    @FunctionName("Left")
    @Signature("Left(string, length)")
    @Description(
        "Returns a specified number of characters from the left side of a "
        + "string.")
    public static String left(String string, int length) {
        final int stringLength = string.length();
        if (length >= stringLength) {
            return string;
        }
        return string.substring(0, length);
    }

    // public Object lenB(Object expression)

    // len is already implemented in BuiltinFunTable... defer

    // @FunctionName("Len")
    // @Signature("Len(String)")
    // @Description("Returns a Long containing the number of characters in a
    // string.")
    // public static int len(String expression) {
    // return expression.length();
    // }

    // public String mid$(String string, int start, Object length)
    // public String midB$(String string, int start, Object length)
    // public Object midB(Object string, int start, Object length)

    @FunctionName("Mid")
    @Signature("Mid(value, beginIndex[, length])")
    @Description("Returns a specified number of characters from a string.")
    public static String mid(String value, int beginIndex) {
        // If we used 'value.length() - beginIndex' as the default value for
        // length, we'd have problems if beginIndex is huge;
        // so 'value.length()' looks like an overestimate - but will always
        // return the correct result.
        final int length = value.length();
        return mid(value, beginIndex, length);
    }

    @FunctionName("Mid")
    @Signature("Mid(value, beginIndex[, length])")
    @Description("Returns a specified number of characters from a string.")
    public static String mid(String value, int beginIndex, int length) {
        // Arguments are 1-based. Spec says that the function gives an error if
        // Start <= 0 or Length < 0.
        if (beginIndex <= 0) {
            throw new InvalidArgumentException(
                "Invalid parameter. "
                + "Start parameter of Mid function must be positive");
        }
        if (length < 0) {
            throw new InvalidArgumentException(
                "Invalid parameter. "
                + "Length parameter of Mid function must be non-negative");
        }

        if (beginIndex > value.length()) {
            return "";
        }

        // Shift from 1-based to 0-based.
        --beginIndex;
        int endIndex = beginIndex + length;
        return endIndex >= value.length() ? value.substring(beginIndex) : value
                .substring(beginIndex, endIndex);
    }

    @FunctionName("MonthName")
    @Signature("MonthName(month, abbreviate)")
    @Description("Returns a string indicating the specified month.")
    public static String monthName(int month, boolean abbreviate) {
        // VB months are 1-based, Java months are 0-based
        --month;
        return (abbreviate ? getDateFormatSymbols().getShortMonths()
                : getDateFormatSymbols().getMonths())[month];
    }

    /**
     * Returns an instance of {@link DateFormatSymbols} for the current locale.
     *
     * <p>
     * Todo: inherit locale from connection.
     *
     * @return a DateFormatSymbols object
     */
    private static DateFormatSymbols getDateFormatSymbols() {
        // We would use DataFormatSymbols.getInstance(), but it is only
        // available from JDK 1.6 onwards.
        return DATE_FORMAT_SYMBOLS;
    }

    // public String rTrim$(String string)

    @FunctionName("RTrim")
    @Signature("RTrim(string)")
    @Description(
        "Returns a Variant (String) containing a copy of a specified string "
        + "without trailing spaces.")
    public static String rTrim(String string) {
        int i = string.length() - 1;
        while (i >= 0) {
            if (string.charAt(i) > ' ') {
                break;
            }
            i--;
        }
        return string.substring(0, i + 1);
    }

    @FunctionName("Replace")
    @Signature(
        "Replace(expression, find, replace[, start[, count[, compare]]])")
    @Description(
        "Returns a string in which a specified substring has been replaced "
        + "with another substring a specified number of times.")
    public static String replace(
        String expression,
        String find,
        String replace,
        int start,
        int count,
        int compare)
    {
        // compare is currently ignored
        Util.discard(compare);
        return _replace(expression, find, replace, start, count);
    }

    @FunctionName("Replace")
    @Signature(
        "Replace(expression, find, replace[, start[, count[, compare]]])")
    @Description(
        "Returns a string in which a specified substring has been replaced "
        + "with another substring a specified number of times.")
    public static String replace(
        String expression,
        String find,
        String replace,
        int start,
        int count)
    {
        return _replace(expression, find, replace, start, count);
    }

    @FunctionName("Replace")
    @Signature(
        "Replace(expression, find, replace[, start[, count[, compare]]])")
    @Description(
        "Returns a string in which a specified substring has been replaced "
        + "with another substring a specified number of times.")
    public static String replace(
        String expression,
        String find,
        String replace,
        int start)
    {
        return _replace(expression, find, replace, start, -1);
    }

    @FunctionName("Replace")
    @Signature(
        "Replace(expression, find, replace[, start[, count[, compare]]])")
    @Description("")
    public static String replace(
        String expression,
        String find,
        String replace)
    {
        // compare is currently ignored
        return _replace(expression, find, replace, 1, -1);
    }

    private static String _replace(
        String expression,
        String find,
        String replace,
        int start /* default 1 */,
        int count /* default -1 */)
    {
        final StringBuilder buf = new StringBuilder(expression);
        int i = 0;
        int pos = start - 1;
        while (true) {
            if (i++ == count) {
                break;
            }
            final int j = buf.indexOf(find, pos);
            if (j == -1) {
                break;
            }
            buf.replace(j, j + find.length(), replace);
            pos = j + replace.length();
        }
        return buf.toString();
    }

    // public String right$(String string, int length)
    // public String rightB$(String string, int length)
    // public Object rightB(Object string, int length)

    @FunctionName("Right")
    @Signature("Right(string, length)")
    @Description(
        "Returns a Variant (String) containing a specified number of "
        + "characters from the right side of a string.")
    public static String right(String string, int length) {
        final int stringLength = string.length();
        if (length >= stringLength) {
            return string;
        }
        return string.substring(stringLength - length, stringLength);
    }

    // public String space$(int number)

    @FunctionName("Space")
    @Signature("Space(number)")
    @Description(
        "Returns a Variant (String) consisting of the specified number of "
        + "spaces.")
    public static String space(int number) {
        return string(number, ' ');
    }

    // public Object split(String expression, Object delimiter, int limit /*
    // default -1 */, int compare /* default BinaryCompare */)

    @FunctionName("StrComp")
    @Signature("StrComp(string1, string2[, compare])")
    @Description(
        "Returns a Variant (Integer) indicating the result of a string "
        + "comparison.")
    public static int strComp(String string1, String string2) {
        return strComp(string1, string2, 0);
    }

    @FunctionName("StrComp")
    @Signature("StrComp(string1, string2[, compare])")
    @Description(
        "Returns a Variant (Integer) indicating the result of a string "
        + "comparison.")
    public static int strComp(
        String string1,
        String string2,
        int compare /* default BinaryCompare */)
    {
        // Note: compare is currently ignored
        // Wrapper already checked whether args are null
        assert string1 != null;
        assert string2 != null;
        return string1.compareTo(string2);
    }

    // public Object strConv(Object string, StrConv conversion, int localeID)

    @FunctionName("StrReverse")
    @Signature("StrReverse(string)")
    @Description(
        "Returns a string in which the character order of a specified string "
        + "is reversed.")
    public static String strReverse(String expression) {
        final char[] chars = expression.toCharArray();
        for (int i = 0, j = chars.length - 1; i < j; i++, j--) {
            char c = chars[i];
            chars[i] = chars[j];
            chars[j] = c;
        }
        return new String(chars);
    }

    // public String string$(int number, Object character)

    @FunctionName("String")
    @Signature("String(number, character)")
    @Description("")
    public static String string(int number, char character) {
        if (character == 0) {
            return "";
        }
        final char[] chars = new char[number];
        Arrays.fill(chars, (char) (character % 256));
        return new String(chars);
    }

    // public String trim$(String string)

    @FunctionName("Trim")
    @Signature("Trim(string)")
    @Description(
        "Returns a Variant (String) containing a copy of a specified string "
        + "without leading and trailing spaces.")
    public static String trim(String string) {
        // JDK has a method for trim, but not ltrim or rtrim
        return string.trim();
    }

    // ucase is already implemented in BuiltinFunTable... defer

    // public String uCase$(String string)

//    @FunctionName("UCase")
//    @Signature("UCase(string)")
//    @Description("Returns a String that has been converted to uppercase.")
//    public String uCase(String string) {
//        return string.toUpperCase();
//    }

    // TODO: should use connection's locale to determine first day of week,
    // not the JVM's default

    @FunctionName("WeekdayName")
    @Signature("WeekdayName(weekday, abbreviate, firstdayofweek)")
    @Description("Returns a string indicating the specified day of the week.")
    public static String weekdayName(
        int weekday,
        boolean abbreviate,
        int firstDayOfWeek)
    {
        // Java and VB agree: SUNDAY = 1, ... SATURDAY = 7
        final Calendar calendar = Calendar.getInstance();
        if (firstDayOfWeek == 0) {
            firstDayOfWeek = calendar.getFirstDayOfWeek();
        }
        // compensate for start of week
        weekday += (firstDayOfWeek - 1);
        // bring into range 1..7
        weekday = (weekday - 1) % 7 + 1;
        if (weekday <= 0) {
            // negative numbers give negative modulo
            weekday += 7;
        }
        return
            (abbreviate
             ? getDateFormatSymbols().getShortWeekdays()
             : getDateFormatSymbols().getWeekdays())
            [weekday];
    }

    // Misc

    // public Object array(Object argList)
    // public String input$(int number, int fileNumber)
    // public String inputB$(int number, int fileNumber)
    // public Object inputB(int number, int fileNumber)
    // public Object input(int number, int fileNumber)
    // public void width(int fileNumber, int width)

    // ~ Inner classes

    private enum Interval {
        yyyy("Year", Calendar.YEAR),
        q("Quarter", -1),
        m("Month", Calendar.MONTH),
        y("Day of year", Calendar.DAY_OF_YEAR),
        d("Day", Calendar.DAY_OF_MONTH),
        w("Weekday", Calendar.DAY_OF_WEEK),
        ww("Week", Calendar.WEEK_OF_YEAR),
        h("Hour", Calendar.HOUR_OF_DAY),
        n("Minute", Calendar.MINUTE),
        s("Second", Calendar.SECOND);

        private final int dateField;

        Interval(String desc, int dateField) {
            Util.discard(desc);
            this.dateField = dateField;
        }

        void add(Calendar calendar, int amount) {
            switch (this) {
            case q:
                calendar.add(Calendar.MONTH, amount * 3);
                break;
            default:
                calendar.add(dateField, amount);
                break;
            }
        }

        Calendar floor(Calendar calendar) {
            Calendar calendar2 = Calendar.getInstance();
            calendar2.setTime(calendar.getTime());
            floorInplace(calendar2);
            return calendar2;
        }

        private void floorInplace(Calendar calendar) {
            switch (this) {
            case yyyy:
                calendar.set(Calendar.DAY_OF_YEAR, 1);
                d.floorInplace(calendar);
                break;
            case q:
                int month = calendar.get(Calendar.MONTH);
                month -= month % 3;
                calendar.set(Calendar.MONTH, month);
                calendar.set(Calendar.DAY_OF_MONTH, 1);
                d.floorInplace(calendar);
                break;
            case m:
                calendar.set(Calendar.DAY_OF_MONTH, 1);
                d.floorInplace(calendar);
                break;
            case w:
                final int dow = calendar.get(Calendar.DAY_OF_WEEK);
                final int firstDayOfWeek = calendar.getFirstDayOfWeek();
                if (dow == firstDayOfWeek) {
                    // nothing to do
                } else if (dow > firstDayOfWeek) {
                    final int roll = firstDayOfWeek - dow;
                    assert roll < 0;
                    calendar.roll(Calendar.DAY_OF_WEEK, roll);
                } else {
                    final int roll = firstDayOfWeek - dow - 7;
                    assert roll < 0;
                    calendar.roll(Calendar.DAY_OF_WEEK, roll);
                }
                d.floorInplace(calendar);
                break;
            case y:
            case d:
                calendar.set(Calendar.HOUR_OF_DAY, 0);
                calendar.set(Calendar.MINUTE, 0);
                calendar.set(Calendar.SECOND, 0);
                calendar.set(Calendar.MILLISECOND, 0);
                break;
            case h:
                calendar.set(Calendar.MINUTE, 0);
                calendar.set(Calendar.SECOND, 0);
                calendar.set(Calendar.MILLISECOND, 0);
                break;
            case n:
                calendar.set(Calendar.SECOND, 0);
                calendar.set(Calendar.MILLISECOND, 0);
                break;
            case s:
                calendar.set(Calendar.MILLISECOND, 0);
                break;
            }
        }

        int diff(Calendar calendar1, Calendar calendar2, int firstDayOfWeek) {
            switch (this) {
            case q:
                return m.diff(calendar1, calendar2, firstDayOfWeek) / 3;
            default:
                return floor(calendar1).get(dateField)
                        - floor(calendar2).get(dateField);
            }
        }

        int datePart(Calendar calendar) {
            switch (this) {
            case q:
                return (m.datePart(calendar) + 2) / 3;
            case m:
                return calendar.get(dateField) + 1;
            case w:
                int dayOfWeek = calendar.get(dateField);
                dayOfWeek -= (calendar.getFirstDayOfWeek() - 1);
                dayOfWeek = dayOfWeek % 7;
                if (dayOfWeek <= 0) {
                    dayOfWeek += 7;
                }
                return dayOfWeek;
            default:
                return calendar.get(dateField);
            }
        }
    }

    private enum FirstWeekOfYear {
        vbUseSystem(
            0, "Use the NLS API setting."),

        vbFirstJan1(
            1,
            "Start with week in which January 1 occurs (default)."),

        vbFirstFourDays(
            2,
            "Start with the first week that has at least four days in the new year."),

        vbFirstFullWeek(
            3,
            "Start with first full week of the year.");

        FirstWeekOfYear(int code, String desc) {
            assert code == ordinal();
            assert desc != null;
        }

        void apply(Calendar calendar) {
            switch (this) {
            case vbUseSystem:
                break;
            case vbFirstJan1:
                calendar.setMinimalDaysInFirstWeek(1);
                break;
            case vbFirstFourDays:
                calendar.setMinimalDaysInFirstWeek(4);
                break;
            case vbFirstFullWeek:
                calendar.setMinimalDaysInFirstWeek(7);
                break;
            }
        }
    }
}

// End Vba.java
