/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2000-2005 Julian Hyde
// Copyright (C) 2005-2018 Hitachi Vantara..
// All Rights Reserved.
//
// jhyde, 2 November, 2000
*/

package mondrian.util;

import mondrian.olap.Util;

import java.io.PrintWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.*;
import java.util.*;

/**
 * <code>Format</code> formats numbers, strings and dates according to the
 * same specification as Visual Basic's
 * <code>format()</code> function.  This function is described in more detail
 * <a href="http://www.apostate.com/programming/vb-format.html">here</a>.  We
 * have made the following enhancements to this specification:<ul>
 *
 * <li>if the international currency symbol (&#x00a4;) occurs in a format
 *   string, it is translated to the locale's currency symbol.</li>
 *
 * <li>the format string "Currency" is translated to the locale's currency
 *   format string. Negative currency values appear in parentheses.</li>
 *
 * <li>the string "USD" (abbreviation for U.S. Dollars) may occur in a format
 *   string.</li>
 *
 * </ul>
 *
 * <p>One format object can be used to format multiple values, thereby
 * amortizing the time required to parse the format string.  Example:</p>
 *
 * <pre><code>
 * double[] values;
 * Format format = new Format("##,##0.###;(##,##0.###);;Nil");
 * for (int i = 0; i < values.length; i++) {
 *   System.out.println("Value #" + i + " is " + format.format(values[i]));
 * }
 * </code></pre>
 *
 * <p>Still to be implemented:<ul>
 *
 * <li>String formatting (fill from left/right)</li>
 *
 * <li>Use client's timezone for printing times.</li>
 *
 * </ul>
 *
 * @author jhyde
 */
public class Format {
    private String formatString;
    private BasicFormat format;
    private FormatLocale locale;

    /**
     * Maximum number of entries in the format cache used by
     * {@link #get(String, java.util.Locale)}.
     */
    public static final int CacheLimit = 1000;

    /**
     * Maps (formatString, locale) pairs to {@link Format} objects.
     *
     * <p>If the number of entries in the cache exceeds 1000,
     */
    private static final Map<String, Format> cache =
        new LinkedHashMap<String, Format>() {
            public boolean removeEldestEntry(Map.Entry<String, Format> entry) {
                return size() > CacheLimit;
            }
        };

    static final char thousandSeparator_en = ',';
    static final char decimalPlaceholder_en = '.';
    static final String dateSeparator_en = "/";
    static final String timeSeparator_en = ":";
    static final String currencySymbol_en = "$";
    static final String currencyFormat_en = "$#,##0.00";
    static final String[] daysOfWeekShort_en = {
        "", "Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"
    };
    static final String[] daysOfWeekLong_en = {
        "", "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday",
        "Saturday"
    };
    static final String[] monthsShort_en = {
        "Jan", "Feb", "Mar", "Apr", "May", "Jun",
        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec", "",
    };
    static final String[] monthsLong_en = {
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December", "",
    };
    static final char intlCurrencySymbol = '\u00a4';

    /**
     * Maps strings representing locales (for example, "en_US_Boston", "en_US",
     * "en", or "" for the default) to a {@link Format.FormatLocale}.
     */
    private static final Map<String, FormatLocale> mapLocaleToFormatLocale =
        new HashMap<String, FormatLocale>();

    /**
     * Cache of parsed format strings and their thousand separator
     * tokens length. Used so we don't have to tokenize a format string
     * over and over again.
     */
    private static final Map<String, ArrayStack<Integer>>
        thousandSeparatorTokenMap = new HashMap<String, ArrayStack<Integer>>();

    /**
     * Locale for US English, also the default for English and for all
     * locales.
     */
    static final FormatLocale locale_US = createLocale(
        '\0', '\0', null, null, null, null, null, null, null, null,
        Locale.US);

    static class Token {
        final int code;
        final int flags;
        final String token;
        final FormatType formatType;

        Token(int code, int flags, String token)
        {
            this.code = code;
            this.flags = flags;
            this.token = token;
            this.formatType =
                isNumeric() ? FormatType.NUMERIC
                    : isDate() ? FormatType.DATE
                        : isString() ? FormatType.STRING
                        : null;
        }

        boolean compatibleWith(FormatType formatType)
        {
            return formatType == null
                || this.formatType == null
                || formatType == this.formatType;
        }

        boolean isSpecial()
        {
            return (flags & SPECIAL) == SPECIAL;
        }

        boolean isNumeric()
        {
            return (flags & NUMERIC) == NUMERIC;
        }

        boolean isDate()
        {
            return (flags & DATE) == DATE;
        }

        boolean isString()
        {
            return (flags & STRING) == STRING;
        }

        FormatType getFormatType() {
            return formatType;
        }

        BasicFormat makeFormat(FormatLocale locale)
        {
            if (isDate()) {
                return new DateFormat(code, token, locale, false);
            } else if (isNumeric()) {
                return new LiteralFormat(code, token);
            } else {
                return new LiteralFormat(token);
            }
        }
    }

    /**
     * BasicFormat is the interface implemented by the classes which do all
     * the work.  Whereas {@link Format} has only one method for formatting,
     * {@link Format#format(Object)}, this class provides methods for several
     * primitive types.  To make it easy to combine formatting objects, all
     * methods write to a {@link PrintWriter}.
     *
     * <p>The base implementation of most of these methods throws; there
     * is no requirement that a derived class implements all of these methods.
     * It is up to {@link Format#parseFormatString} to ensure that, for
     * example, the {@link #format(double,StringBuilder)} method is
     * never called for {@link DateFormat}.
     */
    static class BasicFormat {
        final int code;

        BasicFormat() {
            this(0);
        }

        BasicFormat(int code) {
            this.code = code;
        }

        FormatType getFormatType() {
            return null;
        }

        void formatNull(StringBuilder buf) {
            // SSAS formats null values as the empty string. However, SQL Server
            // Management Studio's pivot table formats them as "(null)", so many
            // people believe that this is the server's behavior.
        }

        void format(double d, StringBuilder buf) {
            throw new Error();
        }

        void format(long n, StringBuilder buf) {
            throw new Error();
        }

        void format(String s, StringBuilder buf) {
            throw new Error();
        }

        void format(Date date, StringBuilder buf) {
            Calendar calendar = Calendar.getInstance(); // todo: use locale
            calendar.setTime(date);
            format(calendar, buf);
        }

        void format(BigDecimal bigDecimal, StringBuilder buf) {
            format(bigDecimal.doubleValue(), buf);
        }

        void format(Calendar calendar, StringBuilder buf) {
            throw new Error();
        }

        /**
         * Returns whether this format can handle a given value.
         *
         * <p>Usually returns true;
         * one notable exception is a format for negative numbers which
         * causes the number to be underflow to zero and therefore be
         * ineligible for the negative format.
         *
         * @param n value
         * @return Whether this format is applicable for a given value
         */
        boolean isApplicableTo(double n) {
            return true;
        }

        /**
         * Returns whether this format can handle a given value.
         *
         * <p>Usually returns true;
         * one notable exception is a format for negative numbers which
         * causes the number to be underflow to zero and therefore be
         * ineligible for the negative format.
         *
         * @param n value
         * @return Whether this format is applicable for a given value
         */
        boolean isApplicableTo(long n) {
            return true;
        }
    }

    /**
     * AlternateFormat is an implementation of {@link Format.BasicFormat} which
     * allows a different format to be used for different kinds of values.  If
     * there are 4 formats, purposes are as follows:<ol>
     * <li>positive numbers</li>
     * <li>negative numbers</li>
     * <li>zero</li>
     * <li>null values</li>
     * </ol>
     *
     * <p>If there are fewer than 4 formats, the first is used as a fall-back.
     * See the <a href="http://apostate.com/vb-format-syntax">the
     * visual basic format specification</a> for more details.
     */
    static class AlternateFormat extends BasicFormat {
        final BasicFormat[] formats;
        final JavaFormat javaFormat;

        AlternateFormat(BasicFormat[] formats, FormatLocale locale)
        {
            this.formats = formats;
            assert formats.length >= 1;
            this.javaFormat = new JavaFormat(locale.locale);
        }

        void formatNull(StringBuilder buf) {
            if (formats.length >= 4) {
                formats[3].format(0, buf);
            } else {
                super.formatNull(buf);
            }
        }

        void format(double n, StringBuilder buf) {
            int i;
            if (n == 0
                && formats.length >= 3
                && formats[2] != null)
            {
                i = 2;
            } else if (n < 0) {
                if (formats.length >= 2
                    && formats[1] != null)
                {
                    if (formats[1].isApplicableTo(n)) {
                        n = -n;
                        i = 1;
                    } else {
                        // Does not fit into the negative mask, so use the
                        // nil mask, if there is one. For example,
                        // "#.0;(#.0);Nil" formats -0.0001 as "Nil".
                        if (formats.length >= 3
                            && formats[2] != null)
                        {
                            i = 2;
                        } else {
                            i = 0;
                        }
                    }
                } else {
                    i = 0;
                    if (formats[0].isApplicableTo(n)) {
                        if (!Bug.BugMondrian687Fixed) {
                            // Special case for format strings with style,
                            // like "|#|style='red'". JPivot expects the
                            // '-' to immediately precede the digits, viz
                            // "|-6|style='red'|", not "-|6|style='red'|".
                            // This is not consistent with SSAS 2005, hence
                            // the bug.
                            //
                            // But for other formats, we want '-' to precede
                            // literals, viz '-$6' not '$-6'. This is SSAS
                            // 2005's behavior too.
                            int size = buf.length();
                            buf.append('-');
                            n = -n;
                            formats[i].format(n, buf);
                            if (buf.substring(size, size + 2).equals(
                                    "-|"))
                            {
                                buf.setCharAt(size, '|');
                                buf.setCharAt(size + 1, '-');
                            }
                            return;
                        }
                        buf.append('-');
                        n = -n;
                    } else {
                        n = 0;
                    }
                }
            } else {
                i = 0;
            }
            formats[i].format(n, buf);
        }

        void format(long n, StringBuilder buf) {
            int i;
            if (n == 0
                && formats.length >= 3
                && formats[2] != null)
            {
                i = 2;
            } else if (n < 0) {
                if (formats.length >= 2
                    && formats[1] != null)
                {
                    if (formats[1].isApplicableTo(n)) {
                        n = -n;
                        i = 1;
                    } else {
                        // Does not fit into the negative mask, so use the
                        // nil mask, if there is one. For example,
                        // "#.0;(#.0);Nil" formats -0.0001 as "Nil".
                        if (formats.length >= 3
                            && formats[2] != null)
                        {
                            i = 2;
                        } else {
                            i = 0;
                        }
                    }
                } else {
                    i = 0;
                    if (formats[0].isApplicableTo(n)) {
                        if (!Bug.BugMondrian687Fixed) {
                            // Special case for format strings with style,
                            // like "|#|style='red'". JPivot expects the
                            // '-' to immediately precede the digits, viz
                            // "|-6|style='red'|", not "-|6|style='red'|".
                            // This is not consistent with SSAS 2005, hence
                            // the bug.
                            //
                            // But for other formats, we want '-' to precede
                            // literals, viz '-$6' not '$-6'. This is SSAS
                            // 2005's behavior too.
                            final int size = buf.length();
                            buf.append('-');
                            n = -n;
                            formats[i].format(n, buf);
                            if (buf.substring(size, size + 2).equals(
                                    "-|"))
                            {
                                buf.setCharAt(size, '|');
                                buf.setCharAt(size + 1, '-');
                            }
                            return;
                        }
                        buf.append('-');
                        n = -n;
                    } else {
                        n = 0;
                    }
                }
            } else {
                i = 0;
            }
            formats[i].format(n, buf);
        }

        void format(String s, StringBuilder buf) {
            // since it is not a number, ignore all format strings
            buf.append(s);
        }

        void format(Calendar calendar, StringBuilder buf) {
            // We're passing a date to a numeric format string. Convert it to
            // the number of days since 1900.
            BigDecimal bigDecimal = daysSince1900(calendar);

            // since it is not a number, ignore all format strings
            format(bigDecimal.doubleValue(), buf);
        }

        private static BigDecimal daysSince1900(Calendar calendar) {
            final long dayOfYear = calendar.get(Calendar.DAY_OF_YEAR);
            final long year = calendar.get(Calendar.YEAR);
            long yearForLeap = year;
            if (calendar.get(Calendar.MONTH) < 2) {
                --yearForLeap;
            }
            final long leapDays =
                (yearForLeap - 1900) / 4
                - (yearForLeap - 1900) / 100
                + (yearForLeap - 2000) / 400;
            final long days =
                (year - 1900) * 365
                + leapDays
                + dayOfYear
                + 2; // kludge factor to agree with Excel
            final long millis =
                calendar.get(Calendar.HOUR_OF_DAY) * 3600000
                + calendar.get(Calendar.MINUTE) * 60000
                + calendar.get(Calendar.SECOND) * 1000
                + calendar.get(Calendar.MILLISECOND);
            return BigDecimal.valueOf(days).add(
                BigDecimal.valueOf(millis).divide(
                    BigDecimal.valueOf(86400000),
                    8,
                    BigDecimal.ROUND_FLOOR));
        }
    }

    /**
     * LiteralFormat is an implementation of {@link Format.BasicFormat} which
     * prints a constant value, regardless of the value to be formatted.
     *
     * @see CompoundFormat
     */
    static class LiteralFormat extends BasicFormat
    {
        String s;

        LiteralFormat(String s)
        {
            this(FORMAT_LITERAL, s);
        }

        LiteralFormat(int code, String s)
        {
            super(code);
            this.s = s;
        }

        void format(double d, StringBuilder buf) {
            buf.append(s);
        }

        void format(long n, StringBuilder buf) {
            buf.append(s);
        }

        void format(String str, StringBuilder buf) {
            buf.append(s);
        }

        void format(Date date, StringBuilder buf) {
            buf.append(s);
        }

        void format(Calendar calendar, StringBuilder buf) {
            buf.append(s);
        }
    }

    /**
     * CompoundFormat is an implementation of {@link Format.BasicFormat} where
     * each value is formatted by applying a sequence of format elements.  Each
     * format element is itself a format.
     *
     * @see AlternateFormat
     */
    static class CompoundFormat extends BasicFormat
    {
        final BasicFormat[] formats;
        CompoundFormat(BasicFormat[] formats)
        {
            this.formats = formats;
            assert formats.length >= 2;
        }

        void format(double v, StringBuilder buf) {
            for (int i = 0; i < formats.length; i++) {
                formats[i].format(v, buf);
            }
        }

        void format(long v, StringBuilder buf) {
            for (int i = 0; i < formats.length; i++) {
                formats[i].format(v, buf);
            }
        }

        void format(String v, StringBuilder buf) {
            for (int i = 0; i < formats.length; i++) {
                formats[i].format(v, buf);
            }
        }

        void format(Date v, StringBuilder buf) {
            for (int i = 0; i < formats.length; i++) {
                formats[i].format(v, buf);
            }
        }

        void format(Calendar v, StringBuilder buf) {
            for (int i = 0; i < formats.length; i++) {
                formats[i].format(v, buf);
            }
        }

        boolean isApplicableTo(double n) {
            for (int i = 0; i < formats.length; i++) {
                if (!formats[i].isApplicableTo(n)) {
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * JavaFormat is an implementation of {@link Format.BasicFormat} which
     * prints values using Java's default formatting for their type.
     * <code>null</code> values appear as an empty string.
     */
    static class JavaFormat extends BasicFormat
    {
        private final NumberFormat numberFormat;
        private final java.text.DateFormat dateFormat;

        JavaFormat(Locale locale)
        {
            this.numberFormat = NumberFormat.getNumberInstance(locale);
            this.dateFormat =
                java.text.DateFormat.getDateTimeInstance(
                    java.text.DateFormat.SHORT,
                    java.text.DateFormat.MEDIUM,
                    locale);
        }

        // No need to override format(Object,PrintWriter) or
        // format(Date,PrintWriter).

        void format(double d, StringBuilder buf) {
            // NOTE (jhyde, 2006/12/1): We'd use
            // NumberFormat(double,StringBuilder,FieldPosition) if it existed.
            buf.append(numberFormat.format(d));
        }

        void format(BigDecimal d, StringBuilder buf) {
            buf.append(numberFormat.format(d));
        }

        void format(long n, StringBuilder buf) {
            // NOTE (jhyde, 2006/12/1): We'd use
            // NumberFormat(long,StringBuilder,FieldPosition) if it existed.
            buf.append(numberFormat.format(n));
        }

        void format(String s, StringBuilder buf) {
            buf.append(s);
        }

        void format(Calendar calendar, StringBuilder buf) {
            // NOTE (jhyde, 2006/12/1): We'd use
            // NumberFormat(Date,StringBuilder,FieldPosition) if it existed.
            buf.append(dateFormat.format(calendar.getTime()));
        }
    }

    /**
     * FallbackFormat catches un-handled datatypes and prints the original
     * format string.  Better than giving an error.  Abstract base class for
     * NumericFormat and DateFormat.
     */
    static abstract class FallbackFormat extends BasicFormat
    {
        final String token;

        FallbackFormat(int code, String token)
        {
            super(code);
            this.token = token;
        }

        void format(double d, StringBuilder buf) {
            buf.append(token);
        }

        void format(long n, StringBuilder buf) {
            buf.append(token);
        }

        void format(String s, StringBuilder buf) {
            buf.append(token);
        }

        void format(Calendar calendar, StringBuilder buf) {
            buf.append(token);
        }
    }

    /**
     * NumericFormat is an implementation of {@link Format.BasicFormat} which
     * prints numbers with a given number of decimal places, leading zeroes, in
     * exponential notation, etc.
     *
     * <p>It is implemented using {@link MondrianFloatingDecimal}.
     */
    static class NumericFormat extends JavaFormat
    {
        final FormatLocale locale;
        final int digitsLeftOfPoint;
        final int zeroesLeftOfPoint;
        final int digitsRightOfPoint;
        final int zeroesRightOfPoint;
        final int digitsRightOfExp;
        final int zeroesRightOfExp;

        /**
         * Number of decimal places to shift the number left before
         * formatting it: 2 means multiply by 100; -3 means divide by
         * 1000.
         */
        int decimalShift;
        final char expChar;
        final boolean expSign;
        final boolean useDecimal;
        final boolean useThouSep;

        final ArrayStack<Integer> cachedThousandSeparatorPositions;

        NumericFormat(
            String token,
            FormatLocale locale,
            int expFormat,
            int digitsLeftOfPoint, int zeroesLeftOfPoint,
            int digitsRightOfPoint, int zeroesRightOfPoint,
            int digitsRightOfExp, int zeroesRightOfExp,
            boolean useDecimal, boolean useThouSep,
            String formatString)
        {
            super(locale.locale);
            this.locale = locale;
            switch (expFormat) {
            case FORMAT_E_MINUS_UPPER:
                this.expChar = 'E';
                this.expSign = false;
                break;
            case FORMAT_E_PLUS_UPPER:
                this.expChar = 'E';
                this.expSign = true;
                break;
            case FORMAT_E_MINUS_LOWER:
                this.expChar = 'e';
                this.expSign = false;
                break;
            case FORMAT_E_PLUS_LOWER:
                this.expChar = 'e';
                this.expSign = true;
                break;
            default:
                this.expChar = 0;
                this.expSign = false;
            }
            this.digitsLeftOfPoint = digitsLeftOfPoint;
            this.zeroesLeftOfPoint = zeroesLeftOfPoint;
            this.digitsRightOfPoint = digitsRightOfPoint;
            this.zeroesRightOfPoint = zeroesRightOfPoint;
            this.digitsRightOfExp = digitsRightOfExp;
            this.zeroesRightOfExp = zeroesRightOfExp;
            this.useDecimal = useDecimal;
            this.useThouSep = useThouSep;
            this.decimalShift = 0; // set later

            // Check if we're dealing with a format macro token rather than
            // an actual format string.
            formatString = MacroToken.expand(locale, formatString);

            if (thousandSeparatorTokenMap.containsKey(formatString)) {
                cachedThousandSeparatorPositions =
                    thousandSeparatorTokenMap.get(formatString);
            } else {
                // To provide backwards compatibility, we apply the old
                // formatting rules if there are less than 2 thousand
                // separators in the format string.
                String formatStringBuffer = formatString;

                // If the format includes a negative format part, we strip it.
                final int semiPos =
                    formatStringBuffer.indexOf(getFormatToken(FORMAT_SEMI));
                if (semiPos > 0) {
                    formatStringBuffer =
                        formatStringBuffer.substring(0, semiPos);
                }

                final int nbThousandSeparators =
                    countOccurrences(
                        formatStringBuffer,
                        getFormatToken(FORMAT_THOUSEP).charAt(0));
                cachedThousandSeparatorPositions = new ArrayStack<Integer>();
                if (nbThousandSeparators > 1) {
                    // Extract the whole part of the format string
                    final int decimalPos =
                        formatStringBuffer.indexOf(
                            getFormatToken(FORMAT_DECIMAL));
                    final int endIndex =
                        decimalPos == -1
                            ? formatStringBuffer.length()
                            : decimalPos;
                        final String wholeFormat =
                            formatStringBuffer.substring(0, endIndex);

                        // Tokenize it so we can analyze it's structure
                        final StringTokenizer st =
                            new StringTokenizer(
                                wholeFormat,
                                String.valueOf(
                                    getFormatToken(FORMAT_THOUSEP)));

                        // We ignore the first token.
                        // ie: #,###,###
                        st.nextToken();

                        // Now we build a list of the token lengths in
                        // reverse order. The last one in the reversed
                        // list will be re-applied if the number is
                        // longer than the format string.
                        while (st.hasMoreTokens()) {
                            cachedThousandSeparatorPositions.push(
                                st.nextToken().length());
                        }
                } else if (nbThousandSeparators == 1) {
                    // Use old style formatting.
                    cachedThousandSeparatorPositions.add(3);
                }
                thousandSeparatorTokenMap.put(
                    formatString, cachedThousandSeparatorPositions);
            }
        }

        FormatType getFormatType() {
            return FormatType.NUMERIC;
        }

        private ArrayStack<Integer> getThousandSeparatorPositions() {
            // Defensive copy
            return new ArrayStack<Integer>(cachedThousandSeparatorPositions);
        }

        private int countOccurrences(final String s, final char c) {
            final char[] chars = s.toCharArray();
            int count = 0;
            for (int i = 0; i < chars.length; i++) {
                if (chars[i] == c) {
                    count++;
                }
            }
            return count;
        }

        void format(double n, StringBuilder buf)
        {
            MondrianFloatingDecimal fd = new MondrianFloatingDecimal(n);
            shift(fd, decimalShift);
            final int formatDigitsRightOfPoint =
                zeroesRightOfPoint + digitsRightOfPoint;
            if (n == 0.0 || (n < 0 && !shows(fd, formatDigitsRightOfPoint))) {
                // Underflow of negative number. Make it zero, so there is no
                // '-' sign.
                fd = new MondrianFloatingDecimal(0);
            }
            formatFd0(
                fd,
                buf,
                zeroesLeftOfPoint,
                locale.decimalPlaceholder,
                zeroesRightOfPoint,
                formatDigitsRightOfPoint,
                expChar,
                expSign,
                zeroesRightOfExp,
                useThouSep ? locale.thousandSeparator : '\0',
                useDecimal,
                getThousandSeparatorPositions());
        }

        boolean isApplicableTo(double n) {
            if (n >= 0) {
                return true;
            }
            MondrianFloatingDecimal fd = new MondrianFloatingDecimal(n);
            shift(fd, decimalShift);
            final int formatDigitsRightOfPoint =
                zeroesRightOfPoint + digitsRightOfPoint;
            return shows(fd, formatDigitsRightOfPoint);
        }

        private static boolean shows(
            MondrianFloatingDecimal fd,
            int formatDigitsRightOfPoint)
        {
            final int i0 = - fd.decExponent - formatDigitsRightOfPoint;
            if (i0 < 0) {
                return true;
            }
            if (i0 > 0) {
                return false;
            }
            if (fd.digits[0] >= '5') {
                return true;
            }
            return false;
        }

        void format(long n, StringBuilder buf)
        {
            MondrianFloatingDecimal fd =
                new MondrianFloatingDecimal(n);
            shift(fd, decimalShift);
            formatFd0(
                fd,
                buf,
                zeroesLeftOfPoint,
                locale.decimalPlaceholder,
                zeroesRightOfPoint,
                zeroesRightOfPoint + digitsRightOfPoint,
                expChar,
                expSign,
                zeroesRightOfExp,
                useThouSep ? locale.thousandSeparator : '\0',
                useDecimal,
                getThousandSeparatorPositions());
        }
    }

    /**
     * DateFormat is an element of a {@link Format.CompoundFormat} which has a
     * value when applied to a {@link Calendar} object.  (Values of type {@link
     * Date} are automatically converted into {@link Calendar}s when you call
     * {@link Format.BasicFormat#format(Date, StringBuilder)} calls
     * to format other kinds of values give a runtime error.)
     *
     * <p>In a typical use of this class, a format string such as "m/d/yy" is
     * parsed into DateFormat objects for "m", "d", and "yy", and {@link
     * LiteralFormat} objects for "/".  A {@link Format.CompoundFormat} object
     * is created to bind them together.
     */
    static class DateFormat extends FallbackFormat
    {
        FormatLocale locale;
        boolean twelveHourClock;

        DateFormat(
            int code, String s, FormatLocale locale, boolean twelveHourClock)
        {
            super(code, s);
            this.locale = locale;
            this.twelveHourClock = twelveHourClock;
        }

        FormatType getFormatType() {
            return FormatType.DATE;
        }

        void setTwelveHourClock(boolean twelveHourClock)
        {
            this.twelveHourClock = twelveHourClock;
        }

        void format(Calendar calendar, StringBuilder buf)
        {
            format(code, calendar, buf);
        }

        private void format(
            int code,
            Calendar calendar,
            StringBuilder buf)
        {
            switch (code) {
            case FORMAT_C:
            {
                boolean dateSet = !(
                    calendar.get(Calendar.DAY_OF_YEAR) == 0
                    && calendar.get(Calendar.YEAR) == 0);
                boolean timeSet = !(
                    calendar.get(Calendar.SECOND) == 0
                    &&  calendar.get(Calendar.MINUTE) == 0
                    && calendar.get(Calendar.HOUR) == 0);
                if (dateSet) {
                    format(FORMAT_DDDDD, calendar, buf);
                }
                if (dateSet && timeSet) {
                    buf.append(' ');
                }
                if (timeSet) {
                    format(FORMAT_TTTTT, calendar, buf);
                }
                break;
            }
            case FORMAT_D:
            {
                int d = calendar.get(Calendar.DAY_OF_MONTH);
                buf.append(d);
                break;
            }
            case FORMAT_DD:
            {
                int d = calendar.get(Calendar.DAY_OF_MONTH);
                if (d < 10) {
                    buf.append('0');
                }
                buf.append(d);
                break;
            }
            case FORMAT_DDD:
            {
                int dow = calendar.get(Calendar.DAY_OF_WEEK);
                buf.append(locale.daysOfWeekShort[dow]); // e.g. Sun
                break;
            }
            case FORMAT_DDDD:
            {
                int dow = calendar.get(Calendar.DAY_OF_WEEK);
                buf.append(locale.daysOfWeekLong[dow]); // e.g. Sunday
                break;
            }
            case FORMAT_DDDDD:
            {
                // Officially, we should use the system's short date
                // format. But for now, we always print using the default
                // format, m/d/yy.
                format(FORMAT_M, calendar, buf);
                buf.append(locale.dateSeparator);
                format(FORMAT_D, calendar, buf);
                buf.append(locale.dateSeparator);
                format(FORMAT_YY, calendar, buf);
                break;
            }
            case FORMAT_DDDDDD:
            {
                format(FORMAT_MMMM_UPPER, calendar, buf);
                buf.append(" ");
                format(FORMAT_DD, calendar, buf);
                buf.append(", ");
                format(FORMAT_YYYY, calendar, buf);
                break;
            }
            case FORMAT_W:
            {
                int dow = calendar.get(Calendar.DAY_OF_WEEK);
                buf.append(dow);
                break;
            }
            case FORMAT_WW:
            {
                int woy = calendar.get(Calendar.WEEK_OF_YEAR);
                buf.append(woy);
                break;
            }
            case FORMAT_M:
            case FORMAT_M_UPPER:
            {
                int m = calendar.get(Calendar.MONTH) + 1; // 0-based
                buf.append(m);
                break;
            }
            case FORMAT_MM:
            case FORMAT_MM_UPPER:
            {
                int mm = calendar.get(Calendar.MONTH) + 1; // 0-based
                if (mm < 10) {
                    buf.append('0');
                }
                buf.append(mm);
                break;
            }
            case FORMAT_MMM_LOWER:
            case FORMAT_MMM_UPPER:
            {
                int m = calendar.get(Calendar.MONTH); // 0-based
                buf.append(locale.monthsShort[m]); // e.g. Jan
                break;
            }
            case FORMAT_MMMM_LOWER:
            case FORMAT_MMMM_UPPER:
            case FORMAT_MMMMM_LOWER:
            case FORMAT_MMMMM_UPPER:
            {
                int m = calendar.get(Calendar.MONTH); // 0-based
                buf.append(locale.monthsLong[m]); // e.g. January
                break;
            }
            case FORMAT_Q:
            {
                int m = calendar.get(Calendar.MONTH);
                // 0(Jan) -> q1, 1(Feb) -> q1, 2(Mar) -> q1, 3(Apr) -> q2
                int q = m / 3 + 1;
                buf.append(q);
                break;
            }
            case FORMAT_Y:
            {
                int doy = calendar.get(Calendar.DAY_OF_YEAR);
                buf.append(doy);
                break;
            }
            case FORMAT_YY:
            {
                int y = calendar.get(Calendar.YEAR) % 100;
                if (y < 10) {
                    buf.append('0');
                }
                buf.append(y);
                break;
            }
            case FORMAT_YYYY:
            {
                int y = calendar.get(Calendar.YEAR);
                buf.append(y);
                break;
            }
            case FORMAT_H:
            {
                int h = calendar.get(
                    twelveHourClock ? Calendar.HOUR : Calendar.HOUR_OF_DAY);
                buf.append(h);
                break;
            }
            case FORMAT_HH:
            case FORMAT_HH_UPPER:
            {
                int h = calendar.get(
                    twelveHourClock ? Calendar.HOUR : Calendar.HOUR_OF_DAY);
                if (h < 10) {
                    buf.append('0');
                }
                buf.append(h);
                break;
            }
            case FORMAT_N:
            {
                int n = calendar.get(Calendar.MINUTE);
                buf.append(n);
                break;
            }
            case FORMAT_NN:
            {
                int n = calendar.get(Calendar.MINUTE);
                if (n < 10) {
                    buf.append('0');
                }
                buf.append(n);
                break;
            }
            case FORMAT_S:
            {
                int s = calendar.get(Calendar.SECOND);
                buf.append(s);
                break;
            }
            case FORMAT_SS:
            {
                int s = calendar.get(Calendar.SECOND);
                if (s < 10) {
                    buf.append('0');
                }
                buf.append(s);
                break;
            }
            case FORMAT_TTTTT:
            {
                // Officially, we should use the system's time format. But
                // for now, we always print using the default format, h:mm:ss.
                format(FORMAT_H, calendar, buf);
                buf.append(locale.timeSeparator);
                format(FORMAT_NN, calendar, buf);
                buf.append(locale.timeSeparator);
                format(FORMAT_SS, calendar, buf);
                break;
            }
            case FORMAT_AMPM:
            case FORMAT_UPPER_AM_SOLIDUS_PM:
            {
                boolean isAm = calendar.get(Calendar.AM_PM) == Calendar.AM;
                buf.append(isAm ? "AM" : "PM");
                break;
            }
            case FORMAT_LOWER_AM_SOLIDUS_PM:
            {
                boolean isAm = calendar.get(Calendar.AM_PM) == Calendar.AM;
                buf.append(isAm ? "am" : "pm");
                break;
            }
            case FORMAT_UPPER_A_SOLIDUS_P:
            {
                boolean isAm = calendar.get(Calendar.AM_PM) == Calendar.AM;
                buf.append(isAm ? "A" : "P");
                break;
            }
            case FORMAT_LOWER_A_SOLIDUS_P:
            {
                boolean isAm = calendar.get(Calendar.AM_PM) == Calendar.AM;
                buf.append(isAm ? "a" : "p");
                break;
            }
            default:
                throw new Error();
            }
        }
    }

    /**
     * A FormatLocale contains all information necessary to format objects
     * based upon the locale of the end-user.  Use {@link Format#createLocale}
     * to make one.
     */
    public static class FormatLocale
    {
        char thousandSeparator;
        char decimalPlaceholder;
        String dateSeparator;
        String timeSeparator;
        String currencySymbol;
        String currencyFormat;
        String[] daysOfWeekShort;
        String[] daysOfWeekLong;
        String[] monthsShort;
        String[] monthsLong;
        private final Locale locale;

        private FormatLocale(
            char thousandSeparator,
            char decimalPlaceholder,
            String dateSeparator,
            String timeSeparator,
            String currencySymbol,
            String currencyFormat,
            String[] daysOfWeekShort,
            String[] daysOfWeekLong,
            String[] monthsShort,
            String[] monthsLong,
            Locale locale)
        {
            this.locale = locale;
            if (thousandSeparator == '\0') {
                thousandSeparator = thousandSeparator_en;
            }
            this.thousandSeparator = thousandSeparator;
            if (decimalPlaceholder == '\0') {
                decimalPlaceholder = decimalPlaceholder_en;
            }
            this.decimalPlaceholder = decimalPlaceholder;
            if (dateSeparator == null) {
                dateSeparator = dateSeparator_en;
            }
            this.dateSeparator = dateSeparator;
            if (timeSeparator == null) {
                timeSeparator = timeSeparator_en;
            }
            this.timeSeparator = timeSeparator;
            if (currencySymbol == null) {
                currencySymbol = currencySymbol_en;
            }
            this.currencySymbol = currencySymbol;
            if (currencyFormat == null) {
                currencyFormat = currencyFormat_en;
            }
            this.currencyFormat = currencyFormat;
            if (daysOfWeekShort == null) {
                daysOfWeekShort = daysOfWeekShort_en;
            }
            this.daysOfWeekShort = daysOfWeekShort;
            if (daysOfWeekLong == null) {
                daysOfWeekLong = daysOfWeekLong_en;
            }
            this.daysOfWeekLong = daysOfWeekLong;
            if (monthsShort == null) {
                monthsShort = monthsShort_en;
            }
            this.monthsShort = monthsShort;
            if (monthsLong == null) {
                monthsLong = monthsLong_en;
            }
            this.monthsLong = monthsLong;
            if (daysOfWeekShort.length != 8
                || daysOfWeekLong.length != 8
                || monthsShort.length != 13
                || monthsLong.length != 13)
            {
                throw new IllegalArgumentException(
                    "Format: day or month array has incorrect length");
            }
        }
    }

    private static class StringFormat extends BasicFormat
    {
        final StringCase stringCase;
        final String literal;
        final JavaFormat javaFormat;

        StringFormat(StringCase stringCase, String literal, Locale locale) {
            assert stringCase != null;
            this.stringCase = stringCase;
            this.literal = literal;
            this.javaFormat = new JavaFormat(locale);
        }

        @Override
        void format(String s, StringBuilder buf) {
            switch (stringCase) {
            case UPPER:
                s = s.toUpperCase();
                break;
            case LOWER:
                s = s.toLowerCase();
                break;
            }
            buf.append(s);
        }

        void format(double d, StringBuilder buf) {
            final int x = buf.length();
            javaFormat.format(d, buf);
            String s = buf.substring(x);
            buf.setLength(x);
            format(s, buf);
        }

        void format(long n, StringBuilder buf) {
            final int x = buf.length();
            javaFormat.format(n, buf);
            String s = buf.substring(x);
            buf.setLength(x);
            format(s, buf);
        }

        void format(Date date, StringBuilder buf) {
            final int x = buf.length();
            javaFormat.format(date, buf);
            String s = buf.substring(x);
            buf.setLength(x);
            format(s, buf);
        }

        void format(Calendar calendar, StringBuilder buf) {
            final int x = buf.length();
            javaFormat.format(calendar, buf);
            String s = buf.substring(x);
            buf.setLength(x);
            format(s, buf);
        }
    }

    private enum StringCase {
        UPPER,
        LOWER
    }

    /** Types of Format. */
    private static final int GENERAL = 0;
    private static final int DATE = 1;
    private static final int NUMERIC = 2;
    private static final int STRING = 4;

    /** A Format is flagged SPECIAL if it needs special processing
     * during parsing. */
    private static final int SPECIAL = 8;

    /** Values for {@link Format.BasicFormat#code}. */
    private static final int FORMAT_NULL = 0;
    private static final int FORMAT_C = 3;
    private static final int FORMAT_D = 4;
    private static final int FORMAT_DD = 5;
    private static final int FORMAT_DDD = 6;
    private static final int FORMAT_DDDD = 7;
    private static final int FORMAT_DDDDD = 8;
    private static final int FORMAT_DDDDDD = 9;
    private static final int FORMAT_W = 10;
    private static final int FORMAT_WW = 11;
    private static final int FORMAT_M = 12;
    private static final int FORMAT_MM = 13;
    private static final int FORMAT_MMM_UPPER = 14;
    private static final int FORMAT_MMMM_UPPER = 15;
    private static final int FORMAT_Q = 16;
    private static final int FORMAT_Y = 17;
    private static final int FORMAT_YY = 18;
    private static final int FORMAT_YYYY = 19;
    private static final int FORMAT_H = 20;
    private static final int FORMAT_HH = 21;
    private static final int FORMAT_N = 22;
    private static final int FORMAT_NN = 23;
    private static final int FORMAT_S = 24;
    private static final int FORMAT_SS = 25;
    private static final int FORMAT_TTTTT = 26;
    private static final int FORMAT_UPPER_AM_SOLIDUS_PM = 27;
    private static final int FORMAT_LOWER_AM_SOLIDUS_PM = 28;
    private static final int FORMAT_UPPER_A_SOLIDUS_P = 29;
    private static final int FORMAT_LOWER_A_SOLIDUS_P = 30;
    private static final int FORMAT_AMPM = 31;
    private static final int FORMAT_0 = 32;
    private static final int FORMAT_POUND = 33;
    private static final int FORMAT_DECIMAL = 34;
    private static final int FORMAT_PERCENT = 35;
    private static final int FORMAT_THOUSEP = 36;
    private static final int FORMAT_TIMESEP = 37;
    private static final int FORMAT_DATESEP = 38;
    private static final int FORMAT_E_MINUS_UPPER = 39;
    private static final int FORMAT_E_PLUS_UPPER = 40;
    private static final int FORMAT_E_MINUS_LOWER = 41;
    private static final int FORMAT_E_PLUS_LOWER = 42;
    private static final int FORMAT_LITERAL = 43;
    private static final int FORMAT_BACKSLASH = 44;
    private static final int FORMAT_QUOTE = 45;
    private static final int FORMAT_CHARACTER_OR_SPACE = 46;
    private static final int FORMAT_CHARACTER_OR_NOTHING = 47;
    private static final int FORMAT_LOWER = 48;
    private static final int FORMAT_UPPER = 49;
    private static final int FORMAT_FILL_FROM_LEFT = 50;
    private static final int FORMAT_SEMI = 51;
    private static final int FORMAT_GENERAL_NUMBER = 52;
    private static final int FORMAT_GENERAL_DATE = 53;
    private static final int FORMAT_INTL_CURRENCY = 54;
    private static final int FORMAT_MMM_LOWER = 55;
    private static final int FORMAT_MMMM_LOWER = 56;
    private static final int FORMAT_USD = 57;
    private static final int FORMAT_MM_UPPER = 58;
    private static final int FORMAT_MMMMM_LOWER = 59;
    private static final int FORMAT_MMMMM_UPPER = 60;
    private static final int FORMAT_HH_UPPER = 61;
    private static final int FORMAT_M_UPPER = 62;

    private static final Map<Integer, String> formatTokenToFormatString =
        new HashMap<Integer, String>();

    private static Token nfe(
        int code, int flags, String token, String purpose, String description)
    {
        Util.discard(purpose);
        Util.discard(description);
        formatTokenToFormatString.put(code, token);
        return new Token(code, flags, token);
    }

    public static List<Token> getTokenList()
    {
        return Collections.unmodifiableList(Arrays.asList(tokens));
    }

    /**
     * Returns the format token as a string representation
     * which corresponds to a given token code.
     * @param code The code of the token to obtain.
     * @return The string representation of that token.
     */
    public static String getFormatToken(int code)
    {
        return formatTokenToFormatString.get(code);
    }

    private static final Token[] tokens = {
        nfe(
            FORMAT_NULL,
            NUMERIC,
            null,
            "No formatting",
            "Display the number with no formatting."),
        nfe(
            FORMAT_C,
            DATE,
            "C",
            null,
            "Display the date as ddddd and display the time as t t t t t, in "
            + "that order. Display only date information if there is no "
            + "fractional part to the date serial number; display only time "
            + "information if there is no integer portion."),
        nfe(
            FORMAT_D,
            DATE,
            "d",
            null,
            "Display the day as a number without a leading zero (1 - 31)."),
        nfe(
            FORMAT_DD,
            DATE,
            "dd",
            null,
            "Display the day as a number with a leading zero (01 - 31)."),
        nfe(
            FORMAT_DDD,
            DATE,
            "Ddd",
            null,
            "Display the day as an abbreviation (Sun - Sat)."),
        nfe(
            FORMAT_DDDD,
            DATE,
            "dddd",
            null,
            "Display the day as a full name (Sunday - Saturday)."),
        nfe(
            FORMAT_DDDDD,
            DATE,
            "ddddd",
            null,
            "Display the date as a complete date (including day, month, and "
            + "year), formatted according to your system's short date format "
            + "setting. The default short date format is m/d/yy."),
        nfe(
            FORMAT_DDDDDD,
            DATE,
            "dddddd",
            null,
            "Display a date serial number as a complete date (including day, "
            + "month, and year) formatted according to the long date setting "
            + "recognized by your system. The default long date format is mmmm "
            + "dd, yyyy."),
        nfe(
            FORMAT_W,
            DATE,
            "w",
            null,
            "Display the day of the week as a number (1 for Sunday through 7 "
            + "for Saturday)."),
        nfe(
            FORMAT_WW,
            DATE,
            "ww",
            null,
            "Display the week of the year as a number (1 - 53)."),
        nfe(
            FORMAT_M,
            DATE | SPECIAL,
            "m",
            null,
            "Display the month as a number without a leading zero (1 - 12). If "
            + "m immediately follows h or hh, the minute rather than the month "
            + "is displayed."),
        nfe(
            FORMAT_M_UPPER,
            DATE,
            "M",
            null,
            "Display the month as a number without a leading zero (1 - 12)."),
        nfe(
            FORMAT_MM,
            DATE | SPECIAL,
            "mm",
            null,
            "Display the month as a number with a leading zero (01 - 12). If m "
            + "immediately follows h or hh, the minute rather than the month "
            + "is displayed."),
        nfe(
            FORMAT_MM_UPPER,
            DATE,
            "MM",
            null,
            "Display the month as a number with a leading zero (01 - 12)."),
        nfe(
            FORMAT_MMM_LOWER,
            DATE,
            "mmm",
            null,
            "Display the month as an abbreviation (Jan - Dec)."),
        nfe(
            FORMAT_MMMM_LOWER,
            DATE,
            "mmmm",
            null,
            "Display the month as a full month name (January - December)."),
        nfe(
            FORMAT_MMM_UPPER,
            DATE,
            "MMM",
            null,
            "Display the month as an abbreviation (Jan - Dec)."),
        nfe(
            FORMAT_MMMM_UPPER,
            DATE,
            "MMMM",
            null,
            "Display the month as a full month name (January - December)."),
        nfe(
            FORMAT_Q,
            DATE,
            "q",
            null,
            "Display the quarter of the year as a number (1 - 4)."),
        nfe(
            FORMAT_Y,
            DATE,
            "y",
            null,
            "Display the day of the year as a number (1 - 366)."),
        nfe(
            FORMAT_YY,
            DATE,
            "yy",
            null,
            "Display the year as a 2-digit number (00 - 99)."),
        nfe(
            FORMAT_YYYY,
            DATE,
            "yyyy",
            null,
            "Display the year as a 4-digit number (100 - 9999)."),
        nfe(
            FORMAT_H,
            DATE,
            "h",
            null,
            "Display the hour as a number without leading zeros (0 - 23)."),
        nfe(
            FORMAT_HH,
            DATE,
            "hh",
            null,
            "Display the hour as a number with leading zeros (00 - 23)."),
        nfe(
            FORMAT_N,
            DATE,
            "n",
            null,
            "Display the minute as a number without leading zeros (0 - 59)."),
        nfe(
            FORMAT_NN,
            DATE,
            "nn",
            null,
            "Display the minute as a number with leading zeros (00 - 59)."),
        nfe(
            FORMAT_S,
            DATE,
            "s",
            null,
            "Display the second as a number without leading zeros (0 - 59)."),
        nfe(
            FORMAT_SS,
            DATE,
            "ss",
            null,
            "Display the second as a number with leading zeros (00 - 59)."),
        nfe(
            FORMAT_TTTTT,
            DATE,
            "ttttt",
            null,
            "Display a time as a complete time (including hour, minute, and "
            + "second), formatted using the time separator defined by the time "
            + "format recognized by your system. A leading zero is displayed "
            + "if the leading zero option is selected and the time is before "
            + "10:00 A.M. or P.M. The default time format is h:mm:ss."),
        nfe(
            FORMAT_UPPER_AM_SOLIDUS_PM,
            DATE,
            "AM/PM",
            null,
            "Use the 12-hour clock and display an uppercase AM with any hour "
            + "before noon; display an uppercase PM with any hour between noon and 11:59 P.M."),
        nfe(
            FORMAT_LOWER_AM_SOLIDUS_PM,
            DATE,
            "am/pm",
            null,
            "Use the 12-hour clock and display a lowercase AM with any hour "
            + "before noon; display a lowercase PM with any hour between noon "
            + "and 11:59 P.M."),
        nfe(
            FORMAT_UPPER_A_SOLIDUS_P,
            DATE,
            "A/P",
            null,
            "Use the 12-hour clock and display an uppercase A with any hour "
            + "before noon; display an uppercase P with any hour between noon "
            + "and 11:59 P.M."),
        nfe(
            FORMAT_LOWER_A_SOLIDUS_P,
            DATE,
            "a/p",
            null,
            "Use the 12-hour clock and display a lowercase A with any hour "
            + "before noon; display a lowercase P with any hour between noon "
            + "and 11:59 P.M."),
        nfe(
            FORMAT_AMPM,
            DATE,
            "AMPM",
            null,
            "Use the 12-hour clock and display the AM string literal as "
            + "defined by your system with any hour before noon; display the "
            + "PM string literal as defined by your system with any hour "
            + "between noon and 11:59 P.M. AMPM can be either uppercase or "
            + "lowercase, but the case of the string displayed matches the "
            + "string as defined by your system settings. The default format "
            + "is AM/PM."),
        nfe(
            FORMAT_0,
            NUMERIC | SPECIAL,
            "0",
            "Digit placeholder",
            "Display a digit or a zero. If the expression has a digit in the "
            + "position where the 0 appears in the format string, display it; "
            + "otherwise, display a zero in that position. If the number has "
            + "fewer digits than there are zeros (on either side of the "
            + "decimal) in the format expression, display leading or trailing "
            + "zeros. If the number has more digits to the right of the "
            + "decimal separator than there are zeros to the right of the "
            + "decimal separator in the format expression, round the number to "
            + "as many decimal places as there are zeros. If the number has "
            + "more digits to the left of the decimal separator than there are "
            + "zeros to the left of the decimal separator in the format "
            + "expression, display the extra digits without modification."),
        nfe(
            FORMAT_POUND,
            NUMERIC | SPECIAL,
            "#",
            "Digit placeholder",
            "Display a digit or nothing. If the expression has a digit in the "
            + "position where the # appears in the format string, display it; "
            + "otherwise, display nothing in that position.  This symbol works "
            + "like the 0 digit placeholder, except that leading and trailing "
            + "zeros aren't displayed if the number has the same or fewer "
            + "digits than there are # characters on either side of the "
            + "decimal separator in the format expression."),
        nfe(
            FORMAT_DECIMAL,
            NUMERIC | SPECIAL,
            ".",
            "Decimal placeholder",
            "In some locales, a comma is used as the decimal separator. The "
            + "decimal placeholder determines how many digits are displayed to "
            + "the left and right of the decimal separator. If the format "
            + "expression contains only number signs to the left of this "
            + "symbol, numbers smaller than 1 begin with a decimal separator. "
            + "If you always want a leading zero displayed with fractional "
            + "numbers, use 0 as the first digit placeholder to the left of "
            + "the decimal separator instead. The actual character used as a "
            + "decimal placeholder in the formatted output depends on the "
            + "Number Format recognized by your system."),
        nfe(
            FORMAT_PERCENT,
            NUMERIC,
            "%",
            "Percent placeholder",
            "The expression is multiplied by 100. The percent character (%) is "
            + "inserted in the position where it appears in the format "
            + "string."),
        nfe(
            FORMAT_THOUSEP,
            NUMERIC | SPECIAL,
            ",",
            "Thousand separator",
            "In some locales, a period is used as a thousand separator. The "
            + "thousand separator separates thousands from hundreds within a "
            + "number that has four or more places to the left of the decimal "
            + "separator. Standard use of the thousand separator is specified "
            + "if the format contains a thousand separator surrounded by digit "
            + "placeholders (0 or #). Two adjacent thousand separators or a "
            + "thousand separator immediately to the left of the decimal "
            + "separator (whether or not a decimal is specified) means \"scale "
            + "the number by dividing it by 1000, rounding as needed.\"  You "
            + "can scale large numbers using this technique. For example, you "
            + "can use the format string \"##0,,\" to represent 100 million as "
            + "100. Numbers smaller than 1 million are displayed as 0. Two "
            + "adjacent thousand separators in any position other than "
            + "immediately to the left of the decimal separator are treated "
            + "simply as specifying the use of a thousand separator. The "
            + "actual character used as the thousand separator in the "
            + "formatted output depends on the Number Format recognized by "
            + "your system."),
        nfe(
            FORMAT_TIMESEP,
            DATE | SPECIAL,
            ":",
            "Time separator",
            "In some locales, other characters may be used to represent the "
            + "time separator. The time separator separates hours, minutes, "
            + "and seconds when time values are formatted. The actual "
            + "character used as the time separator in formatted output is "
            + "determined by your system settings."),
        nfe(
            FORMAT_DATESEP,
            DATE | SPECIAL,
            "/",
            "Date separator",
            "In some locales, other characters may be used to represent the "
            + "date separator. The date separator separates the day, month, "
            + "and year when date values are formatted. The actual character "
            + "used as the date separator in formatted output is determined by "
            + "your system settings."),
        nfe(
            FORMAT_E_MINUS_UPPER,
            NUMERIC | SPECIAL,
            "E-",
            "Scientific format",
            "If the format expression contains at least one digit placeholder "
            + "(0 or #) to the right of E-, E+, e-, or e+, the number is "
            + "displayed in scientific format and E or e is inserted between "
            + "the number and its exponent. The number of digit placeholders "
            + "to the right determines the number of digits in the exponent. "
            + "Use E- or e- to place a minus sign next to negative exponents. "
            + "Use E+ or e+ to place a minus sign next to negative exponents "
            + "and a plus sign next to positive exponents."),
        nfe(
            FORMAT_E_PLUS_UPPER,
            NUMERIC | SPECIAL,
            "E+",
            "Scientific format",
            "See E-."),
        nfe(
            FORMAT_E_MINUS_LOWER,
            NUMERIC | SPECIAL,
            "e-",
            "Scientific format",
            "See E-."),
        nfe(
            FORMAT_E_PLUS_LOWER,
            NUMERIC | SPECIAL,
            "e+",
            "Scientific format",
            "See E-."),
        nfe(
            FORMAT_LITERAL,
            GENERAL,
            "-",
            "Display a literal character",
            "To display a character other than one of those listed, precede it "
            + "with a backslash (\\) or enclose it in double quotation marks "
            + "(\" \")."),
        nfe(
            FORMAT_LITERAL,
            GENERAL,
            "+",
            "Display a literal character",
            "See -."),
        nfe(
            FORMAT_LITERAL,
            GENERAL,
            "$",
            "Display a literal character",
            "See -."),
        nfe(
            FORMAT_LITERAL,
            GENERAL,
            "(",
            "Display a literal character",
            "See -."),
        nfe(
            FORMAT_LITERAL,
            GENERAL,
            ")",
            "Display a literal character",
            "See -."),
        nfe(
            FORMAT_LITERAL,
            GENERAL,
            " ",
            "Display a literal character",
            "See -."),
        nfe(
            FORMAT_BACKSLASH,
            GENERAL | SPECIAL,
            "\\",
            "Display the next character in the format string",
            "Many characters in the format expression have a special meaning "
            + "and can't be displayed as literal characters unless they are "
            + "preceded by a backslash. The backslash itself isn't displayed. "
            + "Using a backslash is the same as enclosing the next character "
            + "in double quotation marks. To display a backslash, use two "
            + "backslashes (\\).  Examples of characters that can't be "
            + "displayed as literal characters are the date- and "
            + "time-formatting characters (a, c, d, h, m, n, p, q, s, t, w, y, "
            + "and /:), the numeric-formatting characters (#, 0, %, E, e, "
            + "comma, and period), and the string-formatting characters (@, &, "
            + "<, >, and !)."),
        nfe(
            FORMAT_QUOTE,
            GENERAL | SPECIAL,
            "\"",
            "Display the string inside the double quotation marks",
            "To include a string in format from within code, you must use "
            + "Chr(34) to enclose the text (34 is the character code for a "
            + "double quotation mark)."),
        nfe(
            FORMAT_CHARACTER_OR_SPACE,
            STRING,
            "@",
            "Character placeholder",
            "Display a character or a space. If the string has a character in "
            + "the position where the @ appears in the format string, display "
            + "it; otherwise, display a space in that position. Placeholders "
            + "are filled from right to left unless there is an ! character in "
            + "the format string. See below."),
        nfe(
            FORMAT_CHARACTER_OR_NOTHING,
            STRING,
            "&",
            "Character placeholder",
            "Display a character or nothing. If the string has a character in "
            + "the position where the & appears, display it; otherwise, "
            + "display nothing. Placeholders are filled from right to left "
            + "unless there is an ! character in the format string. See "
            + "below."),
        nfe(
            FORMAT_LOWER,
            STRING | SPECIAL,
            "<",
            "Force lowercase",
            "Display all characters in lowercase format."),
        nfe(
            FORMAT_UPPER,
            STRING | SPECIAL,
            ">",
            "Force uppercase",
            "Display all characters in uppercase format."),
        nfe(
            FORMAT_FILL_FROM_LEFT,
            STRING | SPECIAL,
            "!",
            "Force left to right fill of placeholders",
            "The default is to fill from right to left."),
        nfe(
            FORMAT_SEMI,
            GENERAL | SPECIAL,
            ";",
            "Separates format strings for different kinds of values",
            "If there is one section, the format expression applies to all "
            + "values. If there are two sections, the first section applies "
            + "to positive values and zeros, the second to negative values. If "
            + "there are three sections, the first section applies to positive "
            + "values, the second to negative values, and the third to zeros. "
            + "If there are four sections, the first section applies to "
            + "positive values, the second to negative values, the third to "
            + "zeros, and the fourth to Null values."),
        nfe(
            FORMAT_INTL_CURRENCY,
            NUMERIC | SPECIAL,
            intlCurrencySymbol + "",
            null,
            "Display the locale's currency symbol."),
        nfe(
            FORMAT_USD,
            GENERAL,
            "USD",
            null,
            "Display USD (U.S. Dollars)."),
        nfe(
            FORMAT_GENERAL_NUMBER,
            NUMERIC | SPECIAL,
            "General Number",
            null,
            "Shows numbers as entered."),
        nfe(
            FORMAT_GENERAL_DATE,
            DATE | SPECIAL,
            "General Date",
            null,
            "Shows date and time if expression contains both. If expression is "
            + "only a date or a time, the missing information is not "
            + "displayed."),
        nfe(
            FORMAT_MMMMM_LOWER,
            DATE,
            "mmmmm",
            null,
            "Display the month as a full month name (January - December)."),
        nfe(
            FORMAT_MMMMM_UPPER,
            DATE,
            "MMMMM",
            null,
            "Display the month as a full month name (January - December)."),
        nfe(
            FORMAT_HH_UPPER,
            DATE,
            "HH",
            null,
            "Display the hour as a number with leading zeros (00 - 23)."),
    };

    // Named formats.  todo: Supply the translation strings.
    private enum MacroToken {
        CURRENCY(
            "Currency",
            null,
            "Shows currency values according to the locale's CurrencyFormat.  "
            + "Negative numbers are inside parentheses."),
        FIXED(
            "Fixed", "0", "Shows at least one digit."),
        STANDARD(
            "Standard", "#,##0", "Uses a thousands separator."),
        PERCENT(
            "Percent",
            "0.00%",
            "Multiplies the value by 100 with a percent sign at the end."),
        SCIENTIFIC(
            "Scientific", "0.00e+00", "Uses standard scientific notation."),
        LONG_DATE(
            "Long Date",
            "dddd, mmmm dd, yyyy",
            "Uses the Long Date format specified in the Regional Settings "
            + "dialog box of the Microsoft Windows Control Panel."),
        MEDIUM_DATE(
            "Medium Date",
            "dd-mmm-yy",
            "Uses the dd-mmm-yy format (for example, 03-Apr-93)"),
        SHORT_DATE(
            "Short Date",
            "m/d/yy",
            "Uses the Short Date format specified in the Regional Settings "
            + "dialog box of the Windows Control Panel."),
        LONG_TIME(
            "Long Time",
            "h:mm:ss AM/PM",
            "Shows the hour, minute, second, and \"AM\" or \"PM\" using the "
            + "h:mm:ss format."),
        MEDIUM_TIME(
            "Medium Time",
            "h:mm AM/PM",
            "Shows the hour, minute, and \"AM\" or \"PM\" using the \"hh:mm "
            + "AM/PM\" format."),
        SHORT_TIME(
            "Short Time",
            "hh:mm",
            "Shows the hour and minute using the hh:mm format."),
        YES_NO(
            "Yes/No",
            "\\Y\\e\\s;\\Y\\e\\s;\\N\\o;\\N\\o",
            "Any nonzero numeric value (usually - 1) is Yes. Zero is No."),
        TRUE_FALSE(
            "True/False",
            "\\T\\r\\u\\e;\\T\\r\\u\\e;\\F\\a\\l\\s\\e;\\F\\a\\l\\s\\e",
            "Any nonzero numeric value (usually - 1) is True. Zero is False."),
        ON_OFF(
            "On/Off",
            "\\O\\n;\\O\\n;\\O\\f\\f;\\O\\f\\f",
            "Any nonzero numeric value (usually - 1) is On. Zero is Off.");

        /**
         * Maps macro token names with their related object. Used
         * to fast-resolve a macro token without iterating.
         */
        private static final Map<String, MacroToken> MAP =
            new HashMap<String, MacroToken>();

        static {
            for (MacroToken macroToken : values()) {
                MAP.put(macroToken.token, macroToken);
            }
        }

        MacroToken(String token, String translation, String description) {
            this.token = token;
            this.translation = translation;
            this.description = description;
            assert name().equals(
                token
                    .replace(',', '_')
                    .replace(' ', '_')
                    .replace('/', '_')
                    .toUpperCase());
            assert (translation == null) == name().equals("CURRENCY");
        }

        final String token;
        final String translation;
        final String description;

        static String expand(FormatLocale locale, String formatString) {
            final MacroToken macroToken = MAP.get(formatString);
            if (macroToken == null) {
                return formatString;
            }
            if (macroToken == MacroToken.CURRENCY) {
                // e.g. "$#,##0.00;($#,##0.00)"
                return locale.currencyFormat
                    + ";("  + locale.currencyFormat + ")";
            } else {
                return macroToken.translation;
            }
        }
    }

    /**
     * Constructs a <code>Format</code> in a specific locale.
     *
     * @param formatString the format string; see
     *   <a href="http://www.apostate.com/programming/vb-format.html">this
     *   description</a> for more details
     * @param locale The locale
     */
    public Format(String formatString, Locale locale)
    {
        this(formatString, getBestFormatLocale(locale));
    }

    /**
     * Constructs a <code>Format</code> in a specific locale.
     *
     * @param formatString the format string; see
     *   <a href="http://www.apostate.com/programming/vb-format.html">this
     *   description</a> for more details
     * @param locale The locale
     *
     * @see FormatLocale
     * @see #createLocale
     */
    public Format(String formatString, FormatLocale locale)
    {
        if (formatString == null) {
            formatString = "";
        }
        this.formatString = formatString;
        if (locale == null) {
            locale = locale_US;
        }
        this.locale = locale;

        List<BasicFormat> alternateFormatList = new ArrayList<BasicFormat>();
        FormatType[] formatType = {null};
        while (formatString.length() > 0) {
            formatString = parseFormatString(
                formatString, alternateFormatList, formatType);
        }

        // If the format string is empty, use a Java format.
        // Later entries in the formats list default to the first (e.g.
        // "#.00;;Nil"), but the first entry must be set.
        if (alternateFormatList.size() == 0
            || alternateFormatList.get(0) == null)
        {
            format = new JavaFormat(locale.locale);
        } else if (alternateFormatList.size() == 1
                   && (formatType[0] == FormatType.DATE
                       || formatType[0] == FormatType.STRING))
        {
            format = alternateFormatList.get(0);
        } else {
            BasicFormat[] alternateFormats =
                alternateFormatList.toArray(
                    new BasicFormat[alternateFormatList.size()]);
            format = new AlternateFormat(alternateFormats, locale);
        }
    }

    /**
     * Constructs a <code>Format</code> in a specific locale, or retrieves
     * one from the cache if one already exists.
     *
     * <p>If the number of entries in the cache exceeds {@link #CacheLimit},
     * replaces the eldest entry in the cache.
     *
     * @param formatString the format string; see
     *   <a href="http://www.apostate.com/programming/vb-format.html">this
     *   description</a> for more details
     *
     * @return format for given format string in given locale
     */
    public static Format get(String formatString, Locale locale) {
        String key = formatString + "@@@" + locale;
        Format format = cache.get(key);
        if (format == null) {
            synchronized (cache) {
                format = cache.get(key);
                if (format == null) {
                    format = new Format(formatString, locale);
                    cache.put(key, format);
                }
            }
        }
        return format;
    }

    /**
     * Create a {@link FormatLocale} object characterized by the given
     * properties.
     *
     * @param thousandSeparator the character used to separate thousands in
     *   numbers, or ',' by default.  For example, 12345 is '12,345 in English,
     *   '12.345 in French.
     * @param decimalPlaceholder the character placed between the integer and
     *   the fractional part of decimal numbers, or '.' by default.  For
     *   example, 12.34 is '12.34' in English, '12,34' in French.
     * @param dateSeparator the character placed between the year, month and
     *   day of a date such as '12/07/2001', or '/' by default.
     * @param timeSeparator the character placed between the hour, minute and
     *   second value of a time such as '1:23:45 AM', or ':' by default.
     * @param daysOfWeekShort Short forms of the days of the week.
     *            The array is 1-based, because position
     *            {@link Calendar#SUNDAY} (= 1) must hold Sunday, etc.
     *            The array must have 8 elements.
     *            For example {"", "Sun", "Mon", ..., "Sat"}.
     * @param daysOfWeekLong Long forms of the days of the week.
     *            The array is 1-based, because position
     *            {@link Calendar#SUNDAY} must hold Sunday, etc.
     *            The array must have 8 elements.
     *            For example {"", "Sunday", ..., "Saturday"}.
     * @param monthsShort Short forms of the months of the year.
     *            The array is 0-based, because position
     *            {@link Calendar#JANUARY} (= 0) holds January, etc.
     *            For example {"Jan", ..., "Dec", ""}.
     * @param monthsLong Long forms of the months of the year.
     *            The array is 0-based, because position
     *            {@link Calendar#JANUARY} (= 0) holds January, etc.
     *            For example {"January", ..., "December", ""}.
     * @param locale if this is not null, register that the constructed
     *     <code>FormatLocale</code> is the default for <code>locale</code>
     */
    public static FormatLocale createLocale(
        char thousandSeparator,
        char decimalPlaceholder,
        String dateSeparator,
        String timeSeparator,
        String currencySymbol,
        String currencyFormat,
        String[] daysOfWeekShort,
        String[] daysOfWeekLong,
        String[] monthsShort,
        String[] monthsLong,
        Locale locale)
    {
        FormatLocale formatLocale = new FormatLocale(
            thousandSeparator, decimalPlaceholder, dateSeparator,
            timeSeparator, currencySymbol, currencyFormat, daysOfWeekShort,
            daysOfWeekLong, monthsShort, monthsLong, locale);
        if (locale != null) {
            registerFormatLocale(formatLocale, locale);
        }
        return formatLocale;
    }

    public static FormatLocale createLocale(Locale locale)
    {
        final DecimalFormatSymbols decimalSymbols =
            new DecimalFormatSymbols(locale);
        final DateFormatSymbols dateSymbols = new DateFormatSymbols(locale);

        Calendar calendar = Calendar.getInstance(locale);
        calendar.set(1969, Calendar.DECEMBER, 31, 0, 0, 0);
        final Date date = calendar.getTime();

        final java.text.DateFormat dateFormat =
            java.text.DateFormat.getDateInstance(
                java.text.DateFormat.SHORT, locale);
        final String dateValue = dateFormat.format(date); // "12/31/69"
        String dateSeparator = dateValue.substring(2, 3); // "/"

        final java.text.DateFormat timeFormat =
            java.text.DateFormat.getTimeInstance(
                java.text.DateFormat.SHORT, locale);
        final String timeValue = timeFormat.format(date); // "12:00:00"
        String timeSeparator = timeValue.substring(2, 3); // ":"

        // Deduce the locale's currency format.
        // For example, US is "$#,###.00"; France is "#,###-00FF".
        final NumberFormat currencyFormat =
            NumberFormat.getCurrencyInstance(locale);
        final String currencyValue = currencyFormat.format(123456.78);
        String currencyLeft =
            currencyValue.substring(0, currencyValue.indexOf("1"));
        String currencyRight =
            currencyValue.substring(currencyValue.indexOf("8") + 1);
        StringBuilder buf = new StringBuilder();
        buf.append(currencyLeft);
        int minimumIntegerDigits = currencyFormat.getMinimumIntegerDigits();
        for (int i = Math.max(minimumIntegerDigits, 4) - 1; i >= 0; --i) {
            buf.append(i < minimumIntegerDigits ? '0' : '#');
            if (i % 3 == 0 && i > 0) {
                buf.append(',');
            }
        }
        if (currencyFormat.getMaximumFractionDigits() > 0) {
            buf.append('.');
            appendTimes(buf, '0', currencyFormat.getMinimumFractionDigits());
            appendTimes(
                buf, '#',
                currencyFormat.getMaximumFractionDigits()
                - currencyFormat.getMinimumFractionDigits());
        }
        buf.append(currencyRight);
        String currencyFormatString = buf.toString();

        // If the locale passed is only a language, Java cannot
        // resolve the currency symbol and will instead return
        // u00a4 (The international currency symbol). For those cases,
        // we use the default system locale currency symbol.
        String currencySymbol = decimalSymbols.getCurrencySymbol();
        if (currencySymbol.equals(Format.intlCurrencySymbol + "")) {
            final DecimalFormatSymbols defaultDecimalSymbols =
                new DecimalFormatSymbols(Locale.getDefault());
            currencySymbol = defaultDecimalSymbols.getCurrencySymbol();
        }

        return createLocale(
            decimalSymbols.getGroupingSeparator(),
            decimalSymbols.getDecimalSeparator(),
            dateSeparator,
            timeSeparator,
            currencySymbol,
            currencyFormatString,
            dateSymbols.getShortWeekdays(),
            dateSymbols.getWeekdays(),
            dateSymbols.getShortMonths(),
            dateSymbols.getMonths(),
            locale);
    }

    private static void appendTimes(StringBuilder buf, char c, int i) {
        while (i-- > 0) {
            buf.append(c);
        }
    }

    /**
     * Returns the {@link FormatLocale} which precisely matches {@link Locale},
     * if any, or null if there is none.
     */
    public static FormatLocale getFormatLocale(Locale locale)
    {
        if (locale == null) {
            locale = Locale.US;
        }
        String key = locale.toString();
        return mapLocaleToFormatLocale.get(key);
    }

    /**
     * Returns the best {@link FormatLocale} for a given {@link Locale}.
     * Never returns null, even if <code>locale</code> is null.
     */
    public static synchronized FormatLocale getBestFormatLocale(Locale locale)
    {
        FormatLocale formatLocale;
        if (locale == null) {
            return locale_US;
        }
        String key = locale.toString();
        // Look in the cache first.
        formatLocale = mapLocaleToFormatLocale.get(key);
        if (formatLocale == null) {
            // Not in the cache, so ask the factory.
            formatLocale = getFormatLocaleUsingFactory(locale);
            if (formatLocale == null) {
                formatLocale = locale_US;
            }
            // Add to cache.
            mapLocaleToFormatLocale.put(key, formatLocale);
        }
        return formatLocale;
    }

    private static FormatLocale getFormatLocaleUsingFactory(Locale locale)
    {
        FormatLocale formatLocale;
        // Lookup full locale, e.g. "en-US-Boston"
        if (!locale.getVariant().equals("")) {
            formatLocale = createLocale(locale);
            if (formatLocale != null) {
                return formatLocale;
            }
            locale = new Locale(locale.getLanguage(), locale.getCountry());
        }
        // Lookup language and country, e.g. "en-US"
        if (!locale.getCountry().equals("")) {
            formatLocale = createLocale(locale);
            if (formatLocale != null) {
                return formatLocale;
            }
            locale = new Locale(locale.getLanguage());
        }
        // Lookup language, e.g. "en"
        formatLocale = createLocale(locale);
        if (formatLocale != null) {
            return formatLocale;
        }
        return null;
    }

    /**
     * Registers a {@link FormatLocale} to a given {@link Locale}. Returns the
     * previous mapping.
     */
    public static FormatLocale registerFormatLocale(
        FormatLocale formatLocale, Locale locale)
    {
        String key = locale.toString(); // e.g. "en_us_Boston"
        return mapLocaleToFormatLocale.put(key, formatLocale);
    }

    // Values for variable numberState below.
    static final int NOT_IN_A_NUMBER = 0;
    static final int LEFT_OF_POINT = 1;
    static final int RIGHT_OF_POINT = 2;
    static final int RIGHT_OF_EXP = 3;

    /**
     * Reads formatString up to the first semi-colon, or to the end if there
     * are no semi-colons.  Adds a format to alternateFormatList, and returns
     * the remains of formatString.
     */
    private String parseFormatString(
        String formatString,
        List<BasicFormat> alternateFormatList,
        FormatType[] formatTypeOut)
    {
        // Cache the original value
        final String originalFormatString = formatString;

        // Where we are in a numeric format.
        int numberState = NOT_IN_A_NUMBER;
        StringBuilder ignored = new StringBuilder();
        String prevIgnored = null;
        boolean haveSeenNumber = false;
        int digitsLeftOfPoint = 0,
            digitsRightOfPoint = 0,
            digitsRightOfExp = 0,
            zeroesLeftOfPoint = 0,
            zeroesRightOfPoint = 0,
            zeroesRightOfExp = 0;
        boolean useDecimal = false,
            useThouSep = false,
            fillFromRight = true;

        // Whether to print numbers in decimal or exponential format.  Valid
        // values are FORMAT_NULL, FORMAT_E_PLUS_LOWER, FORMAT_E_MINUS_LOWER,
        // FORMAT_E_PLUS_UPPER, FORMAT_E_MINUS_UPPER.
        int expFormat = FORMAT_NULL;

        // todo: Parse the string for ;s

        // Look for the format string in the table of named formats.
        formatString = MacroToken.expand(locale, formatString);

        // Add a semi-colon to the end of the string so the end of the string
        // looks like the end of an alternate.
        if (!formatString.endsWith(";")) {
            formatString = formatString + ";";
        }

        // Scan through the format string for format elements.
        List<BasicFormat> formatList = new ArrayList<BasicFormat>();
        List<Integer> thousands = new ArrayList<Integer>();
        int decimalShift = 0;
        loop:
        while (formatString.length() > 0) {
            BasicFormat format = null;
            String newFormatString;
            final Token token = findToken(formatString, formatTypeOut[0]);
            if (token != null) {
                String matched = token.token;
                newFormatString = formatString.substring(matched.length());
                if (token.isSpecial()) {
                    switch (token.code) {
                    case FORMAT_SEMI:
                        break loop;

                    case FORMAT_POUND:
                        switch (numberState) {
                        case NOT_IN_A_NUMBER:
                            numberState = LEFT_OF_POINT;
                            // fall through
                        case LEFT_OF_POINT:
                            digitsLeftOfPoint++;
                            break;
                        case RIGHT_OF_POINT:
                            digitsRightOfPoint++;
                            break;
                        case RIGHT_OF_EXP:
                            digitsRightOfExp++;
                            break;
                        default:
                            throw new Error();
                        }
                        break;

                    case FORMAT_0:
                        switch (numberState) {
                        case NOT_IN_A_NUMBER:
                            numberState = LEFT_OF_POINT;
                            // fall through
                        case LEFT_OF_POINT:
                            zeroesLeftOfPoint++;
                            break;
                        case RIGHT_OF_POINT:
                            zeroesRightOfPoint++;
                            break;
                        case RIGHT_OF_EXP:
                            zeroesRightOfExp++;
                            break;
                        default:
                            throw new Error();
                        }
                        break;

                    case FORMAT_M:
                    case FORMAT_MM:
                    {
                        // "m" and "mm" mean minute if immediately after
                        // "h" or "hh"; month otherwise.
                        boolean theyMeantMinute = false;
                        int j = formatList.size() - 1;
                        while (j >= 0) {
                            BasicFormat prevFormat = formatList.get(j);
                            if (prevFormat instanceof LiteralFormat) {
                                // ignore boilerplate
                                j--;
                            } else if (prevFormat.code == FORMAT_H
                                       || prevFormat.code == FORMAT_HH
                                       || prevFormat.code == FORMAT_HH_UPPER)
                            {
                                theyMeantMinute = true;
                                break;
                            } else {
                                theyMeantMinute = false;
                                break;
                            }
                        }
                        if (theyMeantMinute) {
                            format = new DateFormat(
                                (token.code == FORMAT_M
                                    ? FORMAT_N
                                    : FORMAT_NN),
                                matched,
                                locale,
                                false);
                        } else {
                            format = token.makeFormat(locale);
                        }
                        break;
                    }

                    case FORMAT_DECIMAL:
                    {
                        if (numberState == LEFT_OF_POINT) {
                            decimalShift =
                                fixThousands(
                                    thousands, formatString, decimalShift);
                        }
                        numberState = RIGHT_OF_POINT;
                        useDecimal = true;
                        break;
                    }

                    case FORMAT_THOUSEP:
                    {
                        if (numberState == LEFT_OF_POINT) {
                            // e.g. "#,##"
                            useThouSep = true;
                            thousands.add(formatString.length());
                        } else {
                            // e.g. "ddd, mmm dd, yyy"
                            format = token.makeFormat(locale);
                        }
                        break;
                    }

                    case FORMAT_TIMESEP:
                    {
                        format = new LiteralFormat(locale.timeSeparator);
                        break;
                    }

                    case FORMAT_DATESEP:
                    {
                        format = new LiteralFormat(locale.dateSeparator);
                        break;
                    }

                    case FORMAT_BACKSLASH:
                    {
                        // Display the next character in the format string.
                        String s;
                        if (formatString.length() == 1) {
                            // Backslash is the last character in the
                            // string.
                            s = "";
                            newFormatString = "";
                        } else {
                            s = formatString.substring(1, 2);
                            newFormatString = formatString.substring(2);
                        }
                        format = new LiteralFormat(s);
                        break;
                    }

                    case FORMAT_E_MINUS_UPPER:
                    case FORMAT_E_PLUS_UPPER:
                    case FORMAT_E_MINUS_LOWER:
                    case FORMAT_E_PLUS_LOWER:
                    {
                        if (numberState == LEFT_OF_POINT) {
                            decimalShift =
                                fixThousands(
                                    thousands, formatString, decimalShift);
                        }
                        numberState = RIGHT_OF_EXP;
                        expFormat = token.code;
                        if (zeroesLeftOfPoint == 0
                            && zeroesRightOfPoint == 0)
                        {
                            // We need a mantissa, so that format(123.45,
                            // "E+") gives "1E+2", not "0E+2" or "E+2".
                            zeroesLeftOfPoint = 1;
                        }
                        break;
                    }

                    case FORMAT_QUOTE:
                    {
                        // Display the string inside the double quotation
                        // marks.
                        String s;
                        int j = formatString.indexOf("\"", 1);
                        if (j == -1) {
                            // The string did not contain a closing quote.
                            // Use the whole string.
                            s = formatString.substring(1);
                            newFormatString = "";
                        } else {
                            // Take the string inside the quotes.
                            s = formatString.substring(1, j);
                            newFormatString = formatString.substring(
                                j + 1);
                        }
                        format = new LiteralFormat(s);
                        break;
                    }

                    case FORMAT_UPPER:
                    {
                        format =
                            new StringFormat(
                                StringCase.UPPER, ">", locale.locale);
                        break;
                    }

                    case FORMAT_LOWER:
                    {
                        format =
                            new StringFormat(
                                StringCase.LOWER, "<", locale.locale);
                        break;
                    }

                    case FORMAT_FILL_FROM_LEFT:
                    {
                        fillFromRight = false;
                        break;
                    }

                    case FORMAT_GENERAL_NUMBER:
                    {
                        format = new JavaFormat(locale.locale);
                        break;
                    }

                    case FORMAT_GENERAL_DATE:
                    {
                        format = new JavaFormat(locale.locale);
                        break;
                    }

                    case FORMAT_INTL_CURRENCY:
                    {
                        format = new LiteralFormat(locale.currencySymbol);
                        break;
                    }

                    default:
                        throw new Error();
                    }
                    if (formatTypeOut[0] == null) {
                        formatTypeOut[0] = token.getFormatType();
                    }
                    if (format == null) {
                        // If the special-case code does not set format,
                        // we should not create a format element.  (The
                        // token probably caused some flag to be set.)
                        ignored.append(matched);
                    } else {
                        prevIgnored = ignored.toString();
                        ignored.setLength(0);
                    }
                } else {
                    format = token.makeFormat(locale);
                }
            } else {
                // None of the standard format elements matched.  Make the
                // current character into a literal.
                format = new LiteralFormat(
                    formatString.substring(0, 1));
                newFormatString = formatString.substring(1);
            }

            if (format != null) {
                if (numberState != NOT_IN_A_NUMBER) {
                    // Having seen a few number tokens, we're looking at a
                    // non-number token.  Create the number first.
                    if (numberState == LEFT_OF_POINT) {
                        decimalShift =
                            fixThousands(
                                thousands, formatString, decimalShift);
                    }
                    NumericFormat numericFormat = new NumericFormat(
                        prevIgnored, locale, expFormat, digitsLeftOfPoint,
                        zeroesLeftOfPoint, digitsRightOfPoint,
                        zeroesRightOfPoint, digitsRightOfExp, zeroesRightOfExp,
                        useDecimal, useThouSep, originalFormatString);
                    formatList.add(numericFormat);
                    numberState = NOT_IN_A_NUMBER;
                    haveSeenNumber = true;
                }

                formatList.add(format);
                if (formatTypeOut[0] == null) {
                    formatTypeOut[0] = format.getFormatType();
                }
            }

            formatString = newFormatString;
        }

        if (numberState != NOT_IN_A_NUMBER) {
            // We're still in a number.  Create a number format.
            if (numberState == LEFT_OF_POINT) {
                decimalShift =
                    fixThousands(
                        thousands, formatString, decimalShift);
            }
            NumericFormat numericFormat = new NumericFormat(
                prevIgnored, locale, expFormat, digitsLeftOfPoint,
                zeroesLeftOfPoint, digitsRightOfPoint, zeroesRightOfPoint,
                digitsRightOfExp, zeroesRightOfExp, useDecimal, useThouSep,
                originalFormatString);
            formatList.add(numericFormat);
            numberState = NOT_IN_A_NUMBER;
            haveSeenNumber = true;
        }

        if (formatString.startsWith(";")) {
            formatString = formatString.substring(1);
        }

        // If they used some symbol like 'AM/PM' in the format string, tell all
        // date formats to use twelve hour clock.  Likewise, figure out the
        // multiplier implied by their use of "%" or ",".
        boolean twelveHourClock = false;
        // User 24 hour format if HH is used in the format String. This follows
        // Java's date format convention.
        boolean isFormatHH = false;
        for (int i = 0; i < formatList.size(); i++) {
            switch (formatList.get(i).code) {
            case FORMAT_HH_UPPER:
                isFormatHH = true;
                break;
            case FORMAT_UPPER_AM_SOLIDUS_PM:
            case FORMAT_LOWER_AM_SOLIDUS_PM:
            case FORMAT_UPPER_A_SOLIDUS_P:
            case FORMAT_LOWER_A_SOLIDUS_P:
            case FORMAT_AMPM:
                twelveHourClock = true & !isFormatHH;
                break;

            case FORMAT_PERCENT:
                // If "%" occurs, the number should be multiplied by 100.
                decimalShift += 2;
                break;

            case FORMAT_THOUSEP:
                // If there is a thousands separator (",") immediately to the
                // left of the point, or at the end of the number, divide the
                // number by 1000.  (Or by 1000^n if there are more than one.)
                if (haveSeenNumber
                    && i + 1 < formatList.size())
                {
                    final BasicFormat nextFormat = formatList.get(i + 1);
                    if (nextFormat.code != FORMAT_THOUSEP
                        && nextFormat.code != FORMAT_0
                        && nextFormat.code != FORMAT_POUND)
                    {
                        for (int j = i;
                            j >= 0 && formatList.get(j).code == FORMAT_THOUSEP;
                            j--)
                        {
                            decimalShift -= 3;
                            formatList.remove(j); // ignore
                            --i;
                        }
                    }
                }
                break;

            default:
            }
        }

        if (twelveHourClock) {
            for (int i = 0; i < formatList.size(); i++) {
                if (formatList.get(i) instanceof DateFormat) {
                    ((DateFormat) formatList.get(i)).setTwelveHourClock(true);
                }
            }
        }

        if (decimalShift != 0) {
            for (int i = 0; i < formatList.size(); i++) {
                if (formatList.get(i) instanceof NumericFormat) {
                    ((NumericFormat) formatList.get(i)).decimalShift =
                        decimalShift;
                }
            }
        }

        // Merge adjacent literal formats.
        //
        // Must do this AFTER adjusting for percent. Otherwise '%' and following
        // '|' might be merged into a plain literal, and '%' would lose its
        // special powers.
        for (int i = 0; i < formatList.size(); ++i) {
            if (i > 0
                && formatList.get(i) instanceof LiteralFormat
                && formatList.get(i - 1) instanceof LiteralFormat)
            {
                formatList.set(
                    i - 1,
                    new LiteralFormat(
                        ((LiteralFormat) formatList.get(i - 1)).s
                        + ((LiteralFormat) formatList.get(i)).s));
                formatList.remove(i);
                --i;
            }
        }

        // Create a CompoundFormat containing all of the format elements.
        // This is the end of an alternate - or of the whole format string.
        // Push the current list of formats onto the list of alternates.

        BasicFormat alternateFormat;
        switch (formatList.size()) {
        case 0:
            alternateFormat = null;
            break;
        case 1:
            alternateFormat = formatList.get(0);
            break;
        default:
            alternateFormat =
                new CompoundFormat(
                    formatList.toArray(
                        new BasicFormat[formatList.size()]));
            break;
        }
        alternateFormatList.add(alternateFormat);
        return formatString;
    }

    private Token findToken(String formatString, FormatType formatType) {
        for (int i = tokens.length - 1; i > 0; i--) {
            final Token token = tokens[i];
            if (formatString.startsWith(token.token)
                && token.compatibleWith(formatType))
            {
                return token;
            }
        }
        return null;
    }

    private int fixThousands(
        List<Integer> thousands, String formatString, int shift)
    {
        int offset = formatString.length() + 1;
        for (int i = thousands.size() - 1; i >= 0; i--) {
            Integer integer = thousands.get(i);
            thousands.set(i, integer - offset);
            ++offset;
        }
        while (thousands.size() > 0
            && thousands.get(thousands.size() - 1) == 0)
        {
            shift -= 3;
            thousands.remove(thousands.size() - 1);
        }
        return shift;
    }

    public String format(Object o)
    {
        StringBuilder buf = new StringBuilder();
        format(o, buf);
        return buf.toString();
    }

    private StringBuilder format(Object o, StringBuilder buf) {
        if (o == null) {
            format.formatNull(buf);
        } else {
            // For final classes, it is more efficient to switch using
            // class equality than using 'instanceof'.
            Class<? extends Object> clazz = o.getClass();
            if (clazz == Double.class) {
                format.format((Double) o, buf);
            } else if (clazz == Float.class) {
                format.format((Float) o, buf);
            } else if (clazz == Integer.class) {
                format.format((Integer) o, buf);
            } else if (clazz == Long.class) {
                format.format((Long) o, buf);
            } else if (clazz == Short.class) {
                format.format((Short) o, buf);
            } else if (clazz == Byte.class) {
                format.format((Byte) o, buf);
            } else if (o instanceof BigDecimal) {
                format.format(
                    (BigDecimal) o, buf); // PDI-16761 if we cast it to double type we lose precision
            } else if (o instanceof BigInteger) {
                format.format(
                    ((BigInteger) o).longValue(), buf);
            } else if (clazz == String.class) {
                format.format((String) o, buf);
            } else if (o instanceof java.util.Date) {
                // includes java.sql.Date, java.sql.Time and java.sql.Timestamp
                format.format((Date) o, buf);
            } else if (o instanceof Calendar) {
                format.format((Calendar) o, buf);
            } else {
                buf.append(o.toString());
            }
        }
        return buf;
    }

    public String getFormatString()
    {
        return formatString;
    }

    private static void shift(
        MondrianFloatingDecimal fd,
        int i)
    {
        if (fd.isExceptional
            || fd.nDigits == 1 && fd.digits[0] == '0')
        {
            ; // don't multiply zero
        } else {
            fd.decExponent += i;
        }
    }

    /** Formats a floating decimal to a given buffer. */
    private static void formatFd0(
        MondrianFloatingDecimal fd,
        StringBuilder buf,
        int minDigitsLeftOfDecimal,
        char decimalChar, // '.' or ','
        int minDigitsRightOfDecimal,
        int maxDigitsRightOfDecimal,
        char expChar, // 'E' or 'e'
        boolean expSign, // whether to print '+' if exp is positive
        int minExpDigits, // minimum digits in exponent
        char thousandChar, // ',' or '.', or 0
        boolean useDecimal,
        ArrayStack<Integer> thousandSeparatorPositions)
    {
        // char result[] = new char[nDigits + 10]; // crashes for 1.000.000,00
        // the result length does *not* depend from nDigits
        //  it is : decExponent
        //         +maxDigitsRightOfDecimal
        //          + 10  (for decimal point and sign or -Infinity)
        //         +decExponent/3 (for the thousand separators)
        // crashes e.g. for 1.1 and format '0000000000000'
        int resultLen =
            10
            + Math.max(
                Math.abs(fd.decExponent),
                minDigitsLeftOfDecimal) * 4 / 3
            + maxDigitsRightOfDecimal;
        char result[] = new char[resultLen];
        int i = formatFd1(
            fd,
            result,
            0,
            minDigitsLeftOfDecimal,
            decimalChar,
            minDigitsRightOfDecimal,
            maxDigitsRightOfDecimal,
            expChar,
            expSign,
            minExpDigits,
            thousandChar,
            useDecimal,
            thousandSeparatorPositions);
        buf.append(result, 0, i);
    }

    /** Formats a floating decimal to a given char array. */
    private static int formatFd1(
        MondrianFloatingDecimal fd,
        char result[],
        int i,
        int minDigitsLeftOfDecimal,
        char decimalChar, // '.' or ','
        int minDigitsRightOfDecimal,
        int maxDigitsRightOfDecimal,
        char expChar, // 'E' or 'e'
        boolean expSign, // whether to print '+' if exp is positive
        int minExpDigits, // minimum digits in exponent
        char thousandChar, // ',' or '.' or 0
        boolean useDecimal,
        ArrayStack<Integer> thousandSeparatorPositions)
    {
        if (expChar != 0) {
            // Print the digits left of the 'E'.
            int oldExp = fd.decExponent;
            fd.decExponent = Math.min(minDigitsLeftOfDecimal, fd.nDigits);
            boolean oldIsNegative = fd.isNegative;
            fd.isNegative = false;
            i = formatFd2(
                fd,
                result,
                i,
                minDigitsLeftOfDecimal,
                decimalChar,
                minDigitsRightOfDecimal,
                maxDigitsRightOfDecimal,
                '\0',
                useDecimal,
                thousandSeparatorPositions);
            fd.decExponent = oldExp;
            fd.isNegative = oldIsNegative;

            result[i++] = expChar;
            // Print the digits right of the 'E'.
            return fd.formatExponent(result, i, expSign, minExpDigits);
        } else {
            return formatFd2(
                fd,
                result,
                i,
                minDigitsLeftOfDecimal,
                decimalChar,
                minDigitsRightOfDecimal,
                maxDigitsRightOfDecimal,
                thousandChar,
                useDecimal,
                thousandSeparatorPositions);
        }
    }

    static int formatFd2(
        MondrianFloatingDecimal fd,
        char result[],
        int i,
        int minDigitsLeftOfDecimal,
        char decimalChar, // '.' or ','
        int minDigitsRightOfDecimal,
        int maxDigitsRightOfDecimal,
        char thousandChar, // ',' or '.' or 0
        boolean useDecimal,
        ArrayStack<Integer> thousandSeparatorPositions)
    {
        if (fd.isNegative) {
            result[i++] = '-';
        }
        if (fd.isExceptional) {
            System.arraycopy(fd.digits, 0, result, i, fd.nDigits);
            return i + fd.nDigits;
        }
        // Build a new array of digits, padded with 0s at either end.  For
        // example, here is the array we would build for 1234.56.
        //
        // |  0     0     1     2     3  .  4     5     6     0     0   |
        // |           |- nDigits=6 -----------------------|            |
        // |           |- decExponent=3 -|                              |
        // |- minDigitsLeftOfDecimal=5 --|                              |
        // |                             |- minDigitsRightOfDecimal=5 --|
        // |- wholeDigits=5 -------------|- fractionDigits=5 -----------|
        // |- totalDigits=10 -------------------------------------------|
        // |                             |- maxDigitsRightOfDecimal=5 --|
        int wholeDigits = Math.max(fd.decExponent, minDigitsLeftOfDecimal),
            fractionDigits = Math.max(
                fd.nDigits - fd.decExponent, minDigitsRightOfDecimal),
            totalDigits = wholeDigits + fractionDigits;
        char[] digits2 = new char[totalDigits];
        for (int j = 0; j < totalDigits; j++) {
            digits2[j] = '0';
        }
        for (int j = 0; j < fd.nDigits; j++) {
            digits2[wholeDigits - fd.decExponent + j] = fd.digits[j];
        }

        // Now round.  Suppose that we want to round 1234.56 to 1 decimal
        // place (that is, maxDigitsRightOfDecimal = 1).  Then lastDigit
        // initially points to '5'.  We find out that we need to round only
        // when we see that the next digit ('6') is non-zero.
        //
        // |  0     0     1     2     3  .  4     5     6     0     0   |
        // |                             |  ^   |                       |
        // |                                maxDigitsRightOfDecimal=1   |
        int lastDigit = wholeDigits + maxDigitsRightOfDecimal;
        if (lastDigit < totalDigits) {
            // We need to truncate -- also round if the trailing digits are
            // 5000... or greater.
            int m = totalDigits;
            if ( digits2.length >= lastDigit && lastDigit != 0 ) {
              while ( digits2[lastDigit - 1] < '0' || digits2[lastDigit - 1] > '9' ) {
                // BACKLOG-15504
                lastDigit--;
              }
            }
            while (true) {
                m--;
                if (m < 0) {
                    // The entire number was 9s.  Re-allocate, so we can
                    // prepend a '1'.
                    wholeDigits++;
                    totalDigits++;
                    lastDigit++;
                    char[] old = digits2;
                    digits2 = new char[totalDigits];
                    digits2[0] = '1';
                    System.arraycopy(old, 0, digits2, 1, old.length);
                    break;
                } else if (m == lastDigit) {
                    char d = digits2[m];
                    digits2[m] = '0';
                    if (d < '5' || d == ':') {
                        break; // no need to round
                    }
                } else if (m > lastDigit) {
                    digits2[m] = '0';
                } else if (digits2[m] == '9') {
                    digits2[m] = '0';
                    // do not break - we have to carry
                } else {
                    digits2[m]++;
                    break; // nothing to carry
                }
            }
        }

        // Find the first non-zero digit and the last non-zero digit.
        int firstNonZero = wholeDigits,
            firstTrailingZero = 0;
        for (int j = 0; j < totalDigits; j++) {
            if (digits2[j] != '0') {
                if (j < firstNonZero) {
                    firstNonZero = j;
                }
                firstTrailingZero = j + 1;
            }
        }

        int firstDigitToPrint = firstNonZero;
        if (firstDigitToPrint > wholeDigits - minDigitsLeftOfDecimal) {
            firstDigitToPrint = wholeDigits - minDigitsLeftOfDecimal;
        }
        int lastDigitToPrint = firstTrailingZero;
        if (lastDigitToPrint > wholeDigits + maxDigitsRightOfDecimal) {
            lastDigitToPrint = wholeDigits + maxDigitsRightOfDecimal;
        }
        if (lastDigitToPrint < wholeDigits + minDigitsRightOfDecimal) {
            lastDigitToPrint = wholeDigits + minDigitsRightOfDecimal;
        }

        if (thousandChar != '\0'
            && thousandSeparatorPositions.size() > 0)
        {
            // Now print the number. That will happen backwards, so we
            // store it temporarily and then invert.
            ArrayStack<Character> formattedWholeDigits =
                new ArrayStack<Character>();
            // We need to keep track of how many digits we printed in the
            // current token.
            int nbInserted = 0;
            for (int j = wholeDigits - 1; j >= firstDigitToPrint; j--) {
                // Check if we need to insert another thousand separator
                if (nbInserted % thousandSeparatorPositions.peek() == 0
                    && nbInserted > 0)
                {
                    formattedWholeDigits.push(thousandChar);
                    nbInserted = 0;
                    // The last format token is kept because we re-apply it
                    // until the end of the digits.
                    if (thousandSeparatorPositions.size() > 1) {
                        thousandSeparatorPositions.pop();
                    }
                }
                // Insert the next digit.
                formattedWholeDigits.push(digits2[j]);
                nbInserted++;
            }
            // We're done. Invert the print out and add it to
            // the result array.
            while (formattedWholeDigits.size() > 0) {
                result[i++] = formattedWholeDigits.pop();
            }
        } else {
            // There are no thousand separators. Just put the
            // digits in the results array.
            for (int j = firstDigitToPrint; j < wholeDigits; j++) {
                result[i++] = digits2[j];
            }
        }

        if (wholeDigits < lastDigitToPrint
            || (useDecimal
            && wholeDigits == lastDigitToPrint))
        {
            result[i++] = decimalChar;
        }
        for (int j = wholeDigits; j < lastDigitToPrint; j++) {
            result[i++] = digits2[j];
        }
        return i;
    }

    private enum FormatType {
        STRING,
        DATE,
        NUMERIC
    }

    private static class DummyDecimalFormat extends DecimalFormat {
        private FieldPosition pos;

        public StringBuffer format(
            double number,
            StringBuffer result,
            FieldPosition fieldPosition)
        {
            pos = fieldPosition;
            return result;
        }
    }

    /** Specification for MondrianFloatingDecimal. */
    private static class MondrianFloatingDecimalSpec {
        boolean     isExceptional;
        boolean     isNegative;
        int         decExponent;
        char        digits[];
        int         nDigits;

        /** Creates a floating decimal with a given value. */
        MondrianFloatingDecimalSpec(double n) {
        }

        /**
         * Appends {@link #decExponent} to result string. Returns i plus the
         * number of chars written.
         *
         * <p>Implementation may assume that exponent has 3 or fewer digits.</p>
         *
         * <p>For example, given {@code decExponent} = 2,
         * {@code formatExponent(result, 5, true, 2)}
         * will write '0' into result[5]
         * and '2' into result[6] and return 7.</p>
         *
         * @param result Result buffer
         * @param i Initial offset into result buffer
         * @param expSign Whether to print a '+' sign if exponent is positive
         *                (always prints '-' if negative)
         * @param minExpDigits Minimum number of digits to write
         * @return Offset into result buffer after writing chars
         */
        int formatExponent(
            char[] result,
            int i,
            boolean expSign,
            int minExpDigits)
        {
            return i;
        }

        /**
         * Handles an exceptional number. If {@link #isExceptional} is false,
         * does nothing. If {@link #isExceptional} is true, appends the contents
         * of {@link #digits} to result starting from i and returns the
         * incremented i.
         *
         * @param result Result buffer
         * @param i Initial offset into result buffer
         * @return Offset into result buffer after writing chars
         */
        int handleExceptional(char[] result, int i) {
            return i;
        }

        /**
         * Handles a negative number. If {@link #isNegative}, appends '-' to
         * result at i and returns i + 1; otherwise does nothing and returns i.
         *
         * @param result Result buffer
         * @param i Initial offset into result buffer
         * @return Offset into result buffer after writing chars
         */
        int handleNegative(char[] result, int i) {
            return i;
        }
    }
}

// End Format.java
