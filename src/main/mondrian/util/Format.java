/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2000-2005 Julian Hyde
// Copyright (C) 2005-2011 Pentaho and others
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
     * Gets the dummy implementation of {@link FieldPosition} which the JDK
     * uses when you don't care about the status of a call to
     * {@link Format#format}.
     */
    private static FieldPosition createDummyFieldPos() {
        final DummyDecimalFormat format1 = new DummyDecimalFormat();
        format1.format(0.0);
        return format1.pos;
    }

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
     * Maps macro token names with their related object. Used
     * to fast-resolve a macro token without iterating.
     */
    private static final Map<String, MacroToken> macroTokenMap =
        new HashMap<String, MacroToken>();

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
     * See the <a href="http://www.apostate.com/programming/vb-format.html">the
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
     * <p>It is implemented using {@link FloatingDecimal}, which is a
     * barely-modified version of <code>java.lang.FloatingDecimal</code>.
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
            if (macroTokenMap.containsKey(formatString)) {
                MacroToken macroToken = macroTokenMap.get(formatString);
                if (macroToken.name.equals("Currency")) {
                    formatString = locale.currencyFormat
                        + ";("  + locale.currencyFormat + ")";
                } else {
                    formatString = macroToken.translation;
                }
            }

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

                        // Now we build a list of the token lenghts in
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
            FloatingDecimal fd = new FloatingDecimal(n);
            fd.shift(decimalShift);
            final int formatDigitsRightOfPoint =
                zeroesRightOfPoint + digitsRightOfPoint;
            if (n == 0.0 || (n < 0 && !shows(fd, formatDigitsRightOfPoint))) {
                // Underflow of negative number. Make it zero, so there is no
                // '-' sign.
                fd = new FloatingDecimal(0);
            }
            String s = fd.toJavaFormatString(
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
            buf.append(s);
        }

        boolean isApplicableTo(double n) {
            if (n >= 0) {
                return true;
            }
            FloatingDecimal fd = new FloatingDecimal(n);
            fd.shift(decimalShift);
            final int formatDigitsRightOfPoint =
                zeroesRightOfPoint + digitsRightOfPoint;
            return shows(fd, formatDigitsRightOfPoint);
        }

        private static boolean shows(
            FloatingDecimal fd,
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
            mondrian.util.Format.FloatingDecimal fd =
                new mondrian.util.Format.FloatingDecimal(n);
            fd.shift(decimalShift);
            String s = fd.toJavaFormatString(
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
            buf.append(s);
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
            {
                int m = calendar.get(Calendar.MONTH) + 1; // 0-based
                buf.append(m);
                break;
            }
            case FORMAT_MM:
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
            FORMAT_MM,
            DATE | SPECIAL,
            "mm",
            null,
            "Display the month as a number with a leading zero (01 - 12). If m "
            + "immediately follows h or hh, the minute rather than the month "
            + "is displayed."),
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
    };

    static class MacroToken {
        String name;
        String translation;
        String description;

        MacroToken(String name, String translation, String description)
        {
            this.name = name;
            this.translation = translation;
            this.description = description;
            macroTokenMap.put(name, this);
        }
    }

    // Named formats.  todo: Supply the translation strings.
    private static final MacroToken[] macroTokens = {
        new MacroToken(
            "Currency",
            null,
            "Shows currency values according to the locale's CurrencyFormat.  "
            + "Negative numbers are inside parentheses."),
        new MacroToken(
            "Fixed", "0", "Shows at least one digit."),
        new MacroToken(
            "Standard", "#,##0", "Uses a thousands separator."),
        new MacroToken(
            "Percent",
            "0.00%",
            "Multiplies the value by 100 with a percent sign at the end."),
        new MacroToken(
            "Scientific", "0.00e+00", "Uses standard scientific notation."),
        new MacroToken(
            "Long Date",
            "dddd, mmmm dd, yyyy",
            "Uses the Long Date format specified in the Regional Settings "
            + "dialog box of the Microsoft Windows Control Panel."),
        new MacroToken(
            "Medium Date",
            "dd-mmm-yy",
            "Uses the dd-mmm-yy format (for example, 03-Apr-93)"),
        new MacroToken(
            "Short Date",
            "m/d/yy",
            "Uses the Short Date format specified in the Regional Settings "
            + "dialog box of the Windows Control Panel."),
        new MacroToken(
            "Long Time",
            "h:mm:ss AM/PM",
            "Shows the hour, minute, second, and \"AM\" or \"PM\" using the "
            + "h:mm:ss format."),
        new MacroToken(
            "Medium Time",
            "h:mm AM/PM",
            "Shows the hour, minute, and \"AM\" or \"PM\" using the \"hh:mm "
            + "AM/PM\" format."),
        new MacroToken(
            "Short Time",
            "hh:mm",
            "Shows the hour and minute using the hh:mm format."),
        new MacroToken(
            "Yes/No",
            "\\Y\\e\\s;\\Y\\e\\s;\\N\\o;\\N\\o",
            "Any nonzero numeric value (usually - 1) is Yes. Zero is No."),
        new MacroToken(
            "True/False",
            "\\T\\r\\u\\e;\\T\\r\\u\\e;\\F\\a\\l\\s\\e;\\F\\a\\l\\s\\e",
            "Any nonzero numeric value (usually - 1) is True. Zero is False."),
        new MacroToken(
            "On/Off",
            "\\O\\n;\\O\\n;\\O\\f\\f;\\O\\f\\f",
            "Any nonzero numeric value (usually - 1) is On. Zero is Off."),
    };

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
        calendar.set(1969, 11, 31, 0, 0, 0);
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

        /*
         * If the locale passed is only a language, Java cannot
         * resolve the currency symbol and will instead return
         * u00a4 (The international currency symbol). For those cases,
         * we use the default system locale currency symbol.
         */
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
        if (macroTokenMap.containsKey(formatString)) {
            MacroToken macroToken = macroTokenMap.get(formatString);
            if (macroToken.translation == null) {
                // this macro requires special-case code
                if (macroToken.name.equals("Currency")) {
                    // e.g. "$#,##0.00;($#,##0.00)"
                    formatString = locale.currencyFormat
                                   + ";("  + locale.currencyFormat + ")";
                } else {
                    throw new Error(
                        "Format: internal: token " + macroToken.name
                        + " should have translation");
                }
            } else {
                formatString = macroToken.translation;
            }
        }

        // Add a semi-colon to the end of the string so the end of the string
        // looks like the end of an alternate.
        if (!formatString.endsWith(";")) {
            formatString = formatString + ";";
        }

        // Scan through the format string for format elements.
        List<BasicFormat> formatList = new ArrayList<BasicFormat>();
        loop:
        while (formatString.length() > 0) {
            BasicFormat format = null;
            String newFormatString = null;
            boolean ignoreToken = false;
            for (int i = tokens.length - 1; i > 0; i--) {
                Token token = tokens[i];
                if (!formatString.startsWith(token.token)) {
                    continue;
                }
                if (!token.compatibleWith(formatTypeOut[0])) {
                    continue;
                }
                String matched = token.token;
                newFormatString = formatString.substring(matched.length());
                if (token.isSpecial()) {
                    switch (token.code) {
                    case FORMAT_SEMI:
                        formatString = newFormatString;
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
                                       || prevFormat.code == FORMAT_HH)
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
                        numberState = RIGHT_OF_POINT;
                        useDecimal = true;
                        break;
                    }

                    case FORMAT_THOUSEP:
                    {
                        if (numberState == LEFT_OF_POINT) {
                            // e.g. "#,##"
                            useThouSep = true;
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
                        ignoreToken = true;
                        ignored.append(matched);
                    } else {
                        prevIgnored = ignored.toString();
                        ignored.setLength(0);
                    }
                } else {
                    format = token.makeFormat(locale);
                }
                break;
            }

            if (format == null && !ignoreToken) {
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
            NumericFormat numericFormat = new NumericFormat(
                prevIgnored, locale, expFormat, digitsLeftOfPoint,
                zeroesLeftOfPoint, digitsRightOfPoint, zeroesRightOfPoint,
                digitsRightOfExp, zeroesRightOfExp, useDecimal, useThouSep,
                originalFormatString);
            formatList.add(numericFormat);
            numberState = NOT_IN_A_NUMBER;
            haveSeenNumber = true;
        }

        // If they used some symbol like 'AM/PM' in the format string, tell all
        // date formats to use twelve hour clock.  Likewise, figure out the
        // multiplier implied by their use of "%" or ",".
        boolean twelveHourClock = false;
        int decimalShift = 0;
        for (int i = 0; i < formatList.size(); i++) {
            switch (formatList.get(i).code) {
            case FORMAT_UPPER_AM_SOLIDUS_PM:
            case FORMAT_LOWER_AM_SOLIDUS_PM:
            case FORMAT_UPPER_A_SOLIDUS_P:
            case FORMAT_LOWER_A_SOLIDUS_P:
            case FORMAT_AMPM:
                twelveHourClock = true;
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
                    ((BigDecimal) o).doubleValue(), buf);
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

    /**
     * Locates a {@link Format.FormatLocale} for a given locale.
     */
    public interface LocaleFormatFactory {
        FormatLocale get(Locale locale);
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

/**
 * Copied from <code>java.lang.FloatingDecimal</code>.
 */
static class FloatingDecimal {
    boolean     isExceptional;
    boolean     isNegative;
    int         decExponent;
    char        digits[];
    int         nDigits;

    /*
     * Constants of the implementation
     * Most are IEEE-754 related.
     * (There are more really boring constants at the end.)
     */
    static final long   signMask = 0x8000000000000000L;
    static final long   expMask  = 0x7ff0000000000000L;
    static final long   fractMask = ~(signMask | expMask);
    static final int    expShift = 52;
    static final int    expBias  = 1023;
    static final long   fractHOB = (1L << expShift); // assumed High-Order bit
    static final long   expOne = ((long)expBias) << expShift; // exponent of 1.0
    static final int    maxSmallBinExp = 62;
    static final int    minSmallBinExp = -(63 / 3);

    static final long   highbyte = 0xff00000000000000L;
    static final long   highbit  = 0x8000000000000000L;
    static final long   lowbytes = ~highbyte;

    static final int    singleSignMask =    0x80000000;
    static final int    singleExpMask  =    0x7f800000;
    static final int    singleFractMask =   ~(singleSignMask | singleExpMask);
    static final int    singleExpShift  =   23;
    static final int    singleFractHOB  =   1 << singleExpShift;
    static final int    singleExpBias   =   127;

    /**
     * count number of bits from high-order 1 bit to low-order 1 bit,
     * inclusive.
     */
    private static int
    countBits(long v) {
        //
        // the strategy is to shift until we get a non-zero sign bit
        // then shift until we have no bits left, counting the difference.
        // we do byte shifting as a hack. Hope it helps.
        //
        if (v == 0L) {
            return 0;
        }

        while ((v & highbyte) == 0L) {
            v <<= 8;
        }
        while (v > 0L) { // i.e. while ((v&highbit) == 0L )
            v <<= 1;
        }

        int n = 0;
        while ((v & lowbytes) != 0L) {
            v <<= 8;
            n += 8;
        }
        while (v != 0L) {
            v <<= 1;
            n += 1;
        }
        return n;
    }

    /**
     * Keep big powers of 5 handy for future reference.
     */
    private static FDBigInt b5p[];

    private static FDBigInt
    big5pow(int p) {
        if (p < 0) {
            throw new RuntimeException("Assertion botch: negative power of 5");
        }
        if (b5p == null) {
            b5p = new FDBigInt[p + 1];
        } else if (b5p.length <= p) {
            FDBigInt t[] = new FDBigInt[p + 1];
            System.arraycopy(b5p, 0, t, 0, b5p.length);
            b5p = t;
        }
        if (b5p[p] != null) {
            return b5p[p];
        } else if (p < small5pow.length) {
            return b5p[p] = new FDBigInt(small5pow[p]);
        } else if (p < long5pow.length) {
            return b5p[p] = new FDBigInt(long5pow[p]);
        } else {
            // construct the damn thing.
            // recursively.
            int q, r;
            // in order to compute 5^p,
            // compute its square root, 5^(p/2) and square.
            // or, let q = p / 2, r = p -q, then
            // 5^p = 5^(q+r) = 5^q * 5^r
            q = p >> 1;
            r = p - q;
            FDBigInt bigq =  b5p[q];
            if (bigq == null) {
                bigq = big5pow(q);
            }
            if (r < small5pow.length) {
                return (b5p[p] = bigq.mult(small5pow[r]));
            } else {
                FDBigInt bigr = b5p[r];
                if (bigr == null) {
                    bigr = big5pow(r);
                }
                return (b5p[p] = bigq.mult(bigr));
            }
        }
    }

    /**
     * This is the easy subcase --
     * all the significant bits, after scaling, are held in lvalue.
     * negSign and decExponent tell us what processing and scaling
     * has already been done. Exceptional cases have already been
     * stripped out.
     *
     * <p>In particular:
     * lvalue is a finite number (not Inf, nor NaN)
     * lvalue > 0L (not zero, nor negative).
     *
     * <p>The only reason that we develop the digits here, rather than
     * calling on Long.toString() is that we can do it a little faster,
     * and besides want to treat trailing 0s specially. If Long.toString
     * changes, we should re-evaluate this strategy!
     */
    private void
    developLongDigits(int decExponent, long lvalue, long insignificant) {
        char digits[];
        int  ndigits;
        int  digitno;
        int  c;
        //
        // Discard non-significant low-order bits, while rounding,
        // up to insignificant value.
        int i;
        for (i = 0; insignificant >= 10L; i++) {
            insignificant /= 10L;
        }
        if (i != 0) {
            long pow10 = long5pow[i] << i; // 10^i == 5^i * 2^i;
            long residue = lvalue % pow10;
            lvalue /= pow10;
            decExponent += i;
            if (residue >= (pow10 >> 1)) {
                // round up based on the low-order bits we're discarding
                lvalue++;
            }
        }
        if (lvalue <= Integer.MAX_VALUE) {
            if (lvalue <= 0L) {
                throw new RuntimeException(
                    "Assertion botch: value " + lvalue + " <= 0");
            }
            // even easier subcase!
            // can do int arithmetic rather than long!
            int  ivalue = (int)lvalue;
            digits = new char[ndigits = 10];
            digitno = ndigits - 1;
            c = ivalue % 10;
            ivalue /= 10;
            while (c == 0) {
                decExponent++;
                c = ivalue % 10;
                ivalue /= 10;
            }
            while (ivalue != 0) {
                digits[digitno--] = (char)(c + '0');
                decExponent++;
                c = ivalue % 10;
                ivalue /= 10;
            }
            digits[digitno] = (char)(c + '0');
        } else {
            // same algorithm as above (same bugs, too)
            // but using long arithmetic.
            digits = new char[ndigits = 20];
            digitno = ndigits - 1;
            c = (int)(lvalue % 10L);
            lvalue /= 10L;
            while (c == 0) {
                decExponent++;
                c = (int)(lvalue % 10L);
                lvalue /= 10L;
            }
            while (lvalue != 0L) {
                digits[digitno--] = (char)(c + '0');
                decExponent++;
                c = (int)(lvalue % 10L);
                lvalue /= 10;
            }
            digits[digitno] = (char)(c + '0');
        }
        char result [];
        ndigits -= digitno;
        if (digitno == 0) {
            result = digits;
        } else {
            result = new char[ndigits];
            System.arraycopy(digits, digitno, result, 0, ndigits);
        }
        this.digits = result;
        this.decExponent = decExponent + 1;
        this.nDigits = ndigits;
    }

    //
    // add one to the least significant digit.
    // in the unlikely event there is a carry out,
    // deal with it.
    // assert that this will only happen where there
    // is only one digit, e.g. (float)1e-44 seems to do it.
    //
    private void
    roundup() {
        int i;
        int q = digits[i = (nDigits - 1)];
        if (q == '9') {
            while (q == '9' && i > 0) {
                digits[i] = '0';
                q = digits[--i];
            }
            if (q == '9') {
                // carryout! High-order 1, rest 0s, larger exp.
                decExponent += 1;
                digits[0] = '1';
                return;
            }
            // else fall through.
        }
        digits[i] = (char)(q + 1);
    }

    /**
     * FIRST IMPORTANT CONSTRUCTOR: DOUBLE
     */
    public FloatingDecimal(double d)
    {
        long    dBits = Double.doubleToLongBits(d);
        long    fractBits;
        int     binExp;
        int     nSignificantBits;

        // discover and delete sign
        if ((dBits & signMask) != 0) {
            isNegative = true;
            dBits ^= signMask;
        } else {
            isNegative = false;
        }
        // Begin to unpack
        // Discover obvious special cases of NaN and Infinity.
        binExp = (int)((dBits & expMask) >> expShift);
        fractBits = dBits & fractMask;
        if (binExp == (int)(expMask >> expShift)) {
            isExceptional = true;
            if (fractBits == 0L) {
                digits =  infinity;
            } else {
                digits = notANumber;
                isNegative = false; // NaN has no sign!
            }
            nDigits = digits.length;
            return;
        }
        isExceptional = false;
        // Finish unpacking
        // Normalize denormalized numbers.
        // Insert assumed high-order bit for normalized numbers.
        // Subtract exponent bias.
        if (binExp == 0) {
            if (fractBits == 0L) {
                // not a denorm, just a 0!
                decExponent = 0;
                digits = zero;
                nDigits = 1;
                return;
            }
            while ((fractBits & fractHOB) == 0L) {
                fractBits <<= 1;
                binExp -= 1;
            }
            // recall binExp is  - shift count.
            nSignificantBits = expShift + binExp;
            binExp += 1;
        } else {
            fractBits |= fractHOB;
            nSignificantBits = expShift + 1;
        }
        binExp -= expBias;
        // call the routine that actually does all the hard work.
        dtoa(binExp, fractBits, nSignificantBits);
    }

    /**
     * SECOND IMPORTANT CONSTRUCTOR: SINGLE
     */
    public FloatingDecimal(float f)
    {
        int     fBits = Float.floatToIntBits(f);
        int     fractBits;
        int     binExp;
        int     nSignificantBits;

        // discover and delete sign
        if ((fBits & singleSignMask) != 0) {
            isNegative = true;
            fBits ^= singleSignMask;
        } else {
            isNegative = false;
        }
        // Begin to unpack
        // Discover obvious special cases of NaN and Infinity.
        binExp = ((fBits & singleExpMask) >> singleExpShift);
        fractBits = fBits & singleFractMask;
        if (binExp == (singleExpMask >> singleExpShift)) {
            isExceptional = true;
            if (fractBits == 0L) {
                digits =  infinity;
            } else {
                digits = notANumber;
                isNegative = false; // NaN has no sign!
            }
            nDigits = digits.length;
            return;
        }
        isExceptional = false;
        // Finish unpacking
        // Normalize denormalized numbers.
        // Insert assumed high-order bit for normalized numbers.
        // Subtract exponent bias.
        if (binExp == 0) {
            if (fractBits == 0) {
                // not a denorm, just a 0!
                decExponent = 0;
                digits = zero;
                nDigits = 1;
                return;
            }
            while ((fractBits & singleFractHOB) == 0) {
                fractBits <<= 1;
                binExp -= 1;
            }
            // recall binExp is  - shift count.
            nSignificantBits = singleExpShift + binExp;
            binExp += 1;
        } else {
            fractBits |= singleFractHOB;
            nSignificantBits = singleExpShift + 1;
        }
        binExp -= singleExpBias;
        // call the routine that actually does all the hard work.
        dtoa(
            binExp,
            ((long)fractBits) << (expShift - singleExpShift),
            nSignificantBits);
    }

    private void
    dtoa(int binExp, long fractBits, int nSignificantBits)
    {
        int     nFractBits; // number of significant bits of fractBits;
        int     nTinyBits;  // number of these to the right of the point.
        int     decExp;

        // Examine number. Determine if it is an easy case,
        // which we can do pretty trivially using float/long conversion,
        // or whether we must do real work.
        nFractBits = countBits(fractBits);
        nTinyBits = Math.max(0, nFractBits - binExp - 1);
        if (binExp <= maxSmallBinExp && binExp >= minSmallBinExp) {
            // Look more closely at the number to decide if,
            // with scaling by 10^nTinyBits, the result will fit in
            // a long.
            if ((nTinyBits < long5pow.length)
                && ((nFractBits + n5bits[nTinyBits]) < 64))
            {
                /*
                 * We can do this:
                 * take the fraction bits, which are normalized.
                 * (a) nTinyBits == 0: Shift left or right appropriately
                 *     to align the binary point at the extreme right, i.e.
                 *     where a long int point is expected to be. The integer
                 *     result is easily converted to a string.
                 * (b) nTinyBits > 0: Shift right by expShift-nFractBits,
                 *     which effectively converts to long and scales by
                 *     2^nTinyBits. Then multiply by 5^nTinyBits to
                 *     complete the scaling. We know this won't overflow
                 *     because we just counted the number of bits necessary
                 *     in the result. The integer you get from this can
                 *     then be converted to a string pretty easily.
                 */
                long halfULP;
                if (nTinyBits == 0) {
                    if (binExp > nSignificantBits) {
                        halfULP = 1L << (binExp - nSignificantBits - 1);
                    } else {
                        halfULP = 0L;
                    }
                    if (binExp >= expShift) {
                        fractBits <<= (binExp - expShift);
                    } else {
                        fractBits >>>= (expShift - binExp);
                    }
                    developLongDigits(0, fractBits, halfULP);
                    return;
                }
                /*
                 * The following causes excess digits to be printed
                 * out in the single-float case. Our manipulation of
                 * halfULP here is apparently not correct. If we
                 * better understand how this works, perhaps we can
                 * use this special case again. But for the time being,
                 * we do not.
                 * else {
                 *     fractBits >>>= expShift + 1-nFractBits;
                 *     fractBits *= long5pow[ nTinyBits ];
                 *     halfULP = long5pow[ nTinyBits ]
                 *         >> (1 + nSignificantBits - nFractBits);
                 *     developLongDigits(-nTinyBits, fractBits, halfULP);
                 *     return;
                 * }
                 */
            }
        }
        /*
         * This is the hard case. We are going to compute large positive
         * integers B and S and integer decExp, s.t.
         *      d = (B / S) * 10^decExp
         *      1 <= B / S < 10
         * Obvious choices are:
         *      decExp = floor(log10(d))
         *      B      = d * 2^nTinyBits * 10^max(0, -decExp)
         *      S      = 10^max(0, decExp) * 2^nTinyBits
         * (noting that nTinyBits has already been forced to non-negative)
         * I am also going to compute a large positive integer
         *      M      = (1/2^nSignificantBits)
         *               * 2^nTinyBits
         *               * 10^max(0, -decExp)
         * i.e. M is (1/2) of the ULP of d, scaled like B.
         * When we iterate through dividing B/S and picking off the
         * quotient bits, we will know when to stop when the remainder
         * is <= M.
         *
         * We keep track of powers of 2 and powers of 5.
         */

        /*
         * Estimate decimal exponent. (If it is small-ish,
         * we could double-check.)
         *
         * First, scale the mantissa bits such that 1 <= d2 < 2.
         * We are then going to estimate
         *          log10(d2) ~=~  (d2-1.5)/1.5 + log(1.5)
         * and so we can estimate
         *      log10(d) ~=~ log10(d2) + binExp * log10(2)
         * take the floor and call it decExp.
         * FIXME -- use more precise constants here. It costs no more.
         */
        double d2 = Double.longBitsToDouble(
            expOne | (fractBits &~ fractHOB));
        decExp = (int)Math.floor(
            (d2 - 1.5D) * 0.289529654D
            + 0.176091259
            + (double)binExp * 0.301029995663981);
        int B2, B5; // powers of 2 and powers of 5, respectively, in B
        int S2, S5; // powers of 2 and powers of 5, respectively, in S
        int M2, M5; // powers of 2 and powers of 5, respectively, in M
        int Bbits; // binary digits needed to represent B, approx.
        int tenSbits; // binary digits needed to represent 10*S, approx.
        FDBigInt Sval, Bval, Mval;

        B5 = Math.max(0, -decExp);
        B2 = B5 + nTinyBits + binExp;

        S5 = Math.max(0, decExp);
        S2 = S5 + nTinyBits;

        M5 = B5;
        M2 = B2 - nSignificantBits;

        /*
         * the long integer fractBits contains the (nFractBits) interesting
         * bits from the mantissa of d (hidden 1 added if necessary) followed
         * by (expShift + 1-nFractBits) zeros. In the interest of compactness,
         * I will shift out those zeros before turning fractBits into a
         * FDBigInt. The resulting whole number will be
         *      d * 2^(nFractBits-1-binExp).
         */
        fractBits >>>= (expShift + 1 - nFractBits);
        B2 -= nFractBits - 1;
        int common2factor = Math.min(B2, S2);
        B2 -= common2factor;
        S2 -= common2factor;
        M2 -= common2factor;

        /*
         * HACK!! For exact powers of two, the next smallest number
         * is only half as far away as we think (because the meaning of
         * ULP changes at power-of-two bounds) for this reason, we
         * hack M2. Hope this works.
         */
        if (nFractBits == 1) {
            M2 -= 1;
        }
        if (M2 < 0) {
            // oops.
            // since we cannot scale M down far enough,
            // we must scale the other values up.
            B2 -= M2;
            S2 -= M2;
            M2 =  0;
        }
        /*
         * Construct, Scale, iterate.
         * Some day, we'll write a stopping test that takes
         * account of the assymetry of the spacing of floating-point
         * numbers below perfect powers of 2
         * 26 Sept 96 is not that day.
         * So we use a symmetric test.
         */
        char digits[] = this.digits = new char[18];
        int  ndigit = 0;
        boolean low, high;
        long lowDigitDifference;
        int  q;

        /*
         * Detect the special cases where all the numbers we are about
         * to compute will fit in int or long integers.
         * In these cases, we will avoid doing FDBigInt arithmetic.
         * We use the same algorithms, except that we "normalize"
         * our FDBigInts before iterating. This is to make division easier,
         * as it makes our fist guess (quotient of high-order words)
         * more accurate!
         *
         * Some day, we'll write a stopping test that takes
         * account of the assymetry of the spacing of floating-point
         * numbers below perfect powers of 2
         * 26 Sept 96 is not that day.
         * So we use a symmetric test.
         */
        Bbits =
            nFractBits
            + B2
            + ((B5 < n5bits.length) ? n5bits[B5] : (B5 * 3));
        tenSbits =
            S2
            + 1
            + (((S5 + 1) < n5bits.length) ? n5bits[(S5 + 1)] : ((S5 + 1) * 3));
        if (Bbits < 64 && tenSbits < 64) {
            if (Bbits < 32 && tenSbits < 32) {
                // wa-hoo! They're all ints!
                int b = ((int)fractBits * small5pow[B5]) << B2;
                int s = small5pow[S5] << S2;
                int m = small5pow[M5] << M2;
                int tens = s * 10;
                /*
                 * Unroll the first iteration. If our decExp estimate
                 * was too high, our first quotient will be zero. In this
                 * case, we discard it and decrement decExp.
                 */
                ndigit = 0;
                q = (b / s);
                b = 10 * (b % s);
                m *= 10;
                low  = (b <  m);
                high = (b + m > tens);
                if (q >= 10) {
                    // bummer, dude
                    throw new RuntimeException(
                        "Assertion botch: excessivly large digit " + q);
                } else if ((q == 0) && ! high) {
                    // oops. Usually ignore leading zero.
                    decExp--;
                } else {
                    digits[ndigit++] = (char)('0' + q);
                }
                /*
                 * HACK! Java spec sez that we always have at least
                 * one digit after the . in either F- or E-form output.
                 * Thus we will need more than one digit if we're using
                 * E-form
                 */
                if (decExp <= -3 || decExp >= 8) {
                    high = low = false;
                }
                while (! low && ! high) {
                    q = (b / s);
                    b = 10 * (b % s);
                    m *= 10;
                    if (q >= 10) {
                        // bummer, dude
                        throw new RuntimeException(
                            "Assertion botch: excessivly large digit " + q);
                    }
                    if (m > 0L) {
                        low  = (b <  m);
                        high = (b + m > tens);
                    } else {
                        // hack -- m might overflow!
                        // in this case, it is certainly > b,
                        // which won't
                        // and b+m > tens, too, since that has overflowed
                        // either!
                        low = true;
                        high = true;
                    }
                    digits[ndigit++] = (char)('0' + q);
                }
                lowDigitDifference = (b << 1) - tens;
            } else {
                // still good! they're all longs!
                long b = (fractBits * long5pow[B5]) << B2;
                long s = long5pow[S5] << S2;
                long m = long5pow[M5] << M2;
                long tens = s * 10L;
                /*
                 * Unroll the first iteration. If our decExp estimate
                 * was too high, our first quotient will be zero. In this
                 * case, we discard it and decrement decExp.
                 */
                ndigit = 0;
                q = (int) (b / s);
                b = 10L * (b % s);
                m *= 10L;
                low  = (b <  m);
                high = (b + m > tens);
                if (q >= 10) {
                    // bummer, dude
                    throw new RuntimeException(
                        "Assertion botch: excessivly large digit " + q);
                } else if ((q == 0) && ! high) {
                    // oops. Usually ignore leading zero.
                    decExp--;
                } else {
                    digits[ndigit++] = (char)('0' + q);
                }
                /*
                 * HACK! Java spec sez that we always have at least
                 * one digit after the . in either F- or E-form output.
                 * Thus we will need more than one digit if we're using
                 * E-form
                 */
                if (decExp <= -3 || decExp >= 8) {
                    high = low = false;
                }
                while (! low && ! high) {
                    q = (int) (b / s);
                    b = 10 * (b % s);
                    m *= 10;
                    if (q >= 10) {
                        // bummer, dude
                        throw new RuntimeException(
                            "Assertion botch: excessivly large digit " + q);
                    }
                    if (m > 0L) {
                        low  = (b <  m);
                        high = (b + m > tens);
                    } else {
                        // hack -- m might overflow!
                        // in this case, it is certainly > b,
                        // which won't
                        // and b+m > tens, too, since that has overflowed
                        // either!
                        low = true;
                        high = true;
                    }
                    digits[ndigit++] = (char)('0' + q);
                }
                lowDigitDifference = (b << 1) - tens;
            }
        } else {
            FDBigInt tenSval;
            int  shiftBias;

            /*
             * We really must do FDBigInt arithmetic.
             * Fist, construct our FDBigInt initial values.
             */
            Bval = new FDBigInt(fractBits);
            if (B5 != 0) {
                if (B5 < small5pow.length) {
                    Bval = Bval.mult(small5pow[B5]);
                } else {
                    Bval = Bval.mult(big5pow(B5));
                }
            }
            if (B2 != 0) {
                Bval.lshiftMe(B2);
            }
            Sval = new FDBigInt(big5pow(S5));
            if (S2 != 0) {
                Sval.lshiftMe(S2);
            }
            Mval = new FDBigInt(big5pow(M5));
            if (M2 != 0) {
                Mval.lshiftMe(M2);
            }


            // normalize so that division works better
            Bval.lshiftMe(shiftBias = Sval.normalizeMe());
            Mval.lshiftMe(shiftBias);
            tenSval = Sval.mult(10);
            /*
             * Unroll the first iteration. If our decExp estimate
             * was too high, our first quotient will be zero. In this
             * case, we discard it and decrement decExp.
             */
            ndigit = 0;
            q = Bval.quoRemIteration(Sval);
            Mval = Mval.mult(10);
            low  = (Bval.cmp(Mval) < 0);
            high = (Bval.add(Mval).cmp(tenSval) > 0);
            if (q >= 10) {
                // bummer, dude
                throw new RuntimeException(
                    "Assertion botch: excessivly large digit " + q);
            } else if ((q == 0) && ! high) {
                // oops. Usually ignore leading zero.
                decExp--;
            } else {
                digits[ndigit++] = (char)('0' + q);
            }
            /*
             * HACK! Java spec sez that we always have at least
             * one digit after the . in either F- or E-form output.
             * Thus we will need more than one digit if we're using
             * E-form
             */
            if (decExp <= -3 || decExp >= 8) {
                high = low = false;
            }
            while (! low && ! high) {
                q = Bval.quoRemIteration(Sval);
                Mval = Mval.mult(10);
                if (q >= 10) {
                    // bummer, dude
                    throw new RuntimeException(
                        "Assertion botch: excessivly large digit " + q);
                }
                low  = (Bval.cmp(Mval) < 0);
                high = (Bval.add(Mval).cmp(tenSval) > 0);
                digits[ndigit++] = (char)('0' + q);
            }
            if (high && low) {
                Bval.lshiftMe(1);
                lowDigitDifference = Bval.cmp(tenSval);
            } else {
                lowDigitDifference = 0L; // this here only for flow analysis!
            }
        }
        this.decExponent = decExp + 1;
        this.digits = digits;
        this.nDigits = ndigit;
        /*
         * Last digit gets rounded based on stopping condition.
         */
        if (high) {
            if (low) {
                if (lowDigitDifference == 0L) {
                    // it's a tie!
                    // choose based on which digits we like.
                    if ((digits[nDigits - 1] & 1) != 0) {
                        roundup();
                    }
                } else if (lowDigitDifference > 0) {
                    roundup();
                }
            } else {
                roundup();
            }
        }
    }

    public String
    toString() {
        // most brain-dead version
        StringBuilder result = new StringBuilder(nDigits + 8);
        if (isNegative) {
            result.append('-');
        }
        if (isExceptional) {
            result.append(digits, 0, nDigits);
        } else {
            result.append("0.");
            result.append(digits, 0, nDigits);
            result.append('e');
            result.append(decExponent);
        }
        return new String(result);
    }

    public String
    toJavaFormatString() {
        char result[] = new char[nDigits + 10];
        int  i = 0;
        if (isNegative) {
            result[0] = '-';
            i = 1;
        }
        if (isExceptional) {
            System.arraycopy(digits, 0, result, i, nDigits);
            i += nDigits;
        } else {
            if (decExponent > 0 && decExponent < 8) {
                // print digits.digits.
                int charLength = Math.min(nDigits, decExponent);
                System.arraycopy(digits, 0, result, i, charLength);
                i += charLength;
                if (charLength < decExponent) {
                    charLength = decExponent - charLength;
                    System.arraycopy(zero, 0, result, i, charLength);
                    i += charLength;
                    result[i++] = '.';
                    result[i++] = '0';
                } else {
                    result[i++] = '.';
                    if (charLength < nDigits) {
                        int t = nDigits - charLength;
                        System.arraycopy(digits, charLength, result, i, t);
                        i += t;
                    } else {
                        result[i++] = '0';
                    }
                }
            } else if (decExponent <=0 && decExponent > -3) {
                result[i++] = '0';
                result[i++] = '.';
                if (decExponent != 0) {
                    System.arraycopy(zero, 0, result, i, -decExponent);
                    i -= decExponent;
                }
                System.arraycopy(digits, 0, result, i, nDigits);
                i += nDigits;
            } else {
                result[i++] = digits[0];
                result[i++] = '.';
                if (nDigits > 1) {
                    System.arraycopy(digits, 1, result, i, nDigits - 1);
                    i += nDigits - 1;
                } else {
                    result[i++] = '0';
                }
                result[i++] = 'E';
                int e;
                if (decExponent <= 0) {
                    result[i++] = '-';
                    e = -decExponent + 1;
                } else {
                    e = decExponent - 1;
                }
                // decExponent has 1, 2, or 3, digits
                if (e <= 9) {
                    result[i++] = (char)(e + '0');
                } else if (e <= 99) {
                    result[i++] = (char)(e / 10 + '0');
                    result[i++] = (char)(e % 10 + '0');
                } else {
                    result[i++] = (char)(e / 100 + '0');
                    e %= 100;
                    result[i++] = (char)(e / 10 + '0');
                    result[i++] = (char)(e % 10 + '0');
                }
            }
        }
        return new String(result, 0, i);
    }

    // jhyde added
    public FloatingDecimal(long n)
    {
        isExceptional = false; // I don't think longs can be exceptional
        if (n < 0) {
            isNegative = true;
            n = -n; // if n == MIN_LONG, oops!
        } else {
            isNegative = false;
        }
        if (n == 0) {
            nDigits = 1;
            digits = new char[] {'0', '0', '0', '0', '0', '0', '0', '0'};
            decExponent = 0;
        } else {
            nDigits = 0;
            for (long m = n; m != 0; m = m / 10) {
                nDigits++;
            }
            decExponent = nDigits;
            digits = new char[nDigits];
            int i = nDigits - 1;
            for (long m = n; m != 0; m = m / 10) {
                digits[i--] = (char) ('0' + (m % 10));
            }
        }
    }

    // jhyde added
    public void shift(int i)
    {
        if (isExceptional
            || nDigits == 1 && digits[0] == '0')
        {
            ; // don't multiply zero
        } else {
            decExponent += i;
        }
    }

    // jhyde added
    public String toJavaFormatString(
        int minDigitsLeftOfDecimal,
        char decimalChar, // '.' or ','
        int minDigitsRightOfDecimal,
        int maxDigitsRightOfDecimal, // todo: use
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
        int resultLen =
            10 + Math.abs(decExponent) * 4 / 3 + maxDigitsRightOfDecimal;
        char result[] = new char[resultLen];
        int i = toJavaFormatString(
            result, 0, minDigitsLeftOfDecimal, decimalChar,
            minDigitsRightOfDecimal, maxDigitsRightOfDecimal, expChar, expSign,
            minExpDigits, thousandChar, useDecimal, thousandSeparatorPositions);
        return new String(result, 0, i);
    }

    // jhyde added
    private synchronized int toJavaFormatString(
        char result[],
        int i,
        int minDigitsLeftOfDecimal,
        char decimalChar, // '.' or ','
        int minDigitsRightOfDecimal,
        int maxDigitsRightOfDecimal, // todo: use
        char expChar, // 'E' or 'e'
        boolean expSign, // whether to print '+' if exp is positive
        int minExpDigits, // minimum digits in exponent
        char thousandChar, // ',' or '.' or 0
        boolean useDecimal,
        ArrayStack<Integer> thousandSeparatorPositions)
    {
        if (isNegative) {
            result[i++] = '-';
        }
        if (isExceptional) {
            System.arraycopy(digits, 0, result, i, nDigits);
            i += nDigits;
        } else if (expChar == 0) {
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
            int wholeDigits = Math.max(decExponent, minDigitsLeftOfDecimal),
                fractionDigits = Math.max(
                    nDigits - decExponent, minDigitsRightOfDecimal),
                totalDigits = wholeDigits + fractionDigits;
            char[] digits2 = new char[totalDigits];
            for (int j = 0; j < totalDigits; j++) {
                digits2[j] = '0';
            }
            for (int j = 0; j < nDigits; j++) {
                digits2[wholeDigits - decExponent + j] = digits[j];
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
                        if (d < '5') {
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
                // store it temporarely and then invert.
                ArrayStack<Character> formatedWholeDigits =
                    new ArrayStack<Character>();
                // We need to keep track of how many digits we printed in the
                // current token.
                int nbInserted = 0;
                for (int j = wholeDigits - 1; j >= firstDigitToPrint; j--) {
                    // Check if we need to insert another thousand separator
                    if (nbInserted % thousandSeparatorPositions.peek() == 0
                        && nbInserted > 0)
                    {
                        formatedWholeDigits.push(thousandChar);
                        nbInserted = 0;
                        // The last format token is kept because we re-apply it
                        // until the end of the digits.
                        if (thousandSeparatorPositions.size() > 1) {
                            thousandSeparatorPositions.pop();
                        }
                    }
                    // Insert the next digit.
                    formatedWholeDigits.push(digits2[j]);
                    nbInserted++;
                }
                // We're done. Invert the print out and add it to
                // the result array.
                while (formatedWholeDigits.size() > 0) {
                    result[i++] = formatedWholeDigits.pop();
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
        } else {
            // Make a recursive call to print the digits left of the 'E'.
            int oldExp = decExponent;
            decExponent = Math.min(minDigitsLeftOfDecimal, nDigits);
            boolean oldIsNegative = isNegative;
            isNegative = false;
            i = toJavaFormatString(
                result, i, minDigitsLeftOfDecimal, decimalChar,
                minDigitsRightOfDecimal, maxDigitsRightOfDecimal, (char) 0,
                false, minExpDigits, '\0', useDecimal,
                thousandSeparatorPositions);
            decExponent = oldExp;
            isNegative = oldIsNegative;

            result[i++] = expChar;
            int de = decExponent;
            if (nDigits == 1 && digits[0] == '0') {
                de = 1; // 0's exponent is 0, but that's not convenient here
            }
            int e;
            if (de <= 0) {
                result[i++] = '-';
                e = -de + 1;
            } else {
                if (expSign) {
                    result[i++] = '+';
                }
                e = de - 1;
            }
            // decExponent has 1, 2, or 3, digits
            int nExpDigits = e <= 9 ? 1 : e <= 99 ? 2 : 3;
            for (int j = nExpDigits; j < minExpDigits; j++) {
                result[i++] = '0';
            }
            if (e <= 9) {
                result[i++] = (char)(e + '0');
            } else if (e <= 99) {
                result[i++] = (char)(e / 10 + '0');
                result[i++] = (char)(e % 10 + '0');
            } else {
                result[i++] = (char)(e / 100 + '0');
                e %= 100;
                result[i++] = (char)(e / 10 + '0');
                result[i++] = (char)(e % 10 + '0');
            }
        }
        return i;
    }

    private static final int small5pow[] = {
        1,
        5,
        5 * 5,
        5 * 5 * 5,
        5 * 5 * 5 * 5,
        5 * 5 * 5 * 5 * 5,
        5 * 5 * 5 * 5 * 5 * 5,
        5 * 5 * 5 * 5 * 5 * 5 * 5,
        5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
        5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
        5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
        5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
        5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
        5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5
    };

    private static final long long5pow[] = {
        1L,
        5L,
        5L * 5,
        5L * 5 * 5,
        5L * 5 * 5 * 5,
        5L * 5 * 5 * 5 * 5,
        5L * 5 * 5 * 5 * 5 * 5,
        5L * 5 * 5 * 5 * 5 * 5 * 5,
        5L * 5 * 5 * 5 * 5 * 5 * 5 * 5,
        5L * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
        5L * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
        5L * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
        5L * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
        5L * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
        5L * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
        5L * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
        5L * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
        5L * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
        5L * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
        5L * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5
        * 5,
        5L * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5
        * 5 * 5,
        5L * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5
        * 5 * 5 * 5,
        5L * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5
        * 5 * 5 * 5 * 5,
        5L * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5
        * 5 * 5 * 5 * 5 * 5,
        5L * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5
        * 5 * 5 * 5 * 5 * 5 * 5,
        5L * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5
        * 5 * 5 * 5 * 5 * 5 * 5 * 5,
        5L * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5
        * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
    };

    // approximately ceil(log2(long5pow[i]))
    private static final int n5bits[] = {
        0,
        3,
        5,
        7,
        10,
        12,
        14,
        17,
        19,
        21,
        24,
        26,
        28,
        31,
        33,
        35,
        38,
        40,
        42,
        45,
        47,
        49,
        52,
        54,
        56,
        59,
        61,
    };

    private static final char infinity[] = {
        'I', 'n', 'f', 'i', 'n', 'i', 't', 'y'
    };
    private static final char notANumber[] = { 'N', 'a', 'N' };
    private static final char zero[] = {
        '0', '0', '0', '0', '0', '0', '0', '0'
    };
}

/**
 * A really, really simple bigint package
 * tailored to the needs of floating base conversion.
 */
static class FDBigInt {
    int nWords; // number of words used
    int data[]; // value: data[0] is least significant

    private static boolean debugging = false;

    public static void setDebugging(boolean d) {
        debugging = d;
    }

    public FDBigInt(int v) {
        nWords = 1;
        data = new int[1];
        data[0] = v;
    }

    public FDBigInt(long v) {
        data = new int[2];
        data[0] = (int)v;
        data[1] = (int)(v >>> 32);
        nWords = (data[1] == 0) ? 1 : 2;
    }

    public FDBigInt(FDBigInt other) {
        data = new int[nWords = other.nWords];
        System.arraycopy(other.data, 0, data, 0, nWords);
    }

    private FDBigInt(int [] d, int n) {
        data = d;
        nWords = n;
    }

    /*
     * Left shift by c bits.
     * Shifts this in place.
     */
    public void
    lshiftMe(int c)throws IllegalArgumentException {
        if (c <= 0) {
            if (c == 0) {
                return; // silly.
            } else {
                throw new IllegalArgumentException("negative shift count");
            }
        }
        int wordcount = c >> 5;
        int bitcount  = c & 0x1f;
        int anticount = 32 - bitcount;
        int t[] = data;
        int s[] = data;
        if (nWords + wordcount + 1 > t.length) {
            // reallocate.
            t = new int[nWords + wordcount + 1];
        }
        int target = nWords + wordcount;
        int src    = nWords - 1;
        if (bitcount == 0) {
            // special hack, since an anticount of 32 won't go!
            System.arraycopy(s, 0, t, wordcount, nWords);
            target = wordcount - 1;
        } else {
            t[target--] = s[src] >>> anticount;
            while (src >= 1) {
                t[target--] = (s[src] << bitcount) | (s[--src] >>> anticount);
            }
            t[target--] = s[src] << bitcount;
        }
        while (target >= 0) {
            t[target--] = 0;
        }
        data = t;
        nWords += wordcount + 1;
        // may have constructed high-order word of 0.
        // if so, trim it
        while (nWords > 1 && data[nWords - 1] == 0) {
            nWords--;
        }
    }

    /**
     * normalize this number by shifting until
     * the MSB of the number is at 0x08000000.
     * This is in preparation for quoRemIteration, below.
     * The idea is that, to make division easier, we want the
     * divisor to be "normalized" -- usually this means shifting
     * the MSB into the high words sign bit. But because we know that
     * the quotient will be 0 < q < 10, we would like to arrange that
     * the dividend not span up into another word of precision.
     * (This needs to be explained more clearly!)
     */
    public int
    normalizeMe() throws IllegalArgumentException {
        int src;
        int wordcount = 0;
        int bitcount  = 0;
        int v = 0;
        for (src = nWords - 1 ; src >= 0 && (v = data[src]) == 0; src--) {
            wordcount += 1;
        }
        if (src < 0) {
            // oops. Value is zero. Cannot normalize it!
            throw new IllegalArgumentException("zero value");
        }
        /*
         * In most cases, we assume that wordcount is zero. This only
         * makes sense, as we try not to maintain any high-order
         * words full of zeros. In fact, if there are zeros, we will
         * simply SHORTEN our number at this point. Watch closely...
         */
        nWords -= wordcount;
        /*
         * Compute how far left we have to shift v s.t. its highest-
         * order bit is in the right place. Then call lshiftMe to
         * do the work.
         */
        if ((v & 0xf0000000) != 0) {
            // will have to shift up into the next word.
            // too bad.
            for (bitcount = 32 ; (v & 0xf0000000) != 0 ; bitcount--) {
                v >>>= 1;
            }
        } else {
            while (v <= 0x000fffff) {
                // hack: byte-at-a-time shifting
                v <<= 8;
                bitcount += 8;
            }
            while (v <= 0x07ffffff) {
                v <<= 1;
                bitcount += 1;
            }
        }
        if (bitcount != 0) {
            lshiftMe(bitcount);
        }
        return bitcount;
    }

    /**
     * Multiply a FDBigInt by an int.
     * Result is a new FDBigInt.
     */
    public FDBigInt
    mult(int iv) {
        long v = iv;
        int r[];
        long p;

        // guess adequate size of r.
        r =
            new int[
                (v * ((long)data[nWords - 1] & 0xffffffffL) > 0xfffffffL)
                    ? nWords + 1
                    : nWords];
        p = 0L;
        for (int i = 0; i < nWords; i++) {
            p += v * ((long)data[i] & 0xffffffffL);
            r[i] = (int)p;
            p >>>= 32;
        }
        if (p == 0L) {
            return new FDBigInt(r, nWords);
        } else {
            r[nWords] = (int)p;
            return new FDBigInt(r, nWords + 1);
        }
    }

    /**
     * Multiply a FDBigInt by another FDBigInt.
     * Result is a new FDBigInt.
     */
    public FDBigInt
    mult(FDBigInt other) {
        // crudely guess adequate size for r
        int r[] = new int[nWords + other.nWords];
        int i;
        // I think I am promised zeros...

        for (i = 0; i < this.nWords; i++) {
            long v = (long)this.data[i] & 0xffffffffL; // UNSIGNED CONVERSION
            long p = 0L;
            int j;
            for (j = 0; j < other.nWords; j++) {
                // UNSIGNED CONVERSIONS ALL 'ROUND.
                p +=
                    ((long)r[i + j] & 0xffffffffL)
                    + v * ((long)other.data[j] & 0xffffffffL);
                r[i + j] = (int)p;
                p >>>= 32;
            }
            r[i + j] = (int)p;
        }
        // compute how much of r we actually needed for all that.
        for (i = r.length - 1; i > 0; i--) {
            if (r[i] != 0) {
                break;
            }
        }
        return new FDBigInt(r, i + 1);
    }

    /**
     * Add one FDBigInt to another. Return a FDBigInt
     */
    public FDBigInt
    add(FDBigInt other) {
        int i;
        int a[], b[];
        int n, m;
        long c = 0L;
        // arrange such that a.nWords >= b.nWords;
        // n = a.nWords, m = b.nWords
        if (this.nWords >= other.nWords) {
            a = this.data;
            n = this.nWords;
            b = other.data;
            m = other.nWords;
        } else {
            a = other.data;
            n = other.nWords;
            b = this.data;
            m = this.nWords;
        }
        int r[] = new int[n];
        for (i = 0; i < n; i++) {
            c += (long)a[i] & 0xffffffffL;
            if (i < m) {
                c += (long)b[i] & 0xffffffffL;
            }
            r[i] = (int) c;
            c >>= 32; // signed shift.
        }
        if (c != 0L) {
            // oops -- carry out -- need longer result.
            int s[] = new int[r.length + 1];
            System.arraycopy(r, 0, s, 0, r.length);
            s[i++] = (int)c;
            return new FDBigInt(s, i);
        }
        return new FDBigInt(r, i);
    }

    /**
     * Subtract one FDBigInt from another. Return a FDBigInt
     * Assert that the result is positive.
     */
    public FDBigInt
    sub(FDBigInt other) {
        int r[] = new int[this.nWords];
        int i;
        int n = this.nWords;
        int m = other.nWords;
        int nzeros = 0;
        long c = 0L;
        for (i = 0; i < n; i++) {
            c += (long)this.data[i] & 0xffffffffL;
            if (i < m) {
                c -= (long)other.data[i] & 0xffffffffL;
            }
            if ((r[i] = (int) c) == 0) {
                nzeros++;
            } else {
                nzeros = 0;
            }
            c >>= 32; // signed shift.
        }
        if (c != 0L) {
            throw new RuntimeException(
                "Assertion botch: borrow out of subtract");
        }
        while (i < m) {
            if (other.data[i++] != 0) {
                throw new RuntimeException(
                    "Assertion botch: negative result of subtract");
            }
        }
        return new FDBigInt(r, n - nzeros);
    }

    /**
     * Compare FDBigInt with another FDBigInt. Return an integer
     * >0: this > other
     *  0: this == other
     * <0: this < other
     */
    public int
    cmp(FDBigInt other) {
        int i;
        if (this.nWords > other.nWords) {
            // if any of my high-order words is non-zero,
            // then the answer is evident
            int j = other.nWords - 1;
            for (i = this.nWords - 1; i > j ; i--) {
                if (this.data[i] != 0) {
                    return 1;
                }
            }
        } else if (this.nWords < other.nWords) {
            // if any of other's high-order words is non-zero,
            // then the answer is evident
            int j = this.nWords - 1;
            for (i = other.nWords - 1; i > j ; i--) {
                if (other.data[i] != 0) {
                    return -1;
                }
            }
        } else {
            i = this.nWords - 1;
        }
        for (; i > 0 ; i--) {
            if (this.data[i] != other.data[i]) {
                break;
            }
        }
        // careful! want unsigned compare!
        // use brute force here.
        int a = this.data[i];
        int b = other.data[i];
        if (a < 0) {
            // a is really big, unsigned
            if (b < 0) {
                return a - b; // both big, negative
            } else {
                return 1; // b not big, answer is obvious;
            }
        } else {
            // a is not really big
            if (b < 0) {
                // but b is really big
                return -1;
            } else {
                return a - b;
            }
        }
    }

    /**
     * Compute
     * q = (int)(this / S)
     * this = 10 * (this mod S)
     * Return q.
     * This is the iteration step of digit development for output.
     * We assume that S has been normalized, as above, and that
     * "this" has been lshift'ed accordingly.
     * Also assume, of course, that the result, q, can be expressed
     * as an integer, 0 <= q < 10.
     */
    public int
    quoRemIteration(FDBigInt S)throws IllegalArgumentException {
        // ensure that this and S have the same number of
        // digits. If S is properly normalized and q < 10 then
        // this must be so.
        if (nWords != S.nWords) {
            throw new IllegalArgumentException("disparate values");
        }
        // estimate q the obvious way. We will usually be
        // right. If not, then we're only off by a little and
        // will re-add.
        int n = nWords - 1;
        long q = ((long)data[n] & 0xffffffffL) / (long)S.data[n];
        long diff = 0L;
        for (int i = 0; i <= n ; i++) {
            diff +=
                ((long)data[i] & 0xffffffffL)
                -  q * ((long)S.data[i] & 0xffffffffL);
            data[i] = (int)diff;
            diff >>= 32; // N.B. SIGNED shift.
        }
        if (diff != 0L) {
            // damn, damn, damn. q is too big.
            // add S back in until this turns +. This should
            // not be very many times!
            long sum = 0L;
            while (sum ==  0L) {
                sum = 0L;
                for (int i = 0; i <= n; i++) {
                    sum +=
                        ((long)data[i] & 0xffffffffL)
                        + ((long)S.data[i] & 0xffffffffL);
                    data[i] = (int) sum;
                    sum >>= 32; // Signed or unsigned, answer is 0 or 1
                }
                /*
                 * Originally the following line read
                 * "if (sum !=0 && sum != -1)"
                 * but that would be wrong, because of the
                 * treatment of the two values as entirely unsigned,
                 * it would be impossible for a carry-out to be interpreted
                 * as -1 -- it would have to be a single-bit carry-out, or
                 *  + 1.
                 */
                if (sum != 0 && sum != 1) {
                    throw new RuntimeException(
                        "Assertion botch: " + sum
                        + " carry out of division correction");
                }
                q -= 1;
            }
        }
        // finally, we can multiply this by 10.
        // it cannot overflow, right, as the high-order word has
        // at least 4 high-order zeros!
        long p = 0L;
        for (int i = 0; i <= n; i++) {
            p += 10 * ((long)data[i] & 0xffffffffL);
            data[i] = (int)p;
            p >>= 32; // SIGNED shift.
        }
        if (p != 0L) {
            throw new RuntimeException("Assertion botch: carry out of *10");
        }
        return (int)q;
    }

    public long
    longValue() {
        // if this can be represented as a long,
        // return the value
        int i;
        for (i = this.nWords - 1; i > 1 ; i--) {
            if (data[i] != 0) {
                throw new RuntimeException("Assertion botch: value too big");
            }
        }
        switch (i) {
        case 1:
            if (data[1] < 0) {
                throw new RuntimeException("Assertion botch: value too big");
            }
            return ((long)(data[1]) << 32) | ((long)data[0] & 0xffffffffL);
        case 0:
            return ((long)data[0] & 0xffffffffL);
        default:
            throw new RuntimeException("Assertion botch: longValue confused");
        }
    }

    public String
    toString() {
        StringBuilder r = new StringBuilder(30);
        r.append('[');
        int i = Math.min(nWords - 1, data.length - 1);
        if (nWords > data.length) {
            r.append("(" + data.length + "<" + nWords + "!)");
        }
        for (; i > 0 ; i--) {
            r.append(Integer.toHexString(data[i]));
            r.append(' ');
        }
        r.append(Integer.toHexString(data[0]));
        r.append(']');
        return new String(r);
    }
}
}

// End Format.java
