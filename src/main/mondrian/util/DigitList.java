/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2012-2012 Pentaho and others
// All Rights Reserved.
//
// -----------------------------------------------------------------------------
// Copied from the ICU project's DigitList class.
//
// Copyright (C) 1996-2011, International Business Machines Corporation and
// others. All Rights Reserved.
*/
package mondrian.util;

import java.math.BigInteger;

/**
 * <code>DigitList</code> handles the transcoding between numeric values and
 * strings of characters.  It only represents non-negative numbers.  The
 * division of labor between <code>DigitList</code> and
 * <code>DecimalFormat</code> is that <code>DigitList</code> handles the radix
 * 10 representation issues and numeric conversion, including rounding;
 * <code>DecimalFormat</code> handles the locale-specific issues such as
 * positive and negative representation, digit grouping, decimal point,
 * currency, and so on.
 *
 * <p>A <code>DigitList</code> is a representation of a finite numeric value.
 * <code>DigitList</code> objects do not represent <code>NaN</code> or infinite
 * values.  A <code>DigitList</code> value can be converted to a
 * <code>BigDecimal</code> without loss of precision.  Conversion to other
 * numeric formats may involve loss of precision, depending on the specific
 * value.
 *
 * <p>The <code>DigitList</code> representation consists of a string of
 * characters, which are the digits radix 10, from '0' to '9'.  It also has a
 * base 10 exponent associated with it.  The value represented by a
 * <code>DigitList</code> object can be computed by mulitplying the fraction
 * <em>f</em>, where 0 <= <em>f</em> < 1, derived by placing all the digits of
 * the list to the right of the decimal point, by 10^exponent.
 *
 * @see java.util.Locale
 * @see java.text.Format
 * @see java.text.ChoiceFormat
 * @see java.text.MessageFormat
 * @version      1.18 08/12/98
 * @author       Mark Davis, Alan Liu
 */
final class DigitList {
    /**
     * The maximum number of significant digits in an IEEE 754 double, that
     * is, in a Java double.  This must not be increased, or garbage digits
     * will be generated, and should not be decreased, or accuracy will be lost.
     */
    public static final int MAX_LONG_DIGITS = 19;
    static {
        assert MAX_LONG_DIGITS == Long.toString(Long.MAX_VALUE).length();
    }

    /**
     * These data members are intentionally public and can be set directly.
     *
     * <p>The value represented is given by placing the decimal point before
     * digits[decimalAt].  If decimalAt is &lt; 0, then leading zeros between
     * the decimal point and the first nonzero digit are implied.  If decimalAt
     * is &gt; count, then trailing zeros between the digits[count-1] and the
     * decimal point are implied.</p>
     *
     * <p>Equivalently, the represented value is given by f * 10^decimalAt.
     * Here f is a value 0.1 &le; f &lt; 1 arrived at by placing the digits in
     * Digits to the right of the decimal.</p>
     *
     * <p>DigitList is normalized, so if it is non-zero, figits[0] is non-zero.
     * We don't allow denormalized numbers because our exponent is effectively
     * of unlimited magnitude.  The count value contains the number of
     * significant digits present in digits[].</p>
     *
     * <p>Zero is represented by any DigitList with count == 0 or with each
     * digits[i] for all i &le; count == '0'.</p>
     */
    public int decimalAt = 0;
    public int count = 0;
    public byte[] digits = new byte[MAX_LONG_DIGITS];

    private final void ensureCapacity(int digitCapacity, int digitsToCopy) {
        if (digitCapacity > digits.length) {
            byte[] newDigits = new byte[digitCapacity * 2];
            System.arraycopy(digits, 0, newDigits, 0, digitsToCopy);
            digits = newDigits;
        }
    }

    /**
     * Appends digits to the list.
     */
    public void append(int digit) {
        ensureCapacity(count + 1, count);
        digits[count++] = (byte) digit;
    }

    /**
     * Set the digit list to a representation of the given double value.
     * This method supports both fixed-point and exponential notation.
     * @param source Value to be converted; must not be Inf, -Inf, Nan,
     * or a value &le; 0.
     * @param maximumDigits The most fractional or total digits which should
     * be converted.
     * @param fixedPoint If true, then maximumDigits is the maximum
     * fractional digits to be converted.  If false, total digits.
     */
    final void set(double source, int maximumDigits, boolean fixedPoint)
    {
        if (source == 0) {
            source = 0;
        }
        // Generate a representation of the form DDDDD, DDDDD.DDDDD, or
        // DDDDDE+/-DDDDD.
        String rep = Double.toString(source);

        set(rep, MAX_LONG_DIGITS);

        if (fixedPoint) {
            // The negative of the exponent represents the number of leading
            // zeros between the decimal and the first non-zero digit, for a
            // value < 0.1 (e.g., for 0.00123, -decimalAt == 2).  If this is
            // more than the maximum fraction digits, then we have an underflow
            // for the printed representation.
            if (-decimalAt > maximumDigits) {
                count = 0;
                return;
            } else if (-decimalAt == maximumDigits) {
                if (shouldRoundUp(0)) {
                    count = 1;
                    ++decimalAt;
                    digits[0] = (byte)'1';
                } else {
                    count = 0;
                }
                return;
            }
            // else fall through
        }

        // Eliminate trailing zeros.
        while (count > 1 && digits[count - 1] == '0') {
            --count;
        }
        // Eliminate digits beyond maximum digits to be displayed.
        // Round up if appropriate.
        round(
            fixedPoint
            ? (maximumDigits + decimalAt)
            : maximumDigits == 0
            ? -1
            : maximumDigits);
    }

    /**
     * Given a string representation of the form DDDDD, DDDDD.DDDDD,
     * or DDDDDE+/-DDDDD, set this object's value to it.  Ignore
     * any leading '-'.
     */
    private void set(String rep, int maxCount) {
        decimalAt = -1;
        count = 0;
        int exponent = 0;
        // Number of zeros between decimal point and first non-zero digit after
        // decimal point, for numbers < 1.
        int leadingZerosAfterDecimal = 0;
        boolean nonZeroDigitSeen = false;
        // Skip over leading '-'
        int i = 0;
        if (rep.charAt(i) == '-') {
            ++i;
        }
        for (; i < rep.length(); ++i) {
            char c = rep.charAt(i);
            if (c == '.') {
                decimalAt = count;
            } else if (c == 'e' || c == 'E') {
                ++i;
                // Integer.parseInt doesn't handle leading '+' signs
                if (rep.charAt(i) == '+') {
                    ++i;
                }
                exponent = Integer.valueOf(rep.substring(i)).intValue();
                break;
            } else if (count < maxCount) {
                if (!nonZeroDigitSeen) {
                    nonZeroDigitSeen = (c != '0');
                    if (!nonZeroDigitSeen && decimalAt != -1) {
                        ++leadingZerosAfterDecimal;
                    }
                }

                if (nonZeroDigitSeen) {
                    ensureCapacity(count + 1, count);
                    digits[count++] = (byte)c;
                }
            }
        }
        if (decimalAt == -1) {
            decimalAt = count;
        }
        decimalAt += exponent - leadingZerosAfterDecimal;
    }

    /**
     * Return true if truncating the representation to the given number
     * of digits will result in an increment to the last digit.  This
     * method implements half-even rounding, the default rounding mode.
     *
     * @param maximumDigits the number of digits to keep, from 0 to
     * <code>count-1</code>.  If 0, then all digits are rounded away, and
     * this method returns true if a one should be generated (e.g., formatting
     * 0.09 with "#.#").
     * @return true if digit <code>maximumDigits-1</code> should be
     * incremented
     */
    private boolean shouldRoundUp(int maximumDigits) {
        // variable not used boolean increment = false;
        // Implement IEEE half-even rounding
        if (maximumDigits < count) {
            if (digits[maximumDigits] > '5') {
                return true;
            } else if (digits[maximumDigits] == '5') {
                for (int i = maximumDigits + 1; i < count; ++i) {
                    if (digits[i] != '0') {
                        return true;
                    }
                }
                return maximumDigits > 0
                    && (digits[maximumDigits - 1] % 2 != 0);
            }
        }
        return false;
    }

    /**
     * Round the representation to the given number of digits.
     * @param maximumDigits The maximum number of digits to be shown.
     * Upon return, count will be less than or equal to maximumDigits.
     * This now performs rounding when maximumDigits is 0, formerly it did not.
     */
    public final void round(int maximumDigits) {
        // Eliminate digits beyond maximum digits to be displayed.
        // Round up if appropriate.
        // [bnf] rewritten to fix 4179818
        if (maximumDigits >= 0 && maximumDigits < count) {
            if (shouldRoundUp(maximumDigits)) {
                // Rounding up involves incrementing digits from LSD to MSD.
                // In most cases this is simple, but in a worst case situation
                // (9999..99) we have to adjust the decimalAt value.
                for (;;) {
                    --maximumDigits;
                    if (maximumDigits < 0) {
                        // We have all 9's, so we increment to a single digit
                        // of one and adjust the exponent.
                        digits[0] = (byte) '1';
                        ++decimalAt;
                        maximumDigits = 0; // Adjust the count
                        break;
                    }

                    ++digits[maximumDigits];
                    if (digits[maximumDigits] <= '9') {
                        break;
                    }
                    // Unnecessary since we'll truncate this:
                    // digits[maximumDigits] = '0';
                }
                ++maximumDigits; // Increment for use as count
            }
            count = maximumDigits;
        }
        // Bug 4217661 DecimalFormat formats 1.001 to "1.00" instead of "1"
        // Eliminate trailing zeros. [Richard/GCL]
        // [dlf] moved outside if block, see ticket #6408
        while (count > 1 && digits[count - 1] == '0') {
          --count;
        }
    }

    /**
     * Utility routine to set the value of the digit list from a long
     */
    public final void set(long source)
    {
        set(source, 0);
    }

    /**
     * Set the digit list to a representation of the given long value.
     * @param source Value to be converted; must be >= 0 or ==
     * Long.MIN_VALUE.
     * @param maximumDigits The most digits which should be converted.
     * If maximumDigits is lower than the number of significant digits
     * in source, the representation will be rounded.  Ignored if <= 0.
     */
    public final void set(long source, int maximumDigits)
    {
        // This method does not expect a negative number. However,
        // "source" can be a Long.MIN_VALUE (-9223372036854775808),
        // if the number being formatted is a Long.MIN_VALUE.  In that
        // case, it will be formatted as -Long.MIN_VALUE, a number
        // which is outside the legal range of a long, but which can
        // be represented by DigitList.
        // [NEW] Faster implementation
        if (source <= 0) {
            if (source == Long.MIN_VALUE) {
                decimalAt = count = MAX_LONG_DIGITS;
                System.arraycopy(LONG_MIN_REP, 0, digits, 0, count);
            } else {
                count = 0;
                decimalAt = 0;
            }
        } else {
            int left = MAX_LONG_DIGITS;
            int right;
            while (source > 0) {
                digits[--left] = (byte) (((long) '0') + (source % 10));
                source /= 10;
            }
            decimalAt = MAX_LONG_DIGITS - left;
            // Don't copy trailing zeros
            // we are guaranteed that there is at least one non-zero digit,
            // so we don't have to check lower bounds
            for (right = MAX_LONG_DIGITS - 1;
                 digits[right] == (byte) '0';
                 --right)
            {
            }
            count = right - left + 1;
            System.arraycopy(digits, left, digits, 0, count);
        }
        if (maximumDigits > 0) {
            round(maximumDigits);
        }
    }

    /**
     * Set the digit list to a representation of the given BigInteger value.
     *
     * @param source Value to be converted
     * @param maximumDigits The most digits which should be converted.
     * If maximumDigits is lower than the number of significant digits
     * in source, the representation will be rounded.  Ignored if <= 0.
     */
    public final void set(BigInteger source, int maximumDigits) {
        String stringDigits = source.toString();

        count = decimalAt = stringDigits.length();

        // Don't copy trailing zeros
        while (count > 1 && stringDigits.charAt(count - 1) == '0') {
            --count;
        }
        int offset = 0;
        if (stringDigits.charAt(0) == '-') {
            ++offset;
            --count;
            --decimalAt;
        }

        ensureCapacity(count, 0);
        for (int i = 0; i < count; ++i) {
            digits[i] = (byte) stringDigits.charAt(i + offset);
        }

        if (maximumDigits > 0) {
            round(maximumDigits);
        }
    }

    private static byte[] LONG_MIN_REP;

    static
    {
        // Store the representation of LONG_MIN without the leading '-'
        String s = Long.toString(Long.MIN_VALUE);
        LONG_MIN_REP = new byte[MAX_LONG_DIGITS];
        for (int i = 0; i < MAX_LONG_DIGITS; ++i) {
            LONG_MIN_REP[i] = (byte)s.charAt(i + 1);
        }
    }
}

// End DigitList.java
