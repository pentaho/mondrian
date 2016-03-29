/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2016 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap.format;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;

/**
 * Default formatter which can be used for different rolap member properties.
 */
public class DefaultNumberFormatter {
    private static final String NUMBER_FORMAT_PATTERN = "#.#";
    private static final char DECIMAL_SEPARATOR = '.';

    private NumberFormat numberFormat;

    /**
     * It's supposed to be a singleton, and initialized by a factory.
     */
    DefaultNumberFormatter() {
        // set number formatting locale-insensitive
        DecimalFormatSymbols formatSymbols = new DecimalFormatSymbols();
        formatSymbols.setDecimalSeparator(DECIMAL_SEPARATOR);
        numberFormat = new DecimalFormat(
            NUMBER_FORMAT_PATTERN,
            formatSymbols);
        numberFormat.setGroupingUsed(false);
        numberFormat.setMaximumFractionDigits(Integer.MAX_VALUE);
    }

    /**
     * Numbers are a special case. We don't want any
     * scientific notations, as well as inaccurate decimal values.
     * So we wrap in a BigDecimal, and format before calling toString.
     * This is cheap to perform here,
     * because this method only gets called by the GUI.
     *
     * @param value generic value to be formatted
     * @return formatted value
     */
    public String format(Object value) {
        if (value != null) {
            if (value instanceof Number) {
                return numberFormat.format(new BigDecimal(value.toString()));
            }
            return value.toString();
        }
        return null;
    }
}
// End DefaultNumberFormatter.java