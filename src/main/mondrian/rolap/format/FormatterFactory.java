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

/**
 * Singleton factory for {@link DefaultNumberFormatter}.
 * Performs lazy initialization of an object.
 */
public class FormatterFactory {

    /**
     * Get default formatter for number values.
     * Should always return an only instance of {@link DefaultNumberFormatter}.
     *
     * @return singleton default formatter
     */
    public static DefaultNumberFormatter getDefaultNumberFormatter() {
        return DefaultFormatterHolder.formatter;
    }

    // Lazy initialization static holder
    private static class DefaultFormatterHolder {
        static DefaultNumberFormatter formatter = new DefaultNumberFormatter();
    }
}
// End FormatterFactory.java