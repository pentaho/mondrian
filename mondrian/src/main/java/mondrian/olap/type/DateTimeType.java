/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/



package mondrian.olap.type;

/**
 * The type of an expression representing a date, time or timestamp.
 *
 * @author jhyde
 * @since Jan 2, 2008
 */
public class DateTimeType extends ScalarType {
    /**
     * Creates a DateTime type.
     */
    public DateTimeType() {
        super("DATETIME");
    }

    public boolean equals(Object obj) {
        return obj instanceof DateTimeType;
    }

    public boolean isInstance(Object value) {
        return value instanceof java.util.Date;
    }
}

// End DateTimeType.java
