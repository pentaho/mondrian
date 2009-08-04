/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2007-2009 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.type;

/**
 * The type of an expression representing a date, time or timestamp.
 *
 * @author jhyde
 * @version $Id$
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
}

// End DateTimeType.java
