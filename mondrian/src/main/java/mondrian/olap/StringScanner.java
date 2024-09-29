/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/


package mondrian.olap;

/**
 * Lexical analyzer whose input is a string.
 *
 * @author jhyde, 20 January, 1999
 */
public class StringScanner extends Scanner {
    private final String s;
    private int i;

    public StringScanner(String s, boolean debug) {
        super(debug);
        this.s = s;
        i = 0;
    }

    // Override Scanner.getChar().
    protected int getChar() {
        return (i >= s.length())
            ? -1
            : s.charAt(i++);
    }
}

// End StringScanner.java
