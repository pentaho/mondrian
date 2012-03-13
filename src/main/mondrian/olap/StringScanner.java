/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 1998-2005 Julian Hyde
// Copyright (C) 2005-2005 Pentaho and others
// All Rights Reserved.
*/
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
