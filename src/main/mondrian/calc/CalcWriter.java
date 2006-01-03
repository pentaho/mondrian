/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2006-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.calc;

import java.io.PrintWriter;

/**
 * Visitor which serializes an expression to text.
 *
 * @author jhyde
 * @version $Id$
 * @since Dec 23, 2005
 */
public class CalcWriter {
    private final PrintWriter writer;

    public CalcWriter(PrintWriter writer) {
        this.writer = writer;
    }

    public PrintWriter getWriter() {
        return writer;
    }
}

// End CalcWriter.java
