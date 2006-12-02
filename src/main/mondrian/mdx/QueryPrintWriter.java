/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.mdx;

import mondrian.olap.Parameter;

import java.io.PrintWriter;
import java.io.Writer;
import java.util.HashSet;
import java.util.Set;

/**
 * PrintWriter used for unparsing queries. Remembers which parameters have
 * been printed. The first time, they print themselves as "Parameter";
 * subsequent times as "ParamRef".
 */
public class QueryPrintWriter extends PrintWriter {
    final Set<Parameter> parameters = new HashSet<Parameter>();

    public QueryPrintWriter(Writer writer) {
        super(writer);
    }
}

// End QueryPrintWriter.java
