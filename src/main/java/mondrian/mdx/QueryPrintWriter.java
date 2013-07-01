/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2006 Pentaho
// All Rights Reserved.
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
