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
