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

import java.io.PrintWriter;

/**
 * Explain statement.
 *
 * @author jhyde
 */
public class Explain extends QueryPart {
    private final QueryPart query;

    /**
     * Creates an Explain statement.
     *
     * @param query Query (SELECT or DRILLTHROUGH)
     */
    Explain(
        QueryPart query)
    {
        this.query = query;
        assert this.query != null;
        assert this.query instanceof Query
            || this.query instanceof DrillThrough;
    }

    @Override
    public void unparse(PrintWriter pw) {
        pw.print("EXPLAIN PLAN FOR ");
        query.unparse(pw);
    }

    @Override
    public Object[] getChildren() {
        return new Object[] {query};
    }

    public QueryPart getQuery() {
        return query;
    }
}

// End Explain.java
