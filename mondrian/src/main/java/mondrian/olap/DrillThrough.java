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
import java.util.*;

/**
 * Drill through statement.
 *
 * @author jhyde
 */
public class DrillThrough extends QueryPart {
    private final Query query;
    private final int maxRowCount;
    private final int firstRowOrdinal;
    private final List<OlapElement> returnList;

    /**
     * Creates a DrillThrough.
     *
     * @param query Query
     * @param maxRowCount Maximum number of rows to return, or -1
     * @param firstRowOrdinal Ordinal of first row to return, or -1
     * @param returnList List of columns to return
     */
    DrillThrough(
        Query query,
        int maxRowCount,
        int firstRowOrdinal,
        List<Exp> returnList)
    {
        this.query = query;
        this.maxRowCount = maxRowCount;
        this.firstRowOrdinal = firstRowOrdinal;
        this.returnList = Collections.unmodifiableList(
            resolveReturnList(returnList));
    }



    @Override
    public void unparse(PrintWriter pw) {
        pw.print("DRILLTHROUGH");
        if (maxRowCount >= 0) {
            pw.print(" MAXROWS ");
            pw.print(maxRowCount);
        }
        if (firstRowOrdinal >= 0) {
            pw.print(" FIRSTROWSET ");
            pw.print(firstRowOrdinal);
        }
        pw.print(" ");
        query.unparse(pw);
        if (returnList != null) {
            ExpBase.unparseList(
                pw, returnList.toArray(new Exp[returnList.size()]),
                " RETURN ", ", ", "");
        }
    }

    @Override
    public Object[] getChildren() {
        return new Object[] {maxRowCount, firstRowOrdinal, query, returnList};
    }

    public Query getQuery() {
        return query;
    }

    public int getMaxRowCount() {
        return maxRowCount;
    }

    public int getFirstRowOrdinal() {
        return firstRowOrdinal;
    }

    public List<OlapElement> getReturnList() {
        return returnList;
    }


    private List<OlapElement> resolveReturnList(List<Exp> returnList) {
        if (returnList == null) {
            return Collections.emptyList();
        }
        List<OlapElement> returnClauseElements = new ArrayList<OlapElement>();
        SchemaReader reader = query.getSchemaReader(true);
        for (Exp exp : returnList) {
            final OlapElement olapElement =
                reader.lookupCompound(
                    query.getCube(),
                    Util.parseIdentifier(exp.toString()),
                    true,
                    Category.Unknown);
            if (olapElement instanceof OlapElement) {
                returnClauseElements.add(olapElement);
            }
        }
        return returnClauseElements;
    }


}

// End DrillThrough.java
