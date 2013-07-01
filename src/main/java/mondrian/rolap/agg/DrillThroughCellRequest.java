/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2012-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.rolap.agg;

import mondrian.rolap.RolapStar;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Subclass of {@link CellRequest} that allows to specify
 * which columns and measures to return as part of the ResultSet
 * which we return to the client.
 */
public class DrillThroughCellRequest extends CellRequest {

    private final Map<RolapStar.Column, String> drillThroughColumns =
        new LinkedHashMap<RolapStar.Column, String>();

    private final List<RolapStar.Measure> drillThroughMeasures =
        new ArrayList<RolapStar.Measure>();

    public DrillThroughCellRequest(
        RolapStar.Measure measure,
        boolean extendedContext)
    {
        super(measure, extendedContext, true);
    }

    public void addDrillThroughColumn(
        RolapStar.Column column,
        String alias)
    {
        if (!this.drillThroughColumns.containsKey(column)) {
            this.drillThroughColumns.put(column, alias);
        }
    }

    /**
     * Returns an array of the constrained columns for this cell
     * request.  The array starts with the ordered array of columns
     * that will be included in the Select list, with any remaining
     * constrained columns added to the end.
     */
    @Override
    public RolapStar.Column[] getConstrainedColumns() {
        List<RolapStar.Column> orderedConstrainedColumns =
            new ArrayList<RolapStar.Column>();
        orderedConstrainedColumns.addAll(drillThroughColumns.keySet());
        RolapStar.Column[] columns = super.getConstrainedColumns();
        for (RolapStar.Column col : columns) {
            if (!orderedConstrainedColumns.contains(col)) {
                orderedConstrainedColumns.add(col);
            }
        }
        return orderedConstrainedColumns.toArray(
            new RolapStar.Column[orderedConstrainedColumns.size()]);
    }

    public boolean includeInSelect(RolapStar.Column column) {
        return drillThroughColumns.containsKey(column);
    }

    public void addDrillThroughMeasure(
        RolapStar.Measure measure,
        String alias)
    {
        this.drillThroughMeasures.add(measure);
    }

    public boolean includeInSelect(RolapStar.Measure measure) {
        return drillThroughMeasures.contains(measure);
    }

    public List<RolapStar.Measure> getDrillThroughMeasures() {
        return Collections.unmodifiableList(drillThroughMeasures);
    }

    public String getColumnAlias(RolapStar.Column column) {
        return drillThroughColumns.get(column);
    }
}

// End DrillThroughCellRequest.java
