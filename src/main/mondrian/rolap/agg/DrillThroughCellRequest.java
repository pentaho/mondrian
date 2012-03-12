/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2012 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 21 March, 2002
*/
package mondrian.rolap.agg;

import mondrian.rolap.RolapStar;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This is a subclass of {@link CellRequest} which allows to specify
 * which columns and measures to return as part of the ResultSet
 * which we return to the client.
 *
 * @since Feb. 2010.
 * @version $Id$
 */
public class DrillThroughCellRequest extends CellRequest {

    private final List<RolapStar.Column> drillThroughColumns =
        new ArrayList<RolapStar.Column>();

    private final List<RolapStar.Measure> drillThroughMeasures =
        new ArrayList<RolapStar.Measure>();

    public DrillThroughCellRequest(
        RolapStar.Measure measure,
        boolean extendedContext)
    {
        super(measure, extendedContext, true);
    }

    public void addDrillThroughColumn(RolapStar.Column column) {
        this.drillThroughColumns.add(column);
    }

    public boolean includeInSelect(RolapStar.Column column) {
        if (drillThroughColumns.size() == 0
            && drillThroughMeasures.size() == 0)
        {
            return true;
        }
        return drillThroughColumns.contains(column);
    }

    public void addDrillThroughMeasure(RolapStar.Measure measure) {
        this.drillThroughMeasures.add(measure);
    }

    public boolean includeInSelect(RolapStar.Measure measure) {
        if (drillThroughColumns.size() == 0
            && drillThroughMeasures.size() == 0)
        {
            return true;
        }
        return drillThroughMeasures.contains(measure);
    }

    public List<RolapStar.Measure> getDrillThroughMeasures() {
        return Collections.unmodifiableList(drillThroughMeasures);
    }
}

// End DrillThroughCellRequest.java
