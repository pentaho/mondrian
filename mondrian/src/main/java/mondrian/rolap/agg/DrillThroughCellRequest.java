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
package mondrian.rolap.agg;

import mondrian.olap.OlapElement;
import mondrian.rolap.RolapStar;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Subclass of {@link CellRequest} that allows to specify
 * which columns and measures to return as part of the ResultSet
 * which we return to the client.
 */
public class DrillThroughCellRequest extends CellRequest {

    private final List<RolapStar.Column> drillThroughColumns =
        new ArrayList<RolapStar.Column>();

    private final List<RolapStar.Measure> drillThroughMeasures =
        new ArrayList<RolapStar.Measure>();
    private final List<OlapElement> nonApplicableMembers;

    public DrillThroughCellRequest(
        RolapStar.Measure measure,
        boolean extendedContext, List<OlapElement> nonApplicableFields)
    {
        super(measure, extendedContext, true);
        this.nonApplicableMembers = nonApplicableFields;
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

    public List<OlapElement> getNonApplicableMembers() {
        return nonApplicableMembers;
    }
}

// End DrillThroughCellRequest.java
