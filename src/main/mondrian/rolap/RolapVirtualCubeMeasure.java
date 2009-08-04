/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2006-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.olap.MondrianDef;
import mondrian.olap.CellFormatter;

/**
 * Measure which is defined in a virtual cube, and based on a stored measure
 * in one of the virtual cube's base cubes.
 *
 * @author jhyde
 * @version $Id$
 * @since Aug 18, 2006
 */
public class RolapVirtualCubeMeasure
    extends RolapMember
    implements RolapStoredMeasure
{
    /**
     * The measure in the underlying cube.
     */
    private final RolapStoredMeasure cubeMeasure;

    public RolapVirtualCubeMeasure(
        RolapMember parentMember,
        RolapLevel level,
        RolapStoredMeasure cubeMeasure)
    {
        super(parentMember, level, cubeMeasure.getName());
        this.cubeMeasure = cubeMeasure;
    }

    public Object getPropertyValue(String propertyName, boolean matchCase) {
        // Look first in this member (against the virtual cube), then
        // fallback on the base measure.
        // This allows, for instance, a measure to be invisible in a virtual
        // cube but visible in its base cube.
        Object value = super.getPropertyValue(propertyName, matchCase);
        if (value == null) {
            value = cubeMeasure.getPropertyValue(propertyName, matchCase);
        }
        return value;
    }

    public RolapCube getCube() {
        return cubeMeasure.getCube();
    }

    public Object getStarMeasure() {
        return cubeMeasure.getStarMeasure();
    }

    public MondrianDef.Expression getMondrianDefExpression() {
        return cubeMeasure.getMondrianDefExpression();
    }

    public RolapAggregator getAggregator() {
        return cubeMeasure.getAggregator();
    }

    public CellFormatter getFormatter() {
        return cubeMeasure.getFormatter();
    }
}

// End RolapVirtualCubeMeasure.java
