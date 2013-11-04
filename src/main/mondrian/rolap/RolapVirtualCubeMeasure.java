/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.*;

/**
 * Measure which is defined in a virtual cube, and based on a stored measure
 * in one of the virtual cube's base cubes.
 *
 * <p>Almost obsolete. Used only in {@link RolapSchemaUpgrader}.</p>
 *
 * @author jhyde
 * @since Aug 18, 2006
 */
class RolapVirtualCubeMeasure
    extends RolapMemberBase
    implements RolapStoredMeasure
{
    /**
     * The measure in the underlying cube.
     */
    private final RolapStoredMeasure cubeMeasure;
    private final Larder larder;
    private final RolapMeasureGroup measureGroup;

    RolapVirtualCubeMeasure(
        RolapMeasureGroup measureGroup,
        RolapMember parentMember,
        RolapCubeLevel level,
        RolapStoredMeasure cubeMeasure,
        Larder larder)
    {
        super(
            parentMember, level, cubeMeasure.getName(), MemberType.MEASURE,
            deriveUniqueName(parentMember, level, cubeMeasure.getName(), false),
            Larders.ofName(cubeMeasure.getName()));
        this.measureGroup = measureGroup;
        Util.deprecated(
            "since all cubes are now virtual, is this class obsolete? we just need a way to clone RolapStoredMeasure in a different measure group",
            false);
        this.cubeMeasure = cubeMeasure;
        this.larder = larder;
    }

    public Object getPropertyValue(Property property) {
        // Look first in this member (against the virtual cube), then
        // fallback on the base measure.
        // This allows, for instance, a measure to be invisible in a virtual
        // cube but visible in its base cube.
        Object value = super.getPropertyValue(property);
        if (value == null) {
            value = cubeMeasure.getPropertyValue(property);
        }
        return value;
    }

    public RolapStar.Measure getStarMeasure() {
        return cubeMeasure.getStarMeasure();
    }

    public RolapMeasureGroup getMeasureGroup() {
        return measureGroup;
    }

    public RolapSchema.PhysColumn getExpr() {
        return cubeMeasure.getExpr();
    }

    public RolapAggregator getAggregator() {
        return cubeMeasure.getAggregator();
    }

    public RolapResult.ValueFormatter getFormatter() {
        return cubeMeasure.getFormatter();
    }
}

// End RolapVirtualCubeMeasure.java
