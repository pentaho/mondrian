/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2007 Pentaho and others
// All Rights Reserved.
//
// jhyde, 10 August, 2001
*/
package mondrian.rolap;

import mondrian.olap.MondrianDef;

/**
 * A measure which is implemented by a SQL column or SQL expression (as opposed
 * to a {@link RolapCalculatedMember}.
 *
 * <p>Implemented by {@link RolapBaseCubeMeasure} and
 * {@link RolapVirtualCubeMeasure}.
 *
 * @author jhyde
 * @since 10 August, 2001
 */
public interface RolapStoredMeasure extends RolapMeasure {
    /**
     * Returns the cube this measure belongs to.
     */
    RolapCube getCube();

    /**
     * Returns the column which holds the value of the measure.
     */
    MondrianDef.Expression getMondrianDefExpression();

    /**
     * Returns the aggregation function which rolls up this measure: "SUM",
     * "COUNT", etc.
     */
    RolapAggregator getAggregator();

    /**
     * Returns the {@link mondrian.rolap.RolapStar.Measure} from which this
     * member is computed. Untyped, because another implementation might store
     * it somewhere else.
     */
    Object getStarMeasure();
}

// End RolapStoredMeasure.java
