/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2013 Pentaho and others
// All Rights Reserved.
//
// jhyde, 10 August, 2001
*/
package mondrian.rolap;


/**
 * A measure which is implemented by a SQL column or SQL expression (as opposed
 * to a {@link RolapCalculatedMember}.
 */
public interface RolapStoredMeasure extends RolapMeasure {
    /**
     * Returns the cube this measure belongs to.
     *
     * @return cube
     */
    RolapCube getCube();

    /**
     * Returns the expression that the measure is aggregating.
     * @return expression
     */
    RolapSchema.PhysColumn getExpr();

    /**
     * Returns the aggregate function which rolls up this measure: "SUM",
     * "COUNT", etc.
     *
     * @return aggregate function
     */
    RolapAggregator getAggregator();

    /**
     * Returns the {@link mondrian.rolap.RolapStar.Measure} from which this
     * measure is computed.
     *
     * @return star measure
     */
    RolapStar.Measure getStarMeasure();

    RolapMeasureGroup getMeasureGroup();
}

// End RolapStoredMeasure.java
