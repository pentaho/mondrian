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
import mondrian.spi.CellFormatter;
import mondrian.spi.Dialect;

/**
 * Measure which is computed from a SQL column (or expression).
 */
public class RolapBaseCubeMeasure
    extends RolapMemberBase
    implements RolapStoredMeasure
{
    /**
     * For SQL generator. Column which holds the value of the measure.
     */
    private final RolapSchema.PhysColumn expression;

    /**
     * For SQL generator. Has values "SUM", "COUNT", etc.
     */
    private final RolapAggregator aggregator;

    /**
     * Holds the {@link mondrian.rolap.RolapStar.Measure} from which this
     * member is computed.
     */
    private RolapStar.Measure starMeasure;

    private RolapResult.ValueFormatter formatter;

    private final RolapMeasureGroup measureGroup;
    private final Dialect.Datatype datatype;

    /**
     * Creates a RolapBaseCubeMeasure.
     *
     * @param measureGroup Measure group that this measure belongs to
     * @param level Level this member belongs to
     * @param key Name of this member
     * @param expression Expression (or null if not calculated)
     * @param aggregator Aggregator
     * @param datatype Data type
     * @param larder Larder
     * @param uniqueName Unique name
     */
    RolapBaseCubeMeasure(
        RolapMeasureGroup measureGroup,
        RolapCubeLevel level,
        String key,
        String uniqueName,
        RolapSchema.PhysColumn expression,
        final RolapAggregator aggregator,
        Dialect.Datatype datatype,
        Larder larder)
    {
        super(null, level, key, MemberType.MEASURE, uniqueName, larder);
        assert larder != null;
        this.larder = larder;
        this.measureGroup = measureGroup;
        assert measureGroup.getCube() == level.cube;
        RolapSchema.PhysRelation factRelation = measureGroup.getFactRelation();
        assert factRelation != null;
        assert expression == null || (expression.relation == factRelation)
            : "inconsistent fact: " + expression + " vs. " + factRelation;
        this.expression = expression;
        this.aggregator = aggregator;
        this.datatype = datatype;
    }

    public RolapSchema.PhysColumn getExpr() {
        return expression;
    }

    public RolapAggregator getAggregator() {
        return aggregator;
    }

    public RolapMeasureGroup getMeasureGroup() {
        return measureGroup;
    }

    public RolapResult.ValueFormatter getFormatter() {
        return formatter;
    }

    public void setFormatter(CellFormatter cellFormatter) {
        this.formatter =
            new RolapResult.CellFormatterValueFormatter(cellFormatter);
    }

    public RolapStar.Measure getStarMeasure() {
        return starMeasure;
    }

    void setStarMeasure(RolapStar.Measure starMeasure) {
        this.starMeasure = starMeasure;
    }

    public Dialect.Datatype getDatatype() {
        return datatype;
    }
}

// End RolapBaseCubeMeasure.java
