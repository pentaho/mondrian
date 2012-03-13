/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2012 Pentaho
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.spi.CellFormatter;
import mondrian.spi.Dialect;

import java.util.*;

/**
 * Measure which is computed from a SQL column (or expression) and which is
 * defined in a non-virtual cube.
 *
 * @see RolapVirtualCubeMeasure
 *
 * @author jhyde
 * @since 24 August, 2006
 */
public class RolapBaseCubeMeasure
    extends RolapMemberBase
    implements RolapStoredMeasure, RolapMemberInCube
{
    /**
     * For SQL generator. Column which holds the value of the measure.
     */
    private final RolapSchema.PhysExpr expression;

    /**
     * For SQL generator. Has values "SUM", "COUNT", etc.
     */
    private final RolapAggregator aggregator;

    private final RolapCube cube;
    private final Map<String, Annotation> annotationMap;

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
     * @param cube Cube
     * @param measureGroup Measure group that this measure belongs to
     * @param parentMember Parent member
     * @param level Level this member belongs to
     * @param name Name of this member
     * @param caption Caption
     * @param description Description
     * @param formatString Format string
     * @param expression Expression (or null if not calculated)
     * @param aggregator Aggregator
     * @param datatype Data type
     * @param annotationMap Annotations
     */
    RolapBaseCubeMeasure(
        RolapCube cube,
        RolapMeasureGroup measureGroup,
        RolapMember parentMember,
        RolapLevel level,
        String name,
        String caption,
        String description,
        String formatString,
        RolapSchema.PhysExpr expression,
        final RolapAggregator aggregator,
        Dialect.Datatype datatype,
        Map<String, Annotation> annotationMap)
    {
        super(parentMember, level, name, null, MemberType.MEASURE);
        assert annotationMap != null;
        this.cube = cube;
        this.annotationMap = annotationMap;
        this.caption = caption;
        this.measureGroup = measureGroup;
        assert measureGroup.getCube() == cube;
        RolapSchema.PhysRelation factRelation = measureGroup.getFactRelation();
        assert factRelation != null;
        assert !(expression instanceof RolapSchema.PhysColumn)
            || (((RolapSchema.PhysColumn) expression).relation == factRelation)
            : "inconsistent fact: " + expression + " vs. " + factRelation;
        this.expression = expression;
        if (description != null) {
            setProperty(
                Property.DESCRIPTION.name,
                description);
        }
        this.aggregator = aggregator;
        if (formatString == null) {
            formatString = "";
        }
        setProperty(
            Property.FORMAT_EXP_PARSED.name,
            Literal.createString(formatString));
        setProperty(
            Property.FORMAT_EXP.name,
            formatString);

        setProperty(Property.AGGREGATION_TYPE.name, this.aggregator);
        this.datatype = datatype;
        setProperty(Property.DATATYPE.name, datatype.name());
    }

    public RolapSchema.PhysExpr getExpr() {
        return expression;
    }

    public RolapAggregator getAggregator() {
        return aggregator;
    }

    public RolapCube getCube() {
        return cube;
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

    @Override
    public Map<String, Annotation> getAnnotationMap() {
        return annotationMap;
    }

    public Dialect.Datatype getDatatype() {
        return datatype;
    }
}

// End RolapBaseCubeMeasure.java
