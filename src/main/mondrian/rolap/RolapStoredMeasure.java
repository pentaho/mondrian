/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 10 August, 2001
*/

package mondrian.rolap;
import mondrian.olap.CellFormatter;
import mondrian.olap.MondrianDef;
import mondrian.olap.Property;
import mondrian.olap.Util;

import java.util.List;
import java.util.Arrays;

/**
 * todo:
 *
 * @author jhyde
 * @since 10 August, 2001
 * @version $Id$
 */
class RolapStoredMeasure extends RolapMeasure {

    private static final List datatypeList = Arrays.asList(
            new String[] {"Integer", "Numeric", "String"});

    /** For SQL generator. Column which holds the value of the measure. */
    private final MondrianDef.Expression expression;
    /** For SQL generator. Has values "SUM", "COUNT", etc. */
    private final RolapAggregator aggregator;
    private final RolapCube cube;

    private CellFormatter formatter;

    RolapStoredMeasure(
            RolapCube cube,
            RolapMember parentMember,
            RolapLevel level,
            String name,
            String formatString,
            MondrianDef.Expression expression,
            String aggregatorName,
            String datatype) {
        super(parentMember, level, name, formatString);
        this.cube = cube;
        this.expression = expression;
        this.aggregator = (RolapAggregator)
                RolapAggregator.enumeration.getValue(aggregatorName, true);
        if (this.aggregator == null) {
            throw Util.newError("Unknown aggregator '" + aggregatorName + "'");
        }
        setProperty(Property.AGGREGATION_TYPE.name, aggregator);
        if (datatype == null) {
            if (aggregator == RolapAggregator.Count ||
                    aggregator == RolapAggregator.DistinctCount) {
                datatype = "Integer";
            } else {
                datatype = "Numeric";
            }
        }
        // todo: End-user error.
        Util.assertTrue(datatypeList.contains(datatype),
                "invalid datatype " + datatype);
        setProperty(Property.DATATYPE.name, datatype);
    }

    MondrianDef.Expression getMondrianDefExpression() {
        return expression;
    }
    RolapAggregator getAggregator() {
        return aggregator;
    }
    RolapCube getCube() {
        return cube;
    }
    // implement RolapMeasure
    CellReader getCellReader() {
        return cube.getCellReader();
    }

    public CellFormatter getFormatter(){
        return formatter;
    }
    public void setFormatter(CellFormatter formatter){
        this.formatter = formatter;
    }
}

// End RolapStoredMeasure.java
