/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2012-2012 Pentaho
// All Rights Reserved.
*/
package mondrian.parser;

import mondrian.olap.*;
import mondrian.server.Statement;

import java.util.List;

/**
 * Implementation of the factory that makes parse tree nodes.
 */
class QueryPartFactoryImpl
    implements MdxParserValidator.QueryPartFactory
{
    public Query makeQuery(
        Statement statement,
        Formula[] formulae,
        QueryAxis[] axes,
        String cube,
        Exp slicer,
        QueryPart[] cellProps,
        boolean strictValidation)
    {
        final QueryAxis slicerAxis =
            slicer == null
                ? null
                : new QueryAxis(
                    false, slicer, AxisOrdinal.StandardAxisOrdinal.SLICER,
                    QueryAxis.SubtotalVisibility.Undefined, new Id[0]);
        return new Query(
            statement, formulae, axes, cube, slicerAxis, cellProps,
            strictValidation);
    }

    public DrillThrough makeDrillThrough(
        Query query,
        int maxRowCount,
        int firstRowOrdinal,
        List<Exp> returnList)
    {
        return new DrillThrough(
            query, maxRowCount, firstRowOrdinal, returnList);
    }

    public Explain makeExplain(
        QueryPart query)
    {
        return new Explain(query);
    }
}

// End QueryPartFactoryImpl.java
