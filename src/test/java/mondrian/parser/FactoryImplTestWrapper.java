/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2015 Pentaho and others
// All Rights Reserved.
 */
package mondrian.parser;

import mondrian.olap.*;
import mondrian.server.Statement;

/**
 * Helper test class which allows injecting a testable Query
 * object during parse.
 *
 * Can be used by instantiating a ParserValidator passing this
 * wrapper class to the JavaccParserValidatorImpl constructor.
 */
public class FactoryImplTestWrapper extends QueryPartFactoryImpl {

    @Override
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
        return new QueryTestWrapper(
            statement, formulae, axes, cube, slicerAxis, cellProps,
            strictValidation);
    }
}

// End FactoryImplTestWrapper.java