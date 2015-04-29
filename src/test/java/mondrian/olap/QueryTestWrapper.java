/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2015 Pentaho and others
// All Rights Reserved.
 */
package mondrian.olap;

import mondrian.server.Statement;

import static org.mockito.Mockito.spy;

public class QueryTestWrapper extends Query {
    private SchemaReader spyReader;

    public QueryTestWrapper(
        Statement statement,
        Formula[] formulas,
        QueryAxis[] axes,
        String cube,
        QueryAxis slicerAxis,
        QueryPart[] cellProps,
        boolean strictValidation)
    {
        super(
            statement,
            Util.lookupCube(statement.getSchemaReader(), cube, true),
            formulas,
            axes,
            slicerAxis,
            cellProps,
            new Parameter[0],
            strictValidation);
    }

    @Override
    public void resolve() {
        // for testing purposes we want to defer resolution till after
        //  Query init (resolve is called w/in constructor).
        // We do still need formulas to be created, though.
        if (getFormulas() != null) {
            for (Formula formula : getFormulas()) {
                formula.createElement(this);
            }
        }
    }

    @Override
    public synchronized SchemaReader getSchemaReader(
        boolean accessControlled)
    {
        if (spyReader == null) {
            spyReader = spy(super.getSchemaReader(accessControlled));
        }
        return spyReader;
    }
}

// End QueryTestWrapper.java