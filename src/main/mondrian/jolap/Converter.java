/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Dec 25, 2002
*/
package mondrian.jolap;

import mondrian.olap.*;

import javax.olap.OLAPException;
import java.util.Iterator;

/**
 * Translates Mondrian JOLAP constructs into their equivalent raw Mondrian
 * constructs.
 *
 * <p>For example, {@link #createQuery} converts a {@link MondrianCubeView}
 * into a {@link Query}.</p>
 *
 * @author jhyde
 * @since Dec 25, 2002
 * @version $Id$
 **/
class Converter {
    Query createQuery(MondrianCubeView cubeView) throws OLAPException {
        final QueryAxis[] queryAxes = new QueryAxis[cubeView.getOrdinateEdge().size()];
        for (int i = 0; i < queryAxes.length; i++) {
            queryAxes[i] = convert((MondrianEdgeView) cubeView.getOrdinateEdge().get(i), i);
        }
        final Exp slicer = null;
        final QueryPart[] cellProps = new QueryPart[0];
        final Parameter[] parameters = new Parameter[0];
        final MondrianJolapCube mondrianJolapCube = (MondrianJolapCube) cubeView.cube;
        if (mondrianJolapCube == null) {
            throw new OLAPException("Cube view " + cubeView + " does not have a cube");
        }
        final Query query = new Query(cubeView.connection.mondrianConnection,
                mondrianJolapCube.cube, new Formula[0], queryAxes, slicer,
                cellProps, parameters);
        return query;
    }

    private QueryAxis convert(MondrianEdgeView edgeView, int axisIndex) throws OLAPException {
        Exp exp = null;
        for (Iterator segments = edgeView.getSegment().iterator(); segments.hasNext();) {
            MondrianSegment segment = (MondrianSegment) segments.next();
            Exp exp2 = convert(segment);
            if (exp == null) {
                exp = exp2;
            } else {
                exp = new FunCall("Union", new Exp[] {exp, exp2});
            }
            Util.assertTrue(exp != null);
        }
        if (exp == null) {
            // There were no segments, so we get the dimension views directly
            // from the edge view. We presume that each dimensionView has
            // at most one step manager.
            for (Iterator dimensionViews = edgeView.getDimensionView().iterator(); dimensionViews.hasNext();) {
                MondrianDimensionView dimensionView = (MondrianDimensionView) dimensionViews.next();
                int count = 0;
                MondrianDimensionStepManager stepManager = null;
                for (Iterator stepManagers = dimensionView.getDimensionStepManager().iterator(); stepManagers.hasNext();) {
                    if (count++ > 1) {
                        throw Util.newInternal("DimensionView should not " +
                                "have more than one DimensionStepManager if " +
                                "its owning EdgeView does not have any Segments");
                    }
                    stepManager = (MondrianDimensionStepManager) stepManagers.next();
                }
                Exp exp2 = convert(dimensionView, stepManager);
                if (exp == null) {
                    exp = exp2;
                } else {
                    exp = new FunCall("Crossjoin", new Exp[] {exp, exp2});
                }
            }
        }
        return new QueryAxis(false, exp, AxisOrdinal.get(axisIndex),
                QueryAxis.SubtotalVisibility.Undefined);
    }

    private Exp convert(MondrianSegment segment) throws OLAPException {
        Exp exp = null;
        for (Iterator stepManagers = segment.getDimensionStepManager().iterator(); stepManagers.hasNext();) {
            MondrianDimensionStepManager stepManager = (MondrianDimensionStepManager) stepManagers.next();
            Exp exp2 = convert(stepManager);
            if (exp == null) {
                exp = exp2;
            } else {
                exp = new FunCall("Crossjoin", new Exp[] {exp, exp2});
            }
        }
        return exp;
    }

    private Exp convert(MondrianDimensionStepManager stepManager) throws OLAPException {
        Exp exp = convert((MondrianDimensionView) stepManager.getDimensionView(), stepManager);
        for (Iterator steps = stepManager.getDimensionStep().iterator(); steps.hasNext();) {
            MondrianDimensionStep step = (MondrianDimensionStep) steps.next();
            exp = step.convert(exp);
        }
        return exp;
    }

    /**
     * Converts {@link MondrianDimensionView dimensionView} to an expression. If
     * {@link MondrianDimensionStepManager stepManager} is not null, it applies
     * a sequence of steps to that expression.
     */
    Exp convert(MondrianDimensionView dimensionView, MondrianDimensionStepManager stepManager) throws OLAPException {
        if (dimensionView.dimension == null) {
            throw new OLAPException("Dimension view " + dimensionView + " has no dimension");
        }
        Exp exp = convert(dimensionView.dimension);
        if (stepManager != null) {
            for (Iterator steps = stepManager.getDimensionStep().iterator(); steps.hasNext();) {
                MondrianDimensionStep step = (MondrianDimensionStep) steps.next();
                exp = step.convert(exp);
            }
        }
        return exp;
    }

    Exp convert(MondrianJolapDimension dimension) {
        return new FunCall("Members", Syntax.Property, new Exp[] {dimension.dimension});
    }

}

// End Converter.java
