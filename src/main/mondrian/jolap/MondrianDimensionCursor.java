/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Dec 24, 2002
*/
package mondrian.jolap;

import mondrian.olap.Axis;
import mondrian.olap.Position;

import javax.olap.OLAPException;
import javax.olap.cursor.DimensionCursor;
import javax.olap.cursor.EdgeCursor;
import javax.olap.query.querycoremodel.DimensionStepManager;

/**
 * Implementation of {@link DimensionCursor}.
 *
 * @author jhyde
 * @since Dec 24, 2002
 * @version $Id$
 */
class MondrianDimensionCursor extends CursorSupport implements DimensionCursor {
    private MondrianEdgeCursor edgeCursor;

    public MondrianDimensionCursor(final MondrianEdgeCursor edgeCursor, final Axis axis, final int axisOffset) {
        super(edgeCursor.navigator,
                new Accessor() {
                    public Object getObject(int i) throws OLAPException {
                        int positionIndex = (int) edgeCursor.navigator.getPosition();
                        // RME
                        //final Position position = axis.positions[positionIndex];
                        //return position.members[axisOffset];
                        final Position position = 
                            axis.getPositions().get(positionIndex);
                        return position.get(axisOffset);
                    }
                });
        this.edgeCursor = edgeCursor;
    }

    public long getEdgeStart() throws OLAPException {
        throw new UnsupportedOperationException();
    }

    public void setEdgeStart(long input) throws OLAPException {
        throw new UnsupportedOperationException();
    }

    public long getEdgeEnd() throws OLAPException {
        throw new UnsupportedOperationException();
    }

    public void setEdgeEnd(long input) throws OLAPException {
        throw new UnsupportedOperationException();
    }

    public void setEdgeCursor(EdgeCursor input) throws OLAPException {
        this.edgeCursor = (MondrianEdgeCursor) input;
    }

    public EdgeCursor getEdgeCursor() throws OLAPException {
        return edgeCursor;
    }

    public void setCurrentDimensionStepManager(DimensionStepManager input) throws OLAPException {
        throw new UnsupportedOperationException();
    }

    public DimensionStepManager getCurrentDimensionStepManager() throws OLAPException {
        throw new UnsupportedOperationException();
    }
}

// End MondrianDimensionCursor.java
