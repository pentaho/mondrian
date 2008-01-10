/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2007-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap4j;

import org.olap4j.*;
import org.olap4j.metadata.*;

import java.util.*;

import mondrian.olap.AxisOrdinal;

/**
 * Implementation of {@link org.olap4j.CellSetAxis}
 * for the Mondrian OLAP engine.
 *
 * @author jhyde
 * @version $Id$
 * @since May 24, 2007
 */
class MondrianOlap4jCellSetAxis implements CellSetAxis {
    private final MondrianOlap4jCellSet olap4jCellSet;
    private final mondrian.olap.QueryAxis queryAxis;
    private final mondrian.olap.Axis axis;

    MondrianOlap4jCellSetAxis(
        MondrianOlap4jCellSet olap4jCellSet,
        mondrian.olap.QueryAxis queryAxis,
        mondrian.olap.Axis axis)
    {
        assert olap4jCellSet != null;
        assert queryAxis != null;
        assert axis != null;
        this.olap4jCellSet = olap4jCellSet;
        this.queryAxis = queryAxis;
        this.axis = axis;
    }

    public Axis getAxisOrdinal() {
        switch (queryAxis.getAxisOrdinal()) {
        case SLICER:
            return Axis.FILTER;
        default:
            return Axis.valueOf(queryAxis.getAxisOrdinal().name());
        }
    }

    public CellSet getCellSet() {
        return olap4jCellSet;
    }

    public CellSetAxisMetaData getAxisMetaData() {
        final AxisOrdinal axisOrdinal = queryAxis.getAxisOrdinal();
        switch (axisOrdinal) {
        case SLICER:
            return olap4jCellSet.getMetaData().getFilterAxisMetaData();
        default:
            return olap4jCellSet.getMetaData().getAxesMetaData().get(
                axisOrdinal.logicalOrdinal());
        }
    }

    public List<Position> getPositions() {
        return new AbstractList<Position>() {
            public Position get(final int index) {
                final mondrian.olap.Position mondrianPosition =
                    axis.getPositions().get(index);
                return new MondrianOlap4jPosition(mondrianPosition, index);
            }

            public int size() {
                return axis.getPositions().size();
            }
        };
    }

    public int getPositionCount() {
        return getPositions().size();
    }

    public ListIterator<Position> iterator() {
        return getPositions().listIterator();
    }

    private class MondrianOlap4jPosition implements Position {
        private final mondrian.olap.Position mondrianPosition;
        private final int index;

        public MondrianOlap4jPosition(
            mondrian.olap.Position mondrianPosition, int index) {
            this.mondrianPosition = mondrianPosition;
            this.index = index;
        }

        public List<Member> getMembers() {
            return new AbstractList<Member>() {

                public Member get(int index) {
                    final mondrian.olap.Member mondrianMember =
                        mondrianPosition.get(index);
                    return new MondrianOlap4jMember(
                        olap4jCellSet.olap4jStatement.olap4jConnection.olap4jSchema,
                        mondrianMember);
                }

                public int size() {
                    return mondrianPosition.size();
                }
            };
        }

        public int getOrdinal() {
            return index;
        }
    }
}

// End MondrianOlap4jCellSetAxis.java
