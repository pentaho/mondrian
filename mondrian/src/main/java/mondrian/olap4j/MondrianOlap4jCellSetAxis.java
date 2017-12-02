/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.olap4j;

import mondrian.calc.TupleList;
import mondrian.olap.AxisOrdinal;
import mondrian.rolap.RolapAxis;
import mondrian.server.Locus;

import org.olap4j.*;
import org.olap4j.metadata.Member;

import java.util.*;

/**
 * Implementation of {@link org.olap4j.CellSetAxis}
 * for the Mondrian OLAP engine.
 *
 * @author jhyde
 * @since May 24, 2007
 */
class MondrianOlap4jCellSetAxis implements CellSetAxis {
    private final MondrianOlap4jCellSet olap4jCellSet;
    private final mondrian.olap.QueryAxis queryAxis;
    private final RolapAxis axis;

    /**
     * Creates a MondrianOlap4jCellSetAxis.
     *
     * @param olap4jCellSet Cell set
     * @param queryAxis Query axis
     * @param axis Axis
     */
    MondrianOlap4jCellSetAxis(
        MondrianOlap4jCellSet olap4jCellSet,
        mondrian.olap.QueryAxis queryAxis,
        RolapAxis axis)
    {
        assert olap4jCellSet != null;
        assert queryAxis != null;
        assert axis != null;
        this.olap4jCellSet = olap4jCellSet;
        this.queryAxis = queryAxis;
        this.axis = axis;
    }

    public Axis getAxisOrdinal() {
        return Axis.Factory.forOrdinal(
            queryAxis.getAxisOrdinal().logicalOrdinal());
    }

    public CellSet getCellSet() {
        return olap4jCellSet;
    }

    public CellSetAxisMetaData getAxisMetaData() {
        final AxisOrdinal axisOrdinal = queryAxis.getAxisOrdinal();
        if (axisOrdinal.isFilter()) {
            return olap4jCellSet.getMetaData().getFilterAxisMetaData();
        } else {
            return olap4jCellSet.getMetaData().getAxesMetaData().get(
                axisOrdinal.logicalOrdinal());
        }
    }

    public List<Position> getPositions() {
        return new AbstractList<Position>() {
            public Position get(final int index) {
                return new MondrianOlap4jPosition(axis.getTupleList(), index);
            }

            public int size() {
              return Locus.execute(
                  olap4jCellSet.olap4jStatement.olap4jConnection
                  .getMondrianConnection2(),
                  "Getting List<Position>.size", new Locus.Action<Integer>() {
                public Integer execute() {
                  return axis.getTupleList().size();
                }
              });
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
        private final TupleList tupleList;
        private final int index;

        /**
         * Creates a MondrianOlap4jPosition.
         *
         * @param tupleList Tuple list
         * @param index Index of tuple
         */
        public MondrianOlap4jPosition(
            TupleList tupleList,
            int index)
        {
            this.tupleList = tupleList;
            this.index = index;
        }

        public List<Member> getMembers() {
            return new AbstractList<Member>() {
                public Member get(int slice) {
                    final mondrian.olap.Member mondrianMember =
                        tupleList.get(slice, index);
                    return olap4jCellSet.olap4jStatement.olap4jConnection
                        .toOlap4j(mondrianMember);
                }

                public int size() {
                    return tupleList.getArity();
                }
            };
        }

        public int getOrdinal() {
            return index;
        }
    }
}

// End MondrianOlap4jCellSetAxis.java
