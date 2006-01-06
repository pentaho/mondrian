/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 28 August, 2001
 */

package mondrian.rolap.agg;

import mondrian.olap.Level;
import mondrian.olap.Member;
import mondrian.olap.SchemaReader;
import mondrian.olap.Util;
import mondrian.rolap.RolapStar;
import mondrian.rolap.BitKey;
import mondrian.rolap.sql.SqlQuery;

import java.lang.ref.SoftReference;
import java.util.*;

/**
 * A <code>Aggregation</code> is a pre-computed aggregation over a set of
 * columns.
 *
 * <p>Rollup operations:<ul>
 * <li>drop an unrestricted column (e.g. state=*)</li>
 * <li>tighten any restriction (e.g. year={1997,1998} becomes
 *     year={1997})</li>
 * <li>restrict an unrestricted column (e.g. year=* becomes
 *     year={1997})</li>
 * </ul>
 *
 * <p>Representation of aggregations. Sparse and dense representations are
 * necessary for different data sets. Should adapt automatically. Use an
 * interface to hold the data set, so the segment doesn't care.</p>
 *
 * Suppose we have a segment {year=1997, quarter={1,2,3},
 * state={CA,WA}}. We want to roll up to a segment for {year=1997,
 * state={CA,WA}}.  We need to know that we have all quarters.  We don't.
 * Because year and quarter are independent, we know that we have all of
 * the ...</p>
 *
 * <p>Suppose we have a segment specified by {region=West, state=*,
 * year=*}, which materializes to ({West}, {CA,WA,OR}, {1997,1998}).
 * Because state=*, we can rollup to {region=West, year=*} or {region=West,
 * year=1997}.</p>
 *
 * <p>The space required for a segment depends upon the dimensionality (d),
 * cell count (c) and the value count (v). We don't count the space
 * required for the actual values, which is the same in any scheme.</p>
 *
 * @author jhyde
 * @since 28 August, 2001
 * @version $Id$
 **/
public class Aggregation {
    private static final int MAX_CONSTRAINTS_ORACLE = 1000;
    private static final int MAX_CONSTRAINTS_MSSQL = 10000;
    private static final int MAX_CONSTRAINTS_DB2 = 2500;
    private static final int MAX_CONSTRAINTS_DEFAULT = 10000;
    private int maxConstraints;

    private final RolapStar star;

    private final BitKey bitKey;

    /**
     * List of soft references to segments.
     * Access must be inside of synchronized methods.
     **/
    private final List segmentRefs;

    /**
     * This is set in the load method and is used during
     * the processing of a particular aggregate load.
     * The AggregateManager synchonizes access to each instance's methods
     * so that an instance is processing a single set of columns, measures,
     * contraints at any one time.
     */
    private RolapStar.Column[] columns;

    public Aggregation(RolapStar star,
                       BitKey bitKey) {
        this.star = star;
        this.bitKey = bitKey;
        this.segmentRefs = new ArrayList();

        SqlQuery.Dialect dialect = star.getSqlQueryDialect();
        if (dialect.isOracle())
            maxConstraints = MAX_CONSTRAINTS_ORACLE;
        else if (dialect.isMSSQL())
            maxConstraints = MAX_CONSTRAINTS_MSSQL;
        else if (dialect.isDB2())
            maxConstraints = MAX_CONSTRAINTS_DB2;
        else
            maxConstraints = MAX_CONSTRAINTS_DEFAULT;
    }

    /**
     * Loads a set of segments into this aggregation, one per measure,
     * each constrained by the same set of column values, and each pinned
     * once.
     * <p>
     * A Column and its constraints are accessed at the same level in their
     * respective arrays.
     *
     * For example,
     *   measures = {unit_sales, store_sales},
     *   state = {CA, OR},
     *   gender = unconstrained
     */
    public synchronized void load(RolapStar.Column[] columns,
                                  RolapStar.Measure[] measures,
                                  ColumnConstraint[][] constraintses,
                                  Collection pinnedSegments) {
        this.columns = columns;

        BitKey measureBK = bitKey.emptyCopy();
        int axisCount = columns.length;
        Util.assertTrue(constraintses.length == axisCount);

        // This array of Aggregation.Axis is shared by all Segments for
        // this set of measures and constraintses
        Aggregation.Axis[] axes = new Aggregation.Axis[axisCount];
        for (int i = 0; i < axisCount; i++) {
            axes[i] = new Aggregation.Axis(columns[i], constraintses[i]);
        }

        boolean isDistinct = false;
        Segment[] segments = new Segment[measures.length];
        for (int i = 0; i < measures.length; i++) {
            RolapStar.Measure measure = measures[i];

            if (measure.getAggregator().isDistinct()) {
                isDistinct = true;
            }

            measureBK.setByPos(measure.getBitPosition());
            Segment segment = new Segment(this, measure, constraintses, axes);
            segments[i] = segment;
            SoftReference ref = new SoftReference(segment);
            segmentRefs.add(ref);
            pinnedSegments.add(segment);
        }
        Segment.load(segments, bitKey, measureBK, isDistinct, pinnedSegments, axes);

    }


    /**
     * Drops constraints, where the list of values is close to the values which
     * would be returned anyway.
     **/
    public synchronized ColumnConstraint[][] optimizeConstraints(
                       RolapStar.Column[] columns,
            ColumnConstraint[][] constraintses) {

        Util.assertTrue(constraintses.length == columns.length);
        ColumnConstraint[][] newConstraintses =
            (ColumnConstraint[][]) constraintses.clone();
        double[] bloats = new double[columns.length];

        // We want to handle the special case "drilldown" which occurs pretty often.
        // Here, the parent is here as a constraint with a single member
        //  and the list of children as well.
        List potentialParents = new ArrayList();
        for (int i = 0; i < constraintses.length; i++) {
            if (constraintses[i] != null && constraintses[i].length == 1
                    && constraintses[i][0].isMember())
                potentialParents.add(constraintses[i][0].getMember());
        }

        for (int i = 0; i < newConstraintses.length; i++) {
            double constraintLength = (double) newConstraintses[i].length;
            // a set of constraints with only one entry will not be optimized away
            if (newConstraintses[i] == null || newConstraintses[i].length < 2) {
                bloats[i] = 0.0;
            } else if (newConstraintses[i].length > maxConstraints) {
                // DBs can handle only a limited number of elements in WHERE IN (...)
                // so hopefully there are other constraints that will limit the result
                bloats[i] = 1.0; // will be optimized away
                continue;
            }
            // more than one - check for children of same parent
            Member parent = null;
            Level level = null;
            for (int j = 0; j < newConstraintses[i].length; j++) {
                if (!(newConstraintses[i][j].isMember())) {
                    // should not occur - but
                    //  we compute bloat by #constraints / column cardinality
                    parent = null;
                    level = null;
                    bloats[i] =  constraintLength / columns[i].getCardinality();
                    break;
                } else {
                    Member m = newConstraintses[i][j].getMember();
                    if (j == 0) {
                        parent = m.getParentMember();
                        level = m.getLevel();
                    } else {
                        if (parent != null
                                && !parent.equals(m.getParentMember())) {
                            parent = null; // no common parent
                        }
                        if (level != null
                                && !level.equals(m.getLevel())) {
                            // should never occur, constraintses are of same level
                            level = null;
                        }
                    }
                }
            }
            boolean done = false;
            if (parent != null) {
                // common parent exists
                if (parent.isAll() || potentialParents.contains(parent) ) {
                    // common parent is there as constraint
                    //  if the children are complete, this constraint set is
                    //  unneccessary try to get the children directly from
                    //  cache for the drilldown case, the children will be
                    //  in the cache
                    // - if not, forget this optimization.
                    int nChildren = -1;
                    SchemaReader scr = star.getSchema().getSchemaReader();
                    nChildren = scr.getChildrenCountFromCache(parent);

                    if (nChildren == -1) {
                        // nothing gotten from cache
                        if (!parent.isAll()) {
                            // parent is in constraints
                            // no information about children cardinality
                            //  constraints must not be optimized away
                            bloats[i] = 0.0;
                            done = true;
                        }
                    } else {
                        bloats[i] = constraintLength / nChildren;
                        done = true;
                    }
                }
            }

            if (!done && level != null) {
                // if the level members are cached, we do not need "count *"
                int nMembers = -1;
                SchemaReader scr = star.getSchema().getSchemaReader();
                nMembers = scr.getLevelCardinalityFromCache(level);
                if ( nMembers > 0 ) {
                    bloats[i] = constraintLength / nMembers;
                    done = true;
                }
            }

            if (!done) {
                bloats[i] = constraintLength / columns[i].getCardinality();
            }
        } // for  i < newConstraintses.length

        // build a list of constraints sorted by 'bloat factor'
        ConstraintComparator comparator = new ConstraintComparator(bloats);
        Integer[] indexes = new Integer[columns.length];
        for (int i = 0; i < columns.length; i++) {
            indexes[i] = new Integer(i);
        }

        // sort indexes by bloat descending
        Arrays.sort(indexes, comparator);

        // eliminate constraints one by one, until the estimated cell count
        // doubles. We can not have an absolute value here, because its
        // very different if we fetch data for 2 years or 10 years (5 times
        // more means 5 times slower). So a relative comparison is ok here
        // but not an absolute one.

        double abloat = 1.0;
        final double aBloatLimit = 0.5;
        for (int i = 0; i < indexes.length; i++) {
            int j = indexes[i].intValue();
            abloat = abloat * bloats[j];
            if (abloat > aBloatLimit) {
                // eliminate this constraint
                newConstraintses[j] = null;
            } else {
                break;
            }
        }
        return newConstraintses;
    }

    private class ConstraintComparator implements Comparator {
        private final double[] bloats;

        ConstraintComparator(double[] bloats) {
            this.bloats = bloats;
        }

        // implement Comparator
        // order by bloat descending
        public int compare(Object o0, Object o1) {
            double bloat0 = bloats[((Integer)o0).intValue()];
            double bloat1 = bloats[((Integer)o1).intValue()];
            return (bloat0 == bloat1)
                ? 0
                : (bloat0 < bloat1)
                    ? 1
                    : -1;
        }

    }

    /**
     * Retrieves the value identified by <code>keys</code>.
     *
     * <p>If <code>pinSet</code> is not null, pins the
     * segment which holds it. <code>pinSet</code> ensures that a segment is
     * only pinned once.
     *
     * Returns <code>null</code> if no segment contains the cell.
     **/
    public synchronized Object get(RolapStar.Measure measure,
                                   Object[] keys,
                                   Collection pinSet) {

        for (Iterator it = segmentRefs.iterator(); it.hasNext();) {
            SoftReference ref = (SoftReference) it.next();
            Segment segment = (Segment) ref.get();
            if (segment == null) {
                it.remove();
                continue; // it's being garbage-collected
            }
            if (segment.measure != measure) {
                continue;
            }
            if (segment.isReady()) {
                Object o = segment.get(keys);
                if (o != null) {
                    if (pinSet != null) {
                        pinSet.add(segment);
                    }
                    return o;
                }
            } else {
                // avoid to call wouldContain - its slow
                if (pinSet != null
                        && !pinSet.contains(segment)
                        && segment.wouldContain(keys))
                {
                    pinSet.add(segment);
                }
            }
        }
        return null;
    }

    /**
     * This is called during Sql generation.
     *
     * @return
     */
    RolapStar.Column[] getColumns() {
        return columns;
    }

    /**
     * This is called during Sql generation.
     *
     * @return
     */
    RolapStar getStar() {
        return star;
    }

    // -- classes -------------------------------------------------------------

    static class Axis {

        // null if no constraint
        private final ColumnConstraint[] constraints;

        // inversion of keys
        private final Map mapKeyToOffset;

        // actual keys retrieved
        private Object[] keys;

        private Set valueSet;

        Axis(RolapStar.Column column,
            ColumnConstraint[] constraints) {
            this.constraints = constraints;
            this.mapKeyToOffset = new HashMap();
            if (constraints != null) {
                valueSet = new HashSet();
                for (int i = 0; i < constraints.length; i++)
                    valueSet.add(constraints[i].getValue());
            }
        }

        ColumnConstraint[] getConstraints() {
            return constraints;
        }

        Object[] getKeys() {
            return this.keys;
        }

        int loadKeys() {
            int size = mapKeyToOffset.size();
            this.keys = new Object[size];
            Iterator it = mapKeyToOffset.entrySet().iterator();
            while(it.hasNext()) {
                Map.Entry e = (Map.Entry) it.next();
                Object key = e.getKey();
                Integer offsetInteger = (Integer) e.getValue();
                int offset = offsetInteger.intValue();

                this.keys[offset] = key;
            }
            return size;
        }

        Integer getOffset(Object key) {
            return (Integer) mapKeyToOffset.get(key);
        }

        void addNextOffset(Object key) {
            int size = mapKeyToOffset.size();
            mapKeyToOffset.put(key, new Integer(size));
        }

        boolean contains(Object key) {
            if (constraints == null) {
                return true;
            }
            return valueSet.contains(key);
        }
    }

}

// End Aggregation.java
