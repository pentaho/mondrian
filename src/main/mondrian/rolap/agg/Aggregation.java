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

import mondrian.olap.Member;
import mondrian.olap.SchemaReader;
import mondrian.olap.Util;
import mondrian.rolap.RolapStar;
import mondrian.rolap.cache.CachePool;
import mondrian.rolap.sql.SqlQuery;

import java.lang.ref.SoftReference;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
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
 * <b>Note to developers</b>: {@link Segment} implements
 * {@link CachePool.Cacheable}, and must adhere to the contract that imposes.
 * For this class, that means that references to segments must be made using
 * soft references (see {@link CachePool.SoftCacheableReference}) so that they
 * can be garbage-collected.
 *
 * @author jhyde
 * @since 28 August, 2001
 * @version $Id$
 **/
public class Aggregation {
    RolapStar star;

    RolapStar.Column[] columns;

    /** List of soft references to segments. **/
    List segmentRefs;

    boolean oracle = false;

    public Aggregation(RolapStar star, RolapStar.Column[] columns) {
        this.star = star;
        this.columns = columns;
        this.segmentRefs = Collections.synchronizedList(new ArrayList());

        // find out if this is an oracle DB
        Connection con = null;
        try {
            con = star.getJdbcConnection();
            DatabaseMetaData md = con.getMetaData();
            SqlQuery sqlQuery = new SqlQuery(md);
            this.oracle = sqlQuery.isOracle();
        } catch (SQLException e) {
            throw Util.newInternal(e, "could not query Metadata");
        } finally {
            if (con != null) {
                try {
                    con.close();
                } catch (SQLException e) {
                    // ignore
                }
            }
        }
    }

    /**
     * Loads a set of segments into this aggregation, one per measure,
     * each constrained by the same set of column values, and each pinned
     * once.
     *
     * For example,
     *   measures = {unit_sales, store_sales},
     *   state = {CA, OR},
     *   gender = unconstrained
     */
    public synchronized void load(RolapStar.Measure[] measures,
            ColumnConstraint[][] constraintses, Collection pinnedSegments) {
        Segment[] segments = new Segment[measures.length];
        for (int i = 0; i < measures.length; i++) {
            RolapStar.Measure measure = measures[i];
            Segment segment = new Segment(this, measure, constraintses);
            segments[i] = segment;
            SoftReference ref = new SoftReference(segment);
            segmentRefs.add(ref);
            pinnedSegments.add(segment);
        }
        Segment.load(segments, pinnedSegments);
    }


    /**
     * Drops constraints, where the list of values is close to the values which
     * would be returned anyway.
     **/
    public synchronized ColumnConstraint[][] optimizeConstraints(ColumnConstraint[][] constraintses) {
        final int MAXLEN_ORACLE = 1000;
        Util.assertTrue(constraintses.length == columns.length);
        ColumnConstraint[][] newConstraintses = (ColumnConstraint[][]) constraintses.clone();
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
            } else {
                // Oracle can only handle up to 1000 elements inside an IN(..) clause
                if (oracle && newConstraintses[i].length > MAXLEN_ORACLE) {
                    bloats[i] = 1.0; // will be optimized away
                    continue;
                }
                // more than one - check for children of same parent
                Member parent = null;
                for (int j = 0; j < newConstraintses[i].length; j++) {
                    if (!(newConstraintses[i][j].isMember())) {
                        // should not occur - but
                        //  we compute bloat by #constraints / column cardinality
                        parent = null;
                        bloats[i] =  constraintLength / columns[i].getCardinality();
                        break;
                    } else {
                        Member m = newConstraintses[i][j].getMember();
                        if (j == 0) {
                            parent = m.getParentMember();
                        } else {
                            if (parent != null
                                    && !parent.equals(m.getParentMember())) {
                                parent = null; // no common parent
                                break;
                            }
                        }
                    }
                }
                if (parent != null) {
                    // common parent exists
                    if (parent.isAll() || potentialParents.contains(parent) ) {
                        // common parent is there as constraint
                        //  if the children are complete, this constraint set is unneccessary
                        // try to get the children directly from cache
                        // for the drilldown case, the children will be in the cache
                        // - if not, forget this optimization.
                        int nChildren = -1;
                        SchemaReader scr = star.getSchema().getSchemaReader();
                        nChildren = scr.getChildrenCountFromCache(parent);

                        if (nChildren == -1) {
                            // nothing gotten from cache
                            if (parent.isAll()) {
                                bloats[i] = constraintLength / columns[i].getCardinality();
                            } else {
                                // no information about children cardinality
                                //  constraints will not be optimized away
                                bloats[i] = 0.0;
                            }
                        } else {
                            bloats[i] = constraintLength / nChildren;
                        }
                    } else {
                        // the parent is not in the constraints
                        bloats[i] = constraintLength / columns[i].getCardinality();
                    }
                } else {
                    // no common parent
                    bloats[i] = constraintLength / columns[i].getCardinality();
                }

            }
        }

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
        double[] bloats;

        ConstraintComparator(double[] bloats) {
            this.bloats = bloats;
        }

        // implement Comparator
        // order by bloat descending
        public int compare(Object o0, Object o1) {
            double bloat0 = bloats[((Integer)o0).intValue()];
            double bloat1 = bloats[((Integer)o1).intValue()];
            if (bloat0 == bloat1) {
                return 0;
            } else if (bloat0 < bloat1) {
                return 1;
            } else {
                return -1;
            }
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
    public synchronized Object get(RolapStar.Measure measure, Object[] keys,
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
                if (segment.wouldContain(keys)) {
                    if (pinSet != null) {
                        pinSet.add(segment);
                    }
                    return null;
                }
            }
        }
        return null;
    }

    public RolapStar.Column[] getColumns() {
        return columns;
    }

    public RolapStar getStar() {
        return star;
    }

    // -- classes -------------------------------------------------------------

    public static class Axis {
        RolapStar.Column column;

        ColumnConstraint[] constraints; // null if no constraint

        Object[] keys; // actual keys retrieved

        HashMap mapKeyToOffset; // inversion of keys

        boolean contains(Object key) {
            if (constraints == null) {
                return true;
            }
            for (int i = 0; i < constraints.length; i++) {
                if (constraints[i].getValue().equals(key)) {
                    return true;
                }
            }
            return false;
        }

        double getBytes() {
            if (keys == null) {
                return 0;
            }
            return 16 + 8 * keys.length;
        }
    }

}

// End Aggregation.java
