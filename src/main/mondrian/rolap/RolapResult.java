/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2003 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 10 August, 2001
*/

package mondrian.rolap;
import mondrian.olap.*;
import mondrian.olap.fun.MondrianEvaluationException;
import mondrian.rolap.agg.AggregationManager;
import mondrian.rolap.agg.CellRequest;
import mondrian.rolap.cache.*;

import java.io.PrintWriter;
import java.util.*;

/**
 * A <code>RolapResult</code> is the result of running a query.
 *
 * @author jhyde
 * @since 10 August, 2001
 * @version $Id$
 */
class RolapResult extends ResultBase
{
	private RolapEvaluator evaluator;
	private CellKey point;
	HashMap cellValues;
	AggregatingCellReader aggregatingReader;
	FastBatchingCellReader batchingReader;
    private int[] modulos;
    private static final int MAX_AGGREGATION_PASS_COUNT = 5;

    RolapResult(Query query) {
		this.query = query;
		this.point = new CellKey(new int[query.axes.length]);
		this.axes = new RolapAxis[query.axes.length];
		this.evaluator = new RolapEvaluator(
				(RolapCube) query.getCube(),
				(RolapConnection) query.getConnection());
		this.aggregatingReader = new AggregatingCellReader();
		final boolean alwaysFlush = MondrianProperties.instance().getFlushAfterQuery();
		final boolean printCacheables = MondrianProperties.instance().getPrintCacheablesAfterQuery();
		HashSet pinnedSegments = new HashSet();
		this.batchingReader = new FastBatchingCellReader(
			(RolapCube) query.getCube(), pinnedSegments);
		try {
			for (int i = -1; i < axes.length; i++) {
				QueryAxis axis;
				if (i == -1) {
					if (query.slicer != null) {
						axis = new QueryAxis(
							false,
							new FunCall(
								"{}", Syntax.Braces, new Exp[] {query.slicer}
                            ).resolve(query.createResolver()),
							"slicer",
							QueryAxis.SubtotalVisibility.Undefined);
					} else {
						axis = null;
					}
				} else {
					axis = query.axes[i];
				}
                RolapAxis axisResult;
                int attempt = 0;
                while (true) {
                    evaluator.cellReader = batchingReader;
                    axisResult = executeAxis(evaluator.push(), axis);
                    evaluator.clearExpResultCache();
                    if (!batchingReader.loadAggregations()) {
                        break;
                    }
                    if (attempt++ > MAX_AGGREGATION_PASS_COUNT) {
                        throw Util.newInternal("Failed to load all aggregations after " +
                                MAX_AGGREGATION_PASS_COUNT +
                                "passes; there's probably a cycle");
                    }
                }

				evaluator.cellReader = aggregatingReader;
				axisResult = executeAxis(evaluator.push(), axis);
				evaluator.clearExpResultCache();

				if (i == -1) {
					this.slicerAxis = axisResult;
					// Use the context created by the slicer for the other
					// axes.  For example, "select filter([Customers], [Store
					// Sales] > 100) on columns from Sales where
					// ([Time].[1998])" should show customers whose 1998 (not
					// total) purchases exceeded 100.
					Position position = this.slicerAxis.positions[0];
					for (int j = 0; j < position.members.length; j++) {
						evaluator.setContext(position.members[j]);
					}
				} else {
					this.axes[i] = axisResult;
				}
			}
            // Suppose the result is 4 x 3 x 2, then modulo = {1, 4, 12, 24}.
            //
            // Then the ordinal of cell (3, 2, 1)
            //  = (modulo[0] * 3) + (modulo[1] * 2) + (modulo[2] * 1)
            //  = (1 * 3) + (4 * 2) + (12 * 1)
            //  = 23
            //
            // Reverse calculation:
            // p[0] = (23 % modulo[1]) / modulo[0] = (23 % 4) / 1 = 3
            // p[1] = (23 % modulo[2]) / modulo[1] = (23 % 12) / 4 = 2
            // p[2] = (23 % modulo[3]) / modulo[2] = (23 % 24) / 12 = 1
            this.modulos = new int[axes.length + 1];
            int modulo = modulos[0] = 1;
            for (int i = 0; i < axes.length; i++) {
                modulo *= axes[i].positions.length;
                modulos[i + 1] = modulo;
            }
			executeBody(query);
		} finally {
			evaluator.clearExpResultCache();
			CachePool.instance().unpin(pinnedSegments);
			if (alwaysFlush) {
				CachePool.instance().flush();
			}
			if (printCacheables) {
				CachePool.instance().validate();
				CachePool.instance().printCacheables(new PrintWriter(System.out));
			}
		}
	}

	// implement Result
	public Axis[] getAxes()
	{
		return axes;
	}
	public Cell getCell(int[] pos)
	{
		if (pos.length != point.ordinals.length) {
			throw Util.newError(
					"coordinates should have dimension " + point.ordinals.length);
		}
		Object value = cellValues.get(new CellKey(pos));
		if (value == null) {
			value = Util.nullValue;
		}
        return new RolapCell(this, getCellOrdinal(pos), value);
	}

    private RolapAxis executeAxis(Evaluator evaluator, QueryAxis axis)
	{
		Position[] positions;
		if (axis == null) {
			// Create an axis containing one position with no members (not
			// the same as an empty axis).
			RolapPosition position = new RolapPosition();
			position.members = new Member[0];
			positions = new Position[] {position};
		} else {
			Exp exp = axis.set;
			evaluator.setNonEmpty(axis.nonEmpty);
			Object value = exp.evaluate(evaluator);
			evaluator.setNonEmpty(false);
			if (value == null) {
				value = Collections.EMPTY_LIST;
			}
			Util.assertTrue(value instanceof List);
			List vector = (List) value;
			positions = new Position[vector.size()];
			for (int i = 0; i < vector.size(); i++) {
				RolapPosition position = new RolapPosition();
				Object o = vector.get(i);
				if (o instanceof Object[]) {
					Object[] a = (Object[]) o;
					position.members = new Member[a.length];
					for (int j = 0; j < a.length; j++) {
						position.members[j] = (Member) a[j];
					}
				} else {
					position.members = new Member[] {(Member) o};
				}
				positions[i] = position;
			}
		}
		return new RolapAxis(positions);
	}

	private void executeBody(Query query) {
		// Compute the cells several times. The first time, use a dummy
		// evaluator which collects requests.
		int count = 0;
        while (true) {
			cellValues = new HashMap();
			//
			evaluator.cellReader = this.batchingReader;
			executeStripe(query.axes.length - 1, (RolapEvaluator) evaluator.push());
			evaluator.clearExpResultCache();

			// Retrieve the aggregations collected.
			//
			//
            if (!batchingReader.loadAggregations()) {
                // We got all of the cells we needed, so the result must be
                // correct.
                return;
            }
			if (count++ > MAX_AGGREGATION_PASS_COUNT) {
				throw Util.newInternal("Query required more than " + count + " iterations");
			}
		}
	}

	/**
	 * A <code>BatchingCellReader</code> doesn't really read cells: when asked
	 * to look up the values of stored measures, it lies, and records the fact
	 * that the value was asked for.  Later, we can look over the values which
	 * are required, fetch them in an efficient way, and re-run the evaluation
	 * with a real evaluator.
	 *
	 * <p>NOTE: When it doesn't know the answer, it lies by returning an error
	 * object.  The calling code must be able to deal with that.</p>
	 **/
	private static class BatchingCellReader implements CellReader
	{
		RolapCube cube;
		HashSet keys;
		HashSet pinnedSegments;
		ArrayList key; // contains [RolapMember 0, ..., RolapMember n - 1]

		BatchingCellReader(RolapCube cube, HashSet pinnedSegments)
		{
			this.cube = cube;
			this.keys = new HashSet();
			this.pinnedSegments = pinnedSegments;
			int dimensionCount = cube.getDimensions().length;
			this.key = new ArrayList(dimensionCount);
			for (int i = 0; i < dimensionCount; i++) {
				this.key.add(null);
			}
		}

        // implement CellReader
		public Object get(Evaluator evaluator)
		{
			RolapMember[] currentMembers =
				((RolapEvaluator) evaluator).currentMembers;
			// try to retrieve a cell and simultaneously pin the segment which
			// contains it
			Object o = AggregationManager.instance().getCellFromCache(
				currentMembers, pinnedSegments);
			if (o == Boolean.TRUE) {
				// Aggregation is being loaded. (todo: Use better value, or
				// throw special exception)
				return RolapUtil.valueNotReadyException;
			}
			if (o != null) {
				return o;
			}
			// if there is no such cell, record that we need to fetch it, and
			// return 'error'
			for (int i = 0, count = currentMembers.length; i < count; i++) {
				key.set(i, currentMembers[i]);
			}
			if (!keys.contains(key)) {
				ArrayList clone = (ArrayList) key.clone();
				keys.add(clone);
			}
			return RolapUtil.valueNotReadyException;
		}

		/**
		 * Loads the aggregations which we will need. Writes the aggregations
		 * it loads (and pins) into <code>pinned</code>; the caller must pass
		 * this to {@link CachePool#unpin(Collection)}.
		 *
         * @return Whether any aggregations were loaded
         *
		 * <h3>Design discussion</h3>
		 *
		 * <p>Do we group them by (a) level, or (b) the underlying columns they
		 * access.  I think the columns.
		 *
		 * <p>Maybe some or all of the group can be derived by rolling up.  We
		 * should probably roll up if possible (the danger is that we end up
		 * with a fragmented aggregation, which we hit many times).
		 *
		 * <p>If we roll up, do we also store?  I think so.  Then we can let the
		 * caching policy kick in.  (It gets interesting if we roll up, but do
		 * not store, part of an aggregation.)
		 *
		 * <p>For each group, extend the aggregation definition a bit, if it
		 * will help us roll up later.
		 **/
		boolean loadAggregations() {
            if (keys.isEmpty()) {
                return false;
            }
			AggregationManager.instance().loadAggregations(keys, pinnedSegments);
            keys.clear();
            return true;
		}
	}

	/**
	 * An <code>AggregatingCellReader</code> reads cell values from the
	 * {@link RolapAggregationManager}.
	 **/
	private static class AggregatingCellReader implements CellReader
	{
		private final RolapAggregationManager aggregationManager = AggregationManager.instance();
		/**
		 * Overrides {@link CellReader#get}. Returns <code>null</code> if no
		 * aggregation contains the required cell.
		 **/
		// implement CellReader
		public Object get(Evaluator evaluator)
		{
			RolapMember[] currentMembers = ((RolapEvaluator) evaluator).currentMembers;
			return aggregationManager.getCellFromCache(currentMembers);
		}
	};

	private void executeStripe(int axis, RolapEvaluator evaluator)
	{
		if (axis < 0) {
			RolapAxis _axis = (RolapAxis) slicerAxis;
			int count = _axis.positions.length;
			for (int i = 0; i < count; i++) {
				RolapPosition position = (RolapPosition) _axis.positions[i];
				for (int j = 0; j < position.members.length; j++) {
					evaluator.setContext(position.members[j]);
				}
				Object o;
				try {
					o = evaluator.evaluateCurrent();
				} catch (MondrianEvaluationException e) {
					o = e;
				}
				if (o != null && o != RolapUtil.valueNotReadyException) {
					CellKey key = point.copy();
					cellValues.put(key, o);
					// Compute the formatted value, to ensure that any needed
					// values are in the cache.
					try {
                        Cell cell = getCell(point.ordinals);
                        Util.discard(cell.getFormattedValue());
					} catch (MondrianEvaluationException e) {
						// ignore
					} catch (Throwable e) {
						Util.discard(e);
					}
				}
			}
		} else {
			RolapAxis _axis = (RolapAxis) axes[axis];
			int count = _axis.positions.length;
			for (int i = 0; i < count; i++) {
				point.ordinals[axis] = i;
				RolapPosition position = (RolapPosition) _axis.positions[i];
				for (int j = 0; j < position.members.length; j++) {
					evaluator.setContext(position.members[j]);
				}
				executeStripe(axis - 1, evaluator);
			}
		}
	}

    /**
     * Converts a cell ordinal to a set of cell coordinates. Converse of
     * {@link #getCellOrdinal}. For example, if this result is 10 x 10 x 10,
     * then cell ordinal 537 has coordinates (5, 3, 7).
     */
    public int[] getCellPos(int cellOrdinal) {
        int[] pos = new int[axes.length];
        for (int j = 0; j < axes.length; j++) {
            pos[j] = (cellOrdinal % modulos[j + 1]) / modulos[j];
        }
        return pos;
    }

    /**
     * Converts a set of cell coordinates to a cell ordinal. Converse of
     * {@link #getCellPos}.
     */
    int getCellOrdinal(int[] pos) {
        int ordinal = 0;
        for (int j = 0; j < axes.length; j++) {
            ordinal += pos[j] * modulos[j];
        }
        return ordinal;
    }

    RolapEvaluator getCellEvaluator(int[] pos) {
        final RolapEvaluator cellEvaluator = (RolapEvaluator) evaluator.push();
        for (int i = 0; i < pos.length; i++) {
            Position position = axes[i].positions[pos[i]];
            for (int j = 0; j < position.members.length; j++) {
                cellEvaluator.setContext(position.members[j]);
            }
        }
        return cellEvaluator;
    }

    Evaluator getEvaluator(int[] pos) {
        // Set up evaluator's context, so that context-dependent format
        // strings work properly.
        Evaluator cellEvaluator = evaluator.push();
        for (int i = -1; i < axes.length; i++) {
            Axis axis;
            int index;
            if (i < 0) {
                axis = slicerAxis;
                index = 0;
            } else {
                axis = axes[i];
                index = pos[i];
            }
            Position position = axis.positions[index];
            for (int j = 0; j < position.members.length; j++) {
                Member member = position.members[j];
                cellEvaluator.setContext(member);
            }
        }
        return cellEvaluator;
    }
}

class RolapAxis extends Axis
{
	private Hashtable mapPositionToIndex = new Hashtable();
	RolapAxis(Position[] positions) {
		this.positions = positions;
		for (int i = 0; i < positions.length; i++) {
			Position position = positions[i];
			mapPositionToIndex.put(position,  new Integer(i));
		}
	}
	int lookupPosition(Position position)
	{
		Integer index = (Integer) mapPositionToIndex.get(position);
		return index == null ? -1 : index.intValue();
	}
};

class RolapPosition extends Position
{
	// override Object
	public boolean equals(Object o)
	{
		if (o instanceof RolapPosition) {
			RolapPosition other = (RolapPosition) o;
			if (other.members.length == this.members.length) {
				for (int i = 0; i < this.members.length; i++) {
					if (this.members[i] != other.members[i]) {
						return false;
					}
				}
				return true;
			}
		}
		return false;
	}
	// override Object
	public int hashCode()
	{
		int h = 0;
		for (int i = 0; i < members.length; i++) {
			h = (h << 4) ^ members[i].hashCode();
		}
		return h;
	}
};

/**
 * <code>RolapCell</code> implements {@link Cell} within a {@link RolapResult}.
 */
class RolapCell implements Cell
{
    private final RolapResult result;
    protected final Object value;
    private final int ordinal;

	RolapCell(RolapResult result, int ordinal, Object value) {
        this.result = result;
        this.value = value;
        this.ordinal = ordinal;
	}

    public Object getValue() {
		return value;
	}
	public String getFormattedValue() {
        final int[] pos = result.getCellPos(ordinal);
        final Evaluator evaluator = result.getEvaluator(pos);
        return evaluator.format(value);
	}
	public boolean isNull() {
		return value == Util.nullValue;
	}
	public boolean isError() {
		return value instanceof Throwable;
	}

	/**
	 * Create an sql query that, when executed, will return the drill through
	 * data for this cell. If the parameter extendedContext is true, then the
	 * query will include all the levels (i.e. columns) of non-constraining members
	 * (i.e. members which are at the "All" level).
	 * If the parameter extendedContext is false, the query will exclude
	 * the levels (coulmns) of non-constraining members.
	 */
	public String getDrillThroughSQL(boolean extendedContext) {
        RolapAggregationManager aggregationManager = AggregationManager.instance();
        CellRequest cellRequest = aggregationManager.makeRequest(
                getEvaluator().currentMembers, extendedContext);
		if (cellRequest == null) {
			return null;
		}
        return aggregationManager.getDrillThroughSQL(cellRequest);
	}

	/**
	 * test if can drill through this cell
	 * drill through is possible if the measure is a stored measure
	 * and not possible for calculated measures
	 * @return true if can drill through
	 */
	public boolean canDrillThrough() {
		// get current members
		final RolapMember[] currentMembers = getEvaluator().currentMembers;
		// first member is the measure, test if it is stored measure, return true if it is, false if not
		return (currentMembers[0] instanceof RolapStoredMeasure);
	}

    private RolapEvaluator getEvaluator() {
        final int[] pos = result.getCellPos(ordinal);
        return result.getCellEvaluator(pos);
    }

    public Object getPropertyValue(String propertyName) {
        if (propertyName.equals(Property.PROPERTY_VALUE)) {
            return getValue();
        } else if (propertyName.equals(Property.PROPERTY_FORMAT_STRING)) {
            return getEvaluator().getFormatString();
        } else if (propertyName.equals(Property.PROPERTY_FORMATTED_VALUE)) {
            return getFormattedValue();
        } else {
            return getEvaluator().getProperty(propertyName);
        }
    }

    public Member getContextMember(Dimension dimension) {
        return result.getMember(result.getCellPos(ordinal), dimension);
    }
}

// End RolapResult.java
