/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2003 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 10 August, 2001
*/

package mondrian.rolap;
import mondrian.olap.*;
import mondrian.olap.fun.MondrianEvaluationException;
import mondrian.rolap.agg.AggregationManager;

import java.util.*;
import java.io.PrintWriter;

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
	BatchingCellReader batchingReader;

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
		this.batchingReader = new BatchingCellReader(
			(RolapCube) query.getCube(), pinnedSegments);
		try {
			for (int i = -1; i < axes.length; i++) {
				QueryAxis axis;
				if (i == -1) {
					if (query.slicer != null) {
						axis = new QueryAxis(
							false,
							new FunCall(
								"{}", new Exp[] {query.slicer},
								FunDef.TypeBraces),
							"slicer",
							QueryAxis.SubtotalVisibility.Undefined);
					} else {
						axis = null;
					}
				} else {
					axis = query.axes[i];
				}
				evaluator.cellReader = batchingReader;
				RolapAxis axisResult = executeAxis(evaluator.push(), axis);
				batchingReader.loadAggregations();
				batchingReader.clear();
				evaluator.cellReader = aggregatingReader;
				axisResult = executeAxis(evaluator.push(), axis);

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
			executeBody(query);
		} finally {
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
		RolapCube cube = (RolapCube) query.getCube();
		RolapMember measure = (RolapMember) getMember(
			pos, cube.measuresHierarchy.getDimension());
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
		return new RolapCell(measure,value,cellEvaluator);
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
			Object value = exp.evaluate(evaluator);
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

	private void executeBody(Query query)
	{
		// Compute the cells several times. The first time, use a dummy
		// evaluator which collects requests.
		int count = 0;
		while (true) {
			cellValues = new HashMap();
			//
			this.evaluator.cellReader = this.batchingReader;
			executeStripe(query.axes.length - 1);

			// Retrieve the aggregations collected.
			//
			//
			if (batchingReader.keys.isEmpty()) {
				// We got all of the cells we needed, so the result must be
				// correct.
				return;
			}
			if (count++ > 3) {
				throw Util.newInternal("Query required more than " + count + " iterations");
			}
			batchingReader.loadAggregations();
			batchingReader.clear();
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
		void clear()
		{
			this.keys.clear();
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
		void loadAggregations() {
			AggregationManager.instance().loadAggregations(keys, pinnedSegments);
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

	private void executeStripe(int axis)
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
						Util.discard(getCell(point.ordinals));
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
				executeStripe(axis - 1);
			}
		}
	}
};

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

class RolapCell implements Cell
{
	protected Object value;
	private String formattedValue;

	RolapCell(RolapMember measure, Object value, Evaluator evaluator) {
		this.value = value;
		this.formattedValue = computeFormattedValue(measure, value, evaluator);
	}
	static String computeFormattedValue(
		RolapMember measure, Object value, Evaluator evaluator) {
		return evaluator.format(value);
	}
	public Object getValue() {
		return value;
	}
	public String getFormattedValue() {
		return formattedValue;
	}
	public boolean isNull() {
		return value == Util.nullValue;
	}
	public boolean isError() {
		return value instanceof Throwable;
	}
}

// End RolapResult.java
