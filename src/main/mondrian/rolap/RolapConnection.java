/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 2 October, 2002
*/
package mondrian.rolap;

import mondrian.olap.*;

import java.sql.SQLException;
import java.util.Locale;
import java.util.HashMap;
import java.util.ArrayList;

/**
 * A <code>RolapConnection</code> is a connection to a mondrian database.
 *
 * @see RolapSchema
 * @see DriverManager
 * @author jhyde
 * @since 2 October, 2002
 * @version $Id$
 */
public class RolapConnection extends ConnectionBase {
	Util.PropertyList connectInfo;
	java.sql.Connection jdbcConnection;
	String jdbcConnectString;
	String catalogName;
	RolapSchema schema;

	public Schema getSchema() {
		return schema;
	}

	public RolapConnection(Util.PropertyList connectInfo) {
		this(connectInfo, null);
	}

	/**
	 * Creates a RolapConnection.
	 *
	 * <p>Only {@link RolapSchema.Pool#get} calls this with schema != null (to
	 * create a schema's internal connection). Other uses retrieve a schema
	 * from the cache based upon the <code>Catalog</code> property.
	 */
	RolapConnection(Util.PropertyList connectInfo, RolapSchema schema) {
		this.connectInfo = connectInfo;
		this.jdbcConnectString = connectInfo.get("Jdbc");
		this.catalogName = connectInfo.get("Catalog");
		String provider = connectInfo.get("Provider");
		Util.assertTrue(provider.equalsIgnoreCase("mondrian"));
		try {
			this.jdbcConnection = java.sql.DriverManager.getConnection(
				jdbcConnectString);
		} catch (SQLException e) {
			throw Util.getRes().newInternal(
				e,
				"while creating RolapSchema (" +
				connectInfo.toString() + ")");
		}
		if (schema == null && catalogName != null) {
			// If RolapSchema.Pool.get were to call this with schema == null,
			// we would loop.
			schema = RolapSchema.Pool.instance().get(
					catalogName, jdbcConnectString, connectInfo);
		}
		this.schema = schema;
	}

	public void close()
	{
		try {
			if (jdbcConnection != null) {
				jdbcConnection.close();
			}
		} catch (SQLException e) {
			// ignore
		}
	}

	public String getConnectString()
	{
		return connectInfo.toString();
	}
	public String getCatalogName()
	{
		return catalogName;
	}
	public Locale getLocale()
	{
		return Locale.US;
	}
	public Result execute(Query query)
	{
		Result result = new RolapResult(query);
		for (int i = 0; i < query.axes.length; i++) {
			QueryAxis axis = query.axes[i];
			if (axis.nonEmpty) {
				result = new NonEmptyResult(result, query, i);
			}
		}
		return result;
	}
}

/**
 * A <code>NonEmptyResult</code> filters a result by removing empty rows
 * on a particular axis.
 */
class NonEmptyResult extends ResultBase {
	Result underlying;
	int axis;
	HashMap map;
	/** workspace. Synchronized access only. **/
	private int[] pos;

	NonEmptyResult(Result result, Query query, int axis) {
		this.underlying = result;
		this.axis = axis;
		this.map = new HashMap();
		int axisCount = underlying.getAxes().length;
		this.pos = new int[axisCount];
		this.query = query;
		this.axes = (Axis[]) underlying.getAxes().clone();
		this.slicerAxis = underlying.getSlicerAxis();
		Position[] positions = underlying.getAxes()[axis].positions;
		ArrayList positionsList = new ArrayList();
		for (int i = 0, count = positions.length; i < count; i++) {
			Position position = positions[i];
			if (isEmpty(i, axis)) {
				continue;
			} else {
				map.put(new Integer(positionsList.size()), new Integer(i));
				positionsList.add(position);
			}
		}
		this.axes[axis] = new RolapAxis(
			(Position[]) positionsList.toArray(new Position[0]));
	}

	/**
	 * Returns true if all cells at a given offset on a given axis are
	 * empty. For example, in a 2x2x2 dataset, <code>isEmpty(1,0)</code>
	 * returns true if cells <code>{(1,0,0), (1,0,1), (1,1,0),
	 * (1,1,1)}</code> are all empty. As you can see, we hold the 0th
	 * coordinate fixed at 1, and vary all other coordinates over all
	 * possible values.
	 */
	private boolean isEmpty(int offset, int fixedAxis) {
		int axisCount = getAxes().length;
		pos[fixedAxis] = offset;
		return isEmptyRecurse(fixedAxis, axisCount - 1);
	}

	private boolean isEmptyRecurse(int fixedAxis, int axis) {
		if (axis < 0) {
			RolapCell cell = (RolapCell) underlying.getCell(pos);
			return cell.isNull();
		} else if (axis == fixedAxis) {
			return isEmptyRecurse(fixedAxis, axis - 1);
		} else {
			Position[] positions = getAxes()[axis].positions;
			for (int i = 0, count = positions.length; i < count; i++) {
				pos[axis] = i;
				if (!isEmptyRecurse(fixedAxis, axis - 1)) {
					return false;
				}
			}
			return true;
		}
	}

	// synchronized because we use 'pos'
	public synchronized Cell getCell(int[] externalPos) {
		System.arraycopy(externalPos, 0, this.pos, 0, externalPos.length);
		int offset = externalPos[axis];
		int mappedOffset = mapOffsetToUnderlying(offset);
		this.pos[axis] = mappedOffset;
		return underlying.getCell(this.pos);
	}

	private int mapOffsetToUnderlying(int offset) {
		return ((Integer) map.get(new Integer(offset))).intValue();
	}
	// synchronized because we use 'pos'
	public synchronized Member getMember(int[] externalPos, Dimension dimension) {
		System.arraycopy(externalPos, 0, this.pos, 0, externalPos.length);
		int offset = externalPos[axis];
		int mappedOffset = mapOffsetToUnderlying(offset);
		pos[axis] = mappedOffset;
		return underlying.getMember(pos, dimension);
	}

	public void close() {
		underlying.close();
	}
}

// End RolapConnection.java
