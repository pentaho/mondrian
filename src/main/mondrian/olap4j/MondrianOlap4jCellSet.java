/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.olap4j;

import mondrian.olap.Axis;
import mondrian.olap.*;
import mondrian.rolap.RolapAxis;
import mondrian.rolap.RolapCell;
import mondrian.server.Execution;
import mondrian.spi.ProfileHandler;

import org.olap4j.Cell;
import org.olap4j.*;
import org.olap4j.Position;

import java.io.*;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.util.*;

/**
 * Implementation of {@link CellSet}
 * for the Mondrian OLAP engine.
 *
 * <p>This class has sub-classes which implement JDBC 3.0 and JDBC 4.0 APIs;
 * it is instantiated using {@link Factory#newCellSet}.</p>
 *
 * @author jhyde
 * @since May 24, 2007
 */
abstract class MondrianOlap4jCellSet
    extends Execution
    implements CellSet
{
    final MondrianOlap4jStatement olap4jStatement;
    final Query query;
    private Result result;
    protected boolean closed;
    private final MondrianOlap4jCellSetMetaData metaData;
    private final List<CellSetAxis> axisList =
        new ArrayList<CellSetAxis>();
    private CellSetAxis filterAxis;

    /**
     * Creates a MondrianOlap4jCellSet.
     *
     * @param olap4jStatement Statement
     */
    public MondrianOlap4jCellSet(
        MondrianOlap4jStatement olap4jStatement)
    {
        super(olap4jStatement, olap4jStatement.getQueryTimeoutMillis());
        this.olap4jStatement = olap4jStatement;
        this.query = olap4jStatement.getQuery();
        assert query != null;
        this.closed = false;
        if (olap4jStatement instanceof MondrianOlap4jPreparedStatement) {
            this.metaData =
                ((MondrianOlap4jPreparedStatement) olap4jStatement)
                    .cellSetMetaData;
        } else {
            this.metaData =
                new MondrianOlap4jCellSetMetaData(
                    olap4jStatement, query);
        }
    }

    /**
     * Executes a query. Not part of the olap4j API; internal to the mondrian
     * driver.
     *
     * <p>This method may take some time. While it is executing, a client may
     * execute {@link MondrianOlap4jStatement#cancel()}.
     *
     * @throws org.olap4j.OlapException on error
     */
    void execute() throws OlapException {
        result =
            olap4jStatement.olap4jConnection.getMondrianConnection().execute(
                this);

        // initialize axes
        mondrian.olap.Axis[] axes = result.getAxes();
        QueryAxis[] queryAxes = result.getQuery().getAxes();
        assert axes.length == queryAxes.length;
        for (int i = 0; i < axes.length; i++) {
            Axis axis = axes[i];
            QueryAxis queryAxis = queryAxes[i];
            axisList.add(
                new MondrianOlap4jCellSetAxis(
                    this, queryAxis, (RolapAxis) axis));
        }

        // initialize filter axis
        QueryAxis queryAxis = result.getQuery().getSlicerAxis();
        final Axis axis = result.getSlicerAxis();
        if (queryAxis == null) {
            // Dummy slicer axis.
            queryAxis =
                new QueryAxis(
                    false, null, AxisOrdinal.StandardAxisOrdinal.SLICER,
                    QueryAxis.SubtotalVisibility.Undefined);
        }
        filterAxis =
            new MondrianOlap4jCellSetAxis(this, queryAxis, (RolapAxis) axis);
    }

    public CellSetMetaData getMetaData() {
        return metaData;
    }

    public List<CellSetAxis> getAxes() {
        return axisList;
    }

    public CellSetAxis getFilterAxis() {
        return filterAxis;
    }

    public Cell getCell(List<Integer> coordinates) {
        int[] coords = new int[coordinates.size()];
        for (int i = 0; i < coords.length; i++) {
            coords[i] = coordinates.get(i);
        }
        return getCellInternal(coords);
    }

    public Cell getCell(int ordinal) {
        final int[] pos = ordinalToCoordinateArray(ordinal);
        return getCellInternal(pos);
    }

    private int[] ordinalToCoordinateArray(int ordinal) {
        Axis[] axes = result.getAxes();
        final int[] pos = new int[axes.length];
        int modulo = 1;
        for (int i = 0; i < axes.length; i++) {
            int prevModulo = modulo;
            modulo *= axes[i].getPositions().size();
            pos[i] = (ordinal % modulo) / prevModulo;
        }
        if (ordinal < 0 || ordinal >= modulo) {
            throw new IndexOutOfBoundsException(
                "Cell ordinal " + ordinal
                + ") lies outside CellSet bounds ("
                + getBoundsAsString() + ")");
        }
        return pos;
    }

    public Cell getCell(Position... positions) {
        int[] coords = new int[positions.length];
        for (int i = 0; i < coords.length; i++) {
            coords[i] = positions[i].getOrdinal();
        }
        return getCellInternal(coords);
    }

    private Cell getCellInternal(int[] pos) {
        RolapCell cell;
        try {
            cell = (RolapCell) result.getCell(pos);
        } catch (MondrianException e) {
            if (e.getMessage().indexOf("coordinates out of range") >= 0) {
                int[] dimensions = new int[getAxes().size()];
                for (int i = 0; i < axisList.size(); i++) {
                    dimensions[i] = axisList.get(i).getPositions().size();
                }
                throw new IndexOutOfBoundsException(
                    "Cell coordinates (" + getCoordsAsString(pos)
                        + ") fall outside CellSet bounds ("
                        + getCoordsAsString(dimensions) + ")");
            } else if (e.getMessage().indexOf(
                    "coordinates should have dimension") >= 0)
            {
                throw new IllegalArgumentException(
                    "Cell coordinates should have dimension "
                        + axisList.size());
            } else {
                throw e;
            }
        }
        return new MondrianOlap4jCell(pos, this, cell);
    }

    private String getBoundsAsString() {
        StringBuilder buf = new StringBuilder();
        Axis[] axes = result.getAxes();
        for (int i = 0; i < axes.length; i++) {
            if (i > 0) {
                buf.append(", ");
            }
            buf.append(axes[i].getPositions().size());
        }
        return buf.toString();
    }

    private static String getCoordsAsString(int[] pos) {
        StringBuilder buf = new StringBuilder();
        for (int i = 0; i < pos.length; i++) {
            int po = pos[i];
            if (i > 0) {
                buf.append(", ");
            }
            buf.append(po);
        }
        return buf.toString();
    }

    public List<Integer> ordinalToCoordinates(int ordinal) {
        final int[] ints = ordinalToCoordinateArray(ordinal);
        final List<Integer> list = new ArrayList<Integer>(ints.length);
        for (int i : ints) {
            list.add(i);
        }
        return list;
    }

    public int coordinatesToOrdinal(List<Integer> coordinates) {
        List<CellSetAxis> axes = getAxes();
        if (coordinates.size() != axes.size()) {
            throw new IllegalArgumentException(
                "Coordinates have different dimension " + coordinates.size()
                    + " than axes " + axes.size());
        }
        int modulo = 1;
        int ordinal = 0;
        int k = 0;
        for (CellSetAxis axis : axes) {
            final Integer coordinate = coordinates.get(k++);
            if (coordinate < 0 || coordinate >= axis.getPositionCount()) {
                throw new IndexOutOfBoundsException(
                    "Coordinate " + coordinate
                    + " of axis " + k
                    + " is out of range ("
                    + getBoundsAsString() + ")");
            }
            ordinal += coordinate * modulo;
            modulo *= axis.getPositionCount();
        }
        return ordinal;
    }

    public boolean next() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void close() {
        if (closed) {
            return;
        }
        this.closed = true;
        final ProfileHandler profileHandler =
            olap4jStatement.getProfileHandler();
        if (profileHandler != null) {
            final StringWriter stringWriter = new StringWriter();
            final PrintWriter printWriter = new PrintWriter(stringWriter);
            olap4jStatement.getQuery().explain(printWriter);
            printWriter.close();
            profileHandler.explain(stringWriter.toString(), getQueryTiming());
        }
        if (this.result != null) {
            this.result.close();
        }
        olap4jStatement.onResultSetClose(this);
    }

    public boolean wasNull() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public String getString(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean getBoolean(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public byte getByte(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public short getShort(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getInt(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public long getLong(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public float getFloat(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public double getDouble(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public BigDecimal getBigDecimal(
        int columnIndex, int scale) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public byte[] getBytes(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Date getDate(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Time getTime(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public String getString(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean getBoolean(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public byte getByte(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public short getShort(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getInt(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public long getLong(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public float getFloat(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public double getDouble(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public BigDecimal getBigDecimal(
        String columnLabel, int scale) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public byte[] getBytes(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Date getDate(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Time getTime(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public InputStream getUnicodeStream(
        String columnLabel) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public SQLWarning getWarnings() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void clearWarnings() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public String getCursorName() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Object getObject(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Object getObject(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int findColumn(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Reader getCharacterStream(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Reader getCharacterStream(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isBeforeFirst() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isAfterLast() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isFirst() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isLast() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void beforeFirst() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void afterLast() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean first() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean last() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getRow() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean absolute(int row) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean relative(int rows) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean previous() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setFetchDirection(int direction) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getFetchDirection() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setFetchSize(int rows) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getFetchSize() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getType() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getConcurrency() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean rowUpdated() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean rowInserted() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean rowDeleted() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateNull(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBigDecimal(
        int columnIndex, BigDecimal x) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateString(int columnIndex, String x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBytes(int columnIndex, byte x[]) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateTimestamp(
        int columnIndex, Timestamp x) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateAsciiStream(
        int columnIndex, InputStream x, int length) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateBinaryStream(
        int columnIndex, InputStream x, int length) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateCharacterStream(
        int columnIndex, Reader x, int length) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateObject(
        int columnIndex, Object x, int scaleOrLength) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateNull(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBoolean(
        String columnLabel, boolean x) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateShort(String columnLabel, short x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateInt(String columnLabel, int x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateLong(String columnLabel, long x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBigDecimal(
        String columnLabel, BigDecimal x) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateString(String columnLabel, String x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBytes(String columnLabel, byte x[]) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateTimestamp(
        String columnLabel, Timestamp x) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateAsciiStream(
        String columnLabel, InputStream x, int length) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateBinaryStream(
        String columnLabel, InputStream x, int length) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateCharacterStream(
        String columnLabel, Reader reader, int length) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateObject(
        String columnLabel, Object x, int scaleOrLength) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void insertRow() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateRow() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void deleteRow() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void refreshRow() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void cancelRowUpdates() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void moveToInsertRow() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void moveToCurrentRow() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public OlapStatement getStatement() {
        return olap4jStatement;
    }

    public Object getObject(
        int columnIndex, Map<String, Class<?>> map) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public Ref getRef(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Blob getBlob(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Clob getClob(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Array getArray(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Object getObject(
        String columnLabel, Map<String, Class<?>> map) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public Ref getRef(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Blob getBlob(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Clob getClob(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Array getArray(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Timestamp getTimestamp(
        int columnIndex, Calendar cal) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public Timestamp getTimestamp(
        String columnLabel, Calendar cal) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public URL getURL(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public URL getURL(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    // implement Wrapper

    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }
}

// End MondrianOlap4jCellSet.java
