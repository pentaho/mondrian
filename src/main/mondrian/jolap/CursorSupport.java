/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Dec 24, 2002
*/
package mondrian.jolap;

import javax.olap.OLAPException;
import javax.olap.cursor.*;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Map;

/**
 * A <code>CursorSupport</code> is ...
 *
 * @author jhyde
 * @since Dec 24, 2002
 * @version $Id$
 **/
abstract class CursorSupport extends QueryObjectSupport
		implements RowDataNavigation, RowDataAccessor, Cursor {
	private int fetchSize;

	CursorSupport() {
		super(false);
	}

	public boolean next() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public void close() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public void beforeFirst() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public void afterLast() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public boolean first() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public int getType() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public boolean isAfterLast() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public boolean isBeforeFirst() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public boolean isFirst() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public boolean isLast() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public boolean last() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public boolean previous() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public boolean relative(int arg0) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public void setFetchDirection(int arg0) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public void setFetchSize(int arg0) throws OLAPException {
		this.fetchSize = arg0;
	}

	public void clearWarnings() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public void getWarnings() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public int getFetchDirection() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public int getFetchSize() throws OLAPException {
		return fetchSize;
	}

	public long getExtent() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public void setPosition(long position) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public long getPosition() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public Object clone() {
		throw new UnsupportedOperationException();
	}

	// RowDataAccessor methods

	public InputStream getAsciiStream(int arg0) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public InputStream getAsciiStream(String arg0) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public BigDecimal getBigDecimal(int arg0) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public BigDecimal getBigDecimal(String arg0) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public InputStream getBinaryStream(int arg0) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public InputStream getBinaryStream(String arg0) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public Blob getBlob(int arg0) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public Blob getBlob(String arg0) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public boolean getBoolean(int arg0) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public boolean getBoolean(String arg0) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public byte getByte(int arg0) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public byte getByte(String arg0) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public byte[] getBytes(int arg0) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public byte[] getBytes(String arg0) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public Reader getCharacterStream(int arg0) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public Reader getCharacterStream(String arg0) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public Clob getClob(int arg0) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public Clob getClob(String arg0) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	// todo: spec: should return a value
	public void getDate(int arg0) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	// todo: spec: should return a value
	public void getDate(String arg0) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	// todo: spec: should return a value
	public void getDate(int arg0, Calendar arg1) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	// todo: spec: should return a value
	public void getDate(String arg0, Calendar arg1) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public RowDataMetaData getMetaData() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public double getDouble(int arg0) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public double getDouble(String arg0) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public float getFloat(int arg0) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public float getFloat(String arg0) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public int getInt(int arg0) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public int getInt(String arg0) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public long getLong(int arg0) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public long getLong(String arg0) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public Object getObject(int arg0) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public Object getObject(String arg0) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public Object getObject(int arg0, Map arg1) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public Object getObject(String arg0, Map arg1) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public short getShort(int arg0) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public short getShort(String arg0) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public String getString(int arg0) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public String getString(String arg0) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public Time getTime(int arg0) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public Time getTime(String arg0) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public Time getTime(int arg0, Calendar arg1) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public Time getTime(String arg0, Calendar arg1) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public Timestamp getTimestamp(int arg0) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public Timestamp getTimestamp(String arg0) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public Timestamp getTimestamp(int arg0, Calendar arg1) throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public Timestamp getTimestamp(String arg0, Calendar arg1) throws OLAPException {
		throw new UnsupportedOperationException();
	}
}

// End CursorSupport.java