package javax. olap. cursor; 
import java. util. List; import java. util. Collection; 
import javax. olap. OLAPException; import java. io.*; 
import javax. olap. query. querycoremodel.*; import java. math.*; 
import java. util.*; import javax. olap. query. querytransaction.*; 


public abstract interface RowDataAccessor { 
// class scalar attributes 
// class references // class operations 
public InputStream getAsciiStream( int arg0) throws OLAPException; public InputStream getAsciiStream( String arg0) throws OLAPException; 
public BigDecimal getBigDecimal( int arg0) throws OLAPException; public BigDecimal getBigDecimal( String arg0) throws OLAPException; 
public InputStream getBinaryStream( int arg0) throws OLAPException; public InputStream getBinaryStream( String arg0) throws OLAPException; 
public Blob getBlob( int arg0) throws OLAPException; public Blob getBlob( String arg0) throws OLAPException; 
public boolean getBoolean( int arg0) throws OLAPException; public boolean getBoolean( String arg0) throws OLAPException; 
public byte getByte( int arg0) throws OLAPException; public byte getByte( String arg0) throws OLAPException; 
public byte[] getBytes( int arg0) throws OLAPException; public byte[] getBytes( String arg0) throws OLAPException; 
public Reader getCharacterStream( int arg0) throws OLAPException; public Reader getCharacterStream( String arg0) throws OLAPException; 
public Clob getClob( int arg0) throws OLAPException; public Clob getClob( String arg0) throws OLAPException; 
public void getDate( int arg0) throws OLAPException; public void getDate( String arg0) throws OLAPException; 
public void getDate( int arg0, Calendar arg1) throws OLAPException; public void getDate( String arg0, Calendar arg1) throws 
OLAPException; public void close() throws OLAPException; 
public RowDataMetaData getMetaData() throws OLAPException; public double getDouble( int arg0) throws OLAPException; 
public double getDouble( String arg0) throws OLAPException; public float getFloat( int arg0) throws OLAPException; 
public float getFloat( String arg0) throws OLAPException; public int getInt( int arg0) throws OLAPException; 
public int getInt( String arg0) throws OLAPException; public long getLong( int arg0) throws OLAPException; 
public long getLong( String arg0) throws OLAPException; public Object getObject( int arg0) throws OLAPException; 
public Object getObject( String arg0) throws OLAPException; public Object getObject( int arg0, Map arg1) throws OLAPException; 
public Object getObject( String arg0, Map arg1) throws OLAPException; public short getShort( int arg0) throws OLAPException; 
public short getShort( String arg0) throws OLAPException; 
public String getString( int arg0) throws OLAPException; public String getString( String arg0) throws OLAPException; 
public Time getTime( int arg0) throws OLAPException; public Time getTime( String arg0) throws OLAPException; 
public Time getTime( int arg0, Calendar arg1) throws OLAPException; public Time getTime( String arg0, Calendar arg1) throws OLAPException; 
public Timestamp getTimestamp( int arg0) throws OLAPException; public Timestamp getTimestamp( String arg0) throws OLAPException; 
public Timestamp getTimestamp( int arg0, Calendar arg1) throws OLAPException; 
public Timestamp getTimestamp( String arg0, Calendar arg1) throws OLAPException; 
} 

