/*
 * Java(TM) OLAP Interface
 */
package javax.olap.cursor;



public interface RowDataMetaData
extends javax.olap.query.querycoremodel.NamedObject {

	// ------------------------------------------------
	// -----   Interface Operations               -----
	// ------------------------------------------------

  public int getColumnCount()
    throws javax.olap.OLAPException;

  public boolean isCaseSensitive( int arg0 )
    throws javax.olap.OLAPException;

  public boolean isCurrency( int arg0 )
    throws javax.olap.OLAPException;

  public boolean isNullable( int arg0 )
    throws javax.olap.OLAPException;

  public boolean isSigned( int arg0 )
    throws javax.olap.OLAPException;

  public int getColumnDisplaySize( int arg0 )
    throws javax.olap.OLAPException;

  public java.lang.String getColumnLabel( int arg0 )
    throws javax.olap.OLAPException;

  public java.lang.String getColumnName( int arg0 )
    throws javax.olap.OLAPException;

  public int getPrecision( int arg0 )
    throws javax.olap.OLAPException;

  public int getScale( int arg0 )
    throws javax.olap.OLAPException;

  public int getColumnType( int arg0 )
    throws javax.olap.OLAPException;

  public java.lang.String getColumnTypeName( int arg0 )
    throws javax.olap.OLAPException;

  public java.lang.String getColumnClassName( int arg0 )
    throws javax.olap.OLAPException;

}
