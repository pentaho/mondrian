/*
 * Java(TM) OLAP Interface
 */
package javax.olap.cursor;



public interface Clob
extends javax.olap.query.querycoremodel.NamedObject {

	// ------------------------------------------------
	// -----   Interface Operations               -----
	// ------------------------------------------------

  public long length()
    throws javax.olap.OLAPException;

  public java.lang.String getSubString( long arg0, int arg1 )
    throws javax.olap.OLAPException;

  public java.io.Reader getCharacterStream()
    throws javax.olap.OLAPException;

  public java.io.InputStream getAsciiStream()
    throws javax.olap.OLAPException;

  public long position( java.lang.String arg0, long arg1 )
    throws javax.olap.OLAPException;

  public long position( Clob arg0, long arg1 )
    throws javax.olap.OLAPException;

}
