/*
 * Java(TM) OLAP Interface
 */
package javax.olap.cursor;



public interface Blob
extends javax.olap.query.querycoremodel.NamedObject {

	// ------------------------------------------------
	// -----   Interface Operations               -----
	// ------------------------------------------------

  public long length()
    throws javax.olap.OLAPException;

  public byte[] getBytes( long arg0, int arg1 )
    throws javax.olap.OLAPException;

  public java.io.InputStream getBinaryStream()
    throws javax.olap.OLAPException;

  public long position( byte[] arg0, long arg1 )
    throws javax.olap.OLAPException;

  public long position( Blob arg0, long arg1 )
    throws javax.olap.OLAPException;

}
