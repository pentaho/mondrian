/*
 * Java(TM) OLAP Interface
 */
package javax.olap.cursor;



public interface Date
extends javax.olap.query.querycoremodel.NamedObject {

	// ------------------------------------------------
	// -----   Interface Operations               -----
	// ------------------------------------------------

  public void date( long date )
    throws javax.olap.OLAPException;

  public void setTime( long date )
    throws javax.olap.OLAPException;

  public javax.olap.cursor.Date valueOf( java.lang.String s )
    throws javax.olap.OLAPException;

  public java.lang.String toString();

}
