/*
 * Java(TM) OLAP Interface
 */
package javax.olap.cursor;



public interface Time
extends javax.olap.query.querycoremodel.NamedObject {

	// ------------------------------------------------
	// -----   Interface Operations               -----
	// ------------------------------------------------

  public javax.olap.cursor.Time valueOf( java.lang.String s )
    throws javax.olap.OLAPException;

  public java.lang.String toString();

  public void setTime( long time )
    throws javax.olap.OLAPException;

}
