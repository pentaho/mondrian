/*
 * Java(TM) OLAP Interface
 */
package javax.olap.cursor;



public interface CubeCursor
extends javax.olap.query.querytransaction.TransactionalObject, javax.olap.cursor.RowDataAccessor, javax.olap.cursor.Cursor {

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public java.util.List getOrdinateEdge()
    throws javax.olap.OLAPException;

  public java.util.Collection getPageEdge()
    throws javax.olap.OLAPException;

	// ------------------------------------------------
	// -----   Interface Operations               -----
	// ------------------------------------------------

  public void synchronizePages()
    throws javax.olap.OLAPException;

}
