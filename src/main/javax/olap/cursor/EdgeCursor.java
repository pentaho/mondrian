/*
 * Java(TM) OLAP Interface
 */
package javax.olap.cursor;



public interface EdgeCursor
extends javax.olap.cursor.RowDataNavigation, javax.olap.cursor.Cursor {

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public java.util.List getDimensionCursor()
    throws javax.olap.OLAPException;

  public javax.olap.cursor.CubeCursor getPageOwner()
    throws javax.olap.OLAPException;

  public void setPageOwner( javax.olap.cursor.CubeCursor value )
    throws javax.olap.OLAPException;

  public javax.olap.cursor.CubeCursor getOrdinateOwner()
    throws javax.olap.OLAPException;

  public void setOrdinateOwner( javax.olap.cursor.CubeCursor value )
    throws javax.olap.OLAPException;

  public javax.olap.query.querycoremodel.Segment getCurrentSegment()
    throws javax.olap.OLAPException;

  public void setCurrentSegment( javax.olap.query.querycoremodel.Segment value )
    throws javax.olap.OLAPException;

}
