/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.querycoremodel;



public interface SelectedObject
extends javax.olap.query.querycoremodel.NamedObject {

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public javax.olap.query.querycoremodel.DimensionView getOwner()
    throws javax.olap.OLAPException;

  public void setOwner( javax.olap.query.querycoremodel.DimensionView value )
    throws javax.olap.OLAPException;

}
