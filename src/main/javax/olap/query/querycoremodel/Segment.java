/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.querycoremodel;



public interface Segment
extends javax.olap.query.querycoremodel.NamedObject {

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public javax.olap.query.querycoremodel.EdgeView getEdgeView()
    throws javax.olap.OLAPException;

  public void setEdgeView( javax.olap.query.querycoremodel.EdgeView value )
    throws javax.olap.OLAPException;

  public java.util.Collection getDimensionStepManager()
    throws javax.olap.OLAPException;

}
