/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.querycoremodel;



public interface EdgeFilter
extends javax.olap.query.querycoremodel.NamedObject {

	// ------------------------------------------------
	// -----   Attribute-Generated                -----
	// ------------------------------------------------

  public javax.olap.query.enumerations.SetEdgeActionType getSetEdgeAction()
    throws javax.olap.OLAPException;

  public void setSetEdgeAction( javax.olap.query.enumerations.SetEdgeActionType value )
    throws javax.olap.OLAPException;

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public javax.olap.query.querycoremodel.EdgeView getOwner()
    throws javax.olap.OLAPException;

  public void setOwner( javax.olap.query.querycoremodel.EdgeView value )
    throws javax.olap.OLAPException;

}
