/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.querycoremodel;



public interface Tuple
extends javax.olap.query.querycoremodel.NamedObject {

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public java.util.List getMember()
    throws javax.olap.OLAPException;

  public javax.olap.query.querycoremodel.EdgeView getOwner()
    throws javax.olap.OLAPException;

  public void setOwner( javax.olap.query.querycoremodel.EdgeView value )
    throws javax.olap.OLAPException;

}
