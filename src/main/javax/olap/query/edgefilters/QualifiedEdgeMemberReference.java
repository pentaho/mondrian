/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.edgefilters;



public interface QualifiedEdgeMemberReference
extends javax.olap.query.querycoremodel.NamedObject {

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public javax.olap.query.edgefilters.SuppressEdgeMemberFilter getOwner()
    throws javax.olap.OLAPException;

  public void setOwner( javax.olap.query.edgefilters.SuppressEdgeMemberFilter value )
    throws javax.olap.OLAPException;

  public java.util.Collection getEdgeMember()
    throws javax.olap.OLAPException;

}
