/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.querycoremodel;



public interface AttributeReference
extends javax.olap.query.calculatedmembers.OperatorInput, javax.olap.query.querycoremodel.SelectedObject, javax.olap.query.dimensionfilters.DataBasedMemberFilterInput, javax.olap.query.derivedattribute.DerivedAttributeComponent {

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public org.omg.java.cwm.objectmodel.core.Attribute getAttribute()
    throws javax.olap.OLAPException;

  public void setAttribute( org.omg.java.cwm.objectmodel.core.Attribute value )
    throws javax.olap.OLAPException;

}
