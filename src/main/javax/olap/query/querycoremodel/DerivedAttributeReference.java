/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.querycoremodel;



public interface DerivedAttributeReference
extends javax.olap.query.derivedattribute.DerivedAttributeComponent, javax.olap.query.querycoremodel.SelectedObject, javax.olap.query.calculatedmembers.OperatorInput, javax.olap.query.dimensionfilters.DataBasedMemberFilterInput {

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public javax.olap.query.derivedattribute.DerivedAttribute getDerivedAttribute()
    throws javax.olap.OLAPException;

  public void setDerivedAttribute( javax.olap.query.derivedattribute.DerivedAttribute value )
    throws javax.olap.OLAPException;

}
