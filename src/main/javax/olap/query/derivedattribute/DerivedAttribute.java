/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.derivedattribute;



public interface DerivedAttribute
extends javax.olap.query.querycoremodel.NamedObject, javax.olap.query.derivedattribute.DerivedAttributeComponent {

	// ------------------------------------------------
	// -----   Attribute-Generated                -----
	// ------------------------------------------------

  public javax.olap.query.enumerations.DAOperator getOperator()
    throws javax.olap.OLAPException;

  public void setOperator( javax.olap.query.enumerations.DAOperator value )
    throws javax.olap.OLAPException;

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public java.util.List getComponent()
    throws javax.olap.OLAPException;

  public javax.olap.query.querycoremodel.DimensionView getDimensionView()
    throws javax.olap.OLAPException;

  public void setDimensionView( javax.olap.query.querycoremodel.DimensionView value )
    throws javax.olap.OLAPException;

	// ------------------------------------------------
	// -----   Interface Operations               -----
	// ------------------------------------------------

  public javax.olap.query.derivedattribute.DerivedAttributeComponent createComponent( javax.olap.query.enumerations.DerivedAttributeComponentType componentType )
    throws javax.olap.OLAPException;

  public javax.olap.query.derivedattribute.DerivedAttributeComponent createComponentBefore( javax.olap.query.enumerations.DerivedAttributeComponentType componentType, javax.olap.query.derivedattribute.DerivedAttributeComponent member )
    throws javax.olap.OLAPException;

  public javax.olap.query.derivedattribute.DerivedAttributeComponent createComponentAfter( javax.olap.query.enumerations.DerivedAttributeComponentType componentType, javax.olap.query.derivedattribute.DerivedAttributeComponent member )
    throws javax.olap.OLAPException;

}
