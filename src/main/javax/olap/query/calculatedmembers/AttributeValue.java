/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.calculatedmembers;



public interface AttributeValue
extends javax.olap.query.querycoremodel.NamedObject {

	// ------------------------------------------------
	// -----   Attribute-Generated                -----
	// ------------------------------------------------

  public java.lang.Object getValue()
    throws javax.olap.OLAPException;

  public void setValue( java.lang.Object value )
    throws javax.olap.OLAPException;

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public javax.olap.query.calculatedmembers.CalculatedMember getCalculatedMember()
    throws javax.olap.OLAPException;

  public void setCalculatedMember( javax.olap.query.calculatedmembers.CalculatedMember value )
    throws javax.olap.OLAPException;

  public org.omg.java.cwm.objectmodel.core.Attribute getAttribute()
    throws javax.olap.OLAPException;

  public void setAttribute( org.omg.java.cwm.objectmodel.core.Attribute value )
    throws javax.olap.OLAPException;

}
