/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.calculatedmembers;



public interface NullHandling
extends javax.olap.query.querycoremodel.NamedObject {

	// ------------------------------------------------
	// -----   Attribute-Generated                -----
	// ------------------------------------------------

  public boolean isNullAsZero()
    throws javax.olap.OLAPException;

  public void setNullAsZero( boolean value )
    throws javax.olap.OLAPException;

  public boolean isNullAsMissing()
    throws javax.olap.OLAPException;

  public void setNullAsMissing( boolean value )
    throws javax.olap.OLAPException;

}
