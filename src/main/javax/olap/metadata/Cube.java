/*
 * Java(TM) OLAP Interface
 */
package javax.olap.metadata;



public interface Cube
extends org.omg.java.cwm.objectmodel.core.CoreClass {

	// ------------------------------------------------
	// -----   Attribute-Generated                -----
	// ------------------------------------------------

  public boolean isVirtual()
    throws javax.olap.OLAPException;

  public void setVirtual( boolean value )
    throws javax.olap.OLAPException;

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public java.util.Collection getCubeDimensionAssociation()
    throws javax.olap.OLAPException;

  public javax.olap.metadata.Schema getSchema()
    throws javax.olap.OLAPException;

  public void setSchema( javax.olap.metadata.Schema value )
    throws javax.olap.OLAPException;

}
