/*
 * Java(TM) OLAP Interface
 */
package javax.olap.metadata;



public interface Dimension
extends org.omg.java.cwm.objectmodel.core.CoreClass {

	// ------------------------------------------------
	// -----   Attribute-Generated                -----
	// ------------------------------------------------

  public boolean isTime()
    throws javax.olap.OLAPException;

  public void setTime( boolean value )
    throws javax.olap.OLAPException;

  public boolean isMeasure()
    throws javax.olap.OLAPException;

  public void setMeasure( boolean value )
    throws javax.olap.OLAPException;

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public java.util.Collection getHierarchy()
    throws javax.olap.OLAPException;

  public java.util.Collection getMemberSelection()
    throws javax.olap.OLAPException;

  public java.util.Collection getCubeDimensionAssociation()
    throws javax.olap.OLAPException;

  public javax.olap.metadata.Hierarchy getDisplayDefault()
    throws javax.olap.OLAPException;

  public void setDisplayDefault( javax.olap.metadata.Hierarchy value )
    throws javax.olap.OLAPException;

  public javax.olap.metadata.Schema getSchema()
    throws javax.olap.OLAPException;

  public void setSchema( javax.olap.metadata.Schema value )
    throws javax.olap.OLAPException;

}
