/*
 * Java(TM) OLAP Interface
 */
package javax.olap.metadata;



public interface Hierarchy
extends org.omg.java.cwm.objectmodel.core.CoreClass {

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public javax.olap.metadata.Dimension getDimension()
    throws javax.olap.OLAPException;

  public void setDimension( javax.olap.metadata.Dimension value )
    throws javax.olap.OLAPException;

  public java.util.Collection getCubeDimensionAssociation()
    throws javax.olap.OLAPException;

  public javax.olap.metadata.Dimension getDefaultedDimension()
    throws javax.olap.OLAPException;

  public void setDefaultedDimension( javax.olap.metadata.Dimension value )
    throws javax.olap.OLAPException;

}
