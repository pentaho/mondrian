/*
 * Java(TM) OLAP Interface
 */
package javax.olap.metadata;



public interface CubeDimensionAssociation
extends org.omg.java.cwm.objectmodel.core.CoreClass {

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public javax.olap.metadata.Dimension getDimension()
    throws javax.olap.OLAPException;

  public void setDimension( javax.olap.metadata.Dimension value )
    throws javax.olap.OLAPException;

  public javax.olap.metadata.Cube getCube()
    throws javax.olap.OLAPException;

  public void setCube( javax.olap.metadata.Cube value )
    throws javax.olap.OLAPException;

  public javax.olap.metadata.Hierarchy getCalcHierarchy()
    throws javax.olap.OLAPException;

  public void setCalcHierarchy( javax.olap.metadata.Hierarchy value )
    throws javax.olap.OLAPException;

}
