/*
 * Java(TM) OLAP Interface
 */
package javax.olap.metadata;



public interface HierarchyLevelAssociation
extends org.omg.java.cwm.objectmodel.core.CoreClass {

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public javax.olap.metadata.LevelBasedHierarchy getLevelBasedHierarchy()
    throws javax.olap.OLAPException;

  public void setLevelBasedHierarchy( javax.olap.metadata.LevelBasedHierarchy value )
    throws javax.olap.OLAPException;

  public javax.olap.metadata.Level getCurrentLevel()
    throws javax.olap.OLAPException;

  public void setCurrentLevel( javax.olap.metadata.Level value )
    throws javax.olap.OLAPException;

}
