/*
 * Java(TM) OLAP Interface
 */
package javax.olap.serversidemetadata;



public interface DimensionDeployment
extends org.omg.java.cwm.objectmodel.core.CoreClass {

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public javax.olap.serversidemetadata.HierarchyLevelAssociation getHierarchyLevelAssociation()
    throws javax.olap.OLAPException;

  public void setHierarchyLevelAssociation( javax.olap.serversidemetadata.HierarchyLevelAssociation value )
    throws javax.olap.OLAPException;

  public javax.olap.serversidemetadata.ValueBasedHierarchy getValueBasedHierarchy()
    throws javax.olap.OLAPException;

  public void setValueBasedHierarchy( javax.olap.serversidemetadata.ValueBasedHierarchy value )
    throws javax.olap.OLAPException;

  public java.util.Collection getStructureMap()
    throws javax.olap.OLAPException;

  public javax.olap.serversidemetadata.StructureMap getListOfValues()
    throws javax.olap.OLAPException;

  public void setListOfValues( javax.olap.serversidemetadata.StructureMap value )
    throws javax.olap.OLAPException;

  public javax.olap.serversidemetadata.StructureMap getImmediateParent()
    throws javax.olap.OLAPException;

  public void setImmediateParent( javax.olap.serversidemetadata.StructureMap value )
    throws javax.olap.OLAPException;

  public javax.olap.serversidemetadata.DeploymentGroup getDeploymentGroup()
    throws javax.olap.OLAPException;

  public void setDeploymentGroup( javax.olap.serversidemetadata.DeploymentGroup value )
    throws javax.olap.OLAPException;

}
