package javax. olap. serversidemetadata;
import java. util. List; import java. util. Collection;
import javax. olap. OLAPException; import org. omg. cwm. foundation. expressions.*;
import org. omg. cwm. objectmodel. core.*; import javax. olap. metadata.*;
import org. omg. cwm. analysis. transformation.*;
public interface DimensionDeployment extends org. omg. cwm. objectmodel. core. Class
{
// class scalar attributes
// class references
public void setHierarchyLevelAssociation( HierarchyLevelAssociation input) throws OLAPException;
public HierarchyLevelAssociation getHierarchyLevelAssociation() throws OLAPException;


public void setValueBasedHierarchy( ValueBasedHierarchy input) throws OLAPException;
public ValueBasedHierarchy getValueBasedHierarchy() throws OLAPException;
public void setStructureMap( Collection input) throws OLAPException;
public Collection getStructureMap() throws OLAPException;
public void removeStructureMap( StructureMap input) throws OLAPException;
public void setListOfValues( StructureMap input) throws OLAPException;
public StructureMap getListOfValues() throws OLAPException;
public void setImmediateParent( StructureMap input) throws OLAPException;


public StructureMap getImmediateParent() throws OLAPException;
public void setDeploymentGroup( DeploymentGroup input) throws OLAPException;


public DeploymentGroup getDeploymentGroup() throws OLAPException;
// class operations
}


