package javax.olap.metadata;
import javax.olap.OLAPException;
import java.util.Collection;
import java.util.List;
public interface LevelBasedHierarchy extends Hierarchy
{
// class scalar attributes
// class references
	public void setHierarchyLevelAssociation(Collection input) throws OLAPException;
	public List getHierarchyLevelAssociation() throws OLAPException;
	public void removeHierarchyLevelAssociation(HierarchyLevelAssociation input) throws OLAPException;
	public void moveHierarchyLevelAssociationBefore(HierarchyLevelAssociation before, HierarchyLevelAssociation input) throws OLAPException;
	public void moveHierarchyLevelAssociationAfter(HierarchyLevelAssociation before, HierarchyLevelAssociation input) throws OLAPException;
// class operations
}
