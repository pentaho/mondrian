package javax.olap.metadata;
import javax.olap.OLAPException;
import java.util.Collection;
public interface Level extends MemberSelection
{
// class scalar attributes
// class references
	public void setHierarchyLevelAssociation(Collection input) throws OLAPException;
	public Collection getHierarchyLevelAssociation() throws OLAPException;
	public void addHierarchyLevelAssociation(HierarchyLevelAssociation input) throws OLAPException;
	public void removeHierarchyLevelAssociation(HierarchyLevelAssociation input) throws OLAPException;
// class operations
}
