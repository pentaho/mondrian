package javax.olap.metadata;
import javax.olap.OLAPException;
import java.util.Collection;
public interface Dimension extends org.omg.cwm.objectmodel.core.Class
{
// class scalar attributes
	public Boolean getIsTime() throws OLAPException;
	public void setIsTime(Boolean input) throws OLAPException;
	public Boolean getIsMeasure() throws OLAPException;
	public void setIsMeasure(Boolean input) throws OLAPException;
// class references
	public void setHierarchy(Collection input) throws OLAPException;
	public Collection getHierarchy() throws OLAPException;
	public void removeHierarchy(Hierarchy input) throws OLAPException;
	public void setMemberSelection(Collection input) throws OLAPException;
	public Collection getMemberSelection() throws OLAPException;
	public void removeMemberSelection(MemberSelection input) throws OLAPException;
	public void setCubeDimensionAssociation(Collection input) throws OLAPException;
	public Collection getCubeDimensionAssociation() throws OLAPException;
	public void addCubeDimensionAssociation(CubeDimensionAssociation input) throws OLAPException;
	public void removeCubeDimensionAssociation(CubeDimensionAssociation input) throws OLAPException;
	public void setDisplayDefault(Hierarchy input) throws OLAPException;
	public Hierarchy getDisplayDefault() throws OLAPException;
	public Schema getSchema() throws OLAPException;
// class operations
}
