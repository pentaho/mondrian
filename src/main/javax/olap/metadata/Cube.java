package javax.olap.metadata;
import javax.olap.OLAPException;
import java.util.Collection;
public interface Cube extends org.omg.cwm.objectmodel.core.Class
{
// class scalar attributes
	public Boolean getIsVirtual() throws OLAPException;
	public void setIsVirtual(Boolean input) throws OLAPException;
// class references
	public void setCubeDimensionAssociation(Collection input) throws OLAPException;
	public Collection getCubeDimensionAssociation() throws OLAPException;
	public void removeCubeDimensionAssociation(CubeDimensionAssociation input) throws OLAPException;
	public Schema getSchema() throws OLAPException;
// class operations
}
