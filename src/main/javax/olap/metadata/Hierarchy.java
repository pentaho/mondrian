package javax.olap.metadata;
import javax.olap.OLAPException;
import java.util.Collection;
public abstract interface Hierarchy extends org.omg.cwm.objectmodel.core.Class
{
// class scalar attributes
// class references
	public Dimension getDimension() throws OLAPException;
	public void setCubeDimensionAssociation(Collection input) throws OLAPException;
	public Collection getCubeDimensionAssociation() throws OLAPException;
	public void addCubeDimensionAssociation(CubeDimensionAssociation input) throws OLAPException;
	public void removeCubeDimensionAssociation(CubeDimensionAssociation input) throws OLAPException;
	public void setDefaultedDimension(Dimension input) throws OLAPException;
	public Dimension getDefaultedDimension() throws OLAPException;
// class operations
}
