package javax.olap.metadata;
import javax.olap.OLAPException;
public interface CubeDimensionAssociation extends org.omg.cwm.objectmodel.core.Class
{
// class scalar attributes
// class references
	public void setDimension(Dimension input) throws OLAPException;
	public Dimension getDimension() throws OLAPException;
	public Cube getCube() throws OLAPException;
	public void setCalcHierarchy(Hierarchy input) throws OLAPException;
	public Hierarchy getCalcHierarchy() throws OLAPException;
// class operations
}
