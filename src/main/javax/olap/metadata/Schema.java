package javax.olap.metadata;
import javax.olap.OLAPException;
import java.util.Collection;
public interface Schema extends org.omg.cwm.objectmodel.core.Package
{
// class scalar attributes
// class references
	public void setCube(Collection input) throws OLAPException;
	public Collection getCube() throws OLAPException;
	public void removeCube(Cube input) throws OLAPException;
	public void setDimension(Collection input) throws OLAPException;
	public Collection getDimension() throws OLAPException;
	public void removeDimension(Dimension input) throws OLAPException;
// class operations
}
