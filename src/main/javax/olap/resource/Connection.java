package javax.olap.resource;
import javax.jmi.reflect.RefObject;
import javax.jmi.reflect.RefPackage;
import javax.olap.OLAPException;
import javax.olap.metadata.Schema;
import javax.olap.query.querycoremodel.CubeView;
import javax.olap.query.querycoremodel.DimensionView;
import javax.olap.query.querycoremodel.EdgeView;
import javax.olap.query.querycoremodel.MeasureView;
import java.util.List;


public interface Connection extends RefObject {
// class scalar attributes
// class references // class operations
	public void close() throws OLAPException;
	public ConnectionMetaData getMetaData() throws OLAPException;
	public RefPackage getTopLevelPackage() throws OLAPException;
	public List getSchema() throws OLAPException;
	public Schema getCurrentSchema() throws OLAPException;
	public void setCurrentSchema( Schema schema) throws OLAPException;
	public List getDimensions() throws OLAPException;
	public List getCubes() throws OLAPException;
	public CubeView createCubeView() throws OLAPException;
	public DimensionView createDimensionView() throws OLAPException;
	public EdgeView createEdgeView() throws OLAPException;

	MeasureView createMeasureView() throws OLAPException; // jhyde added
}
