package javax. olap. resource;
import java. util. List;
import java. util. Collection;
import javax. olap. OLAPException;
import javax. olap. metadata.*;
import javax. olap. query. querycoremodel.*;
import javax. jmi. reflect.*;


public interface Connection extends RefObject {
// class scalar attributes
// class references // class operations
public void close() throws OLAPException; public ConnectionMetaData getMetaData() throws OLAPException;
public RefPackage getTopLevelPackage() throws OLAPException; public List getSchema() throws OLAPException;
public Schema getCurrentSchema() throws OLAPException; public void setCurrentSchema( Schema schema) throws OLAPException;
public List getDimensions() throws OLAPException; public List getCubes() throws OLAPException;
public CubeView createCubeView() throws OLAPException; public DimensionView createDimensionView() throws OLAPException;
public EdgeView createEdgeView() throws OLAPException;
}
