package javax. olap. serversidemetadata;
import java. util. List; import java. util. Collection;
import javax. olap. OLAPException; import org. omg. cwm. foundation. expressions.*;
import org. omg. cwm. objectmodel. core.*; import javax. olap. metadata.*;
import org. omg. cwm. analysis. transformation.*;
public interface StructureMap extends TransformationMap {


// class scalar attributes
// class references
public void setDimensionDeployment( DimensionDeployment input) throws OLAPException;
public DimensionDeployment getDimensionDeployment() throws OLAPException;


public void setDimensionDeploymentLV( DimensionDeployment input) throws OLAPException;
public DimensionDeployment getDimensionDeploymentLV() throws OLAPException;
public void setDimensionDeploymentIP( DimensionDeployment input) throws OLAPException;
public DimensionDeployment getDimensionDeploymentIP() throws OLAPException;
// class operations
}

