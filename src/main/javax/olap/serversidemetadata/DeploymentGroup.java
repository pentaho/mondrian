package javax. olap. serversidemetadata;
import java. util. List; import java. util. Collection;
import javax. olap. OLAPException; import org. omg. cwm. foundation. expressions.*;
import org. omg. cwm. objectmodel. core.*; import javax. olap. metadata.*;
import org. omg. cwm. analysis. transformation.*;
public interface DeploymentGroup extends org. omg. cwm. objectmodel. core. Package
{
// class scalar attributes
// class references
public void setSchema( Schema input) throws OLAPException;


public Schema getSchema() throws OLAPException;
public void setCubeDeployment( Collection input) throws OLAPException;
public Collection getCubeDeployment() throws OLAPException;
public void addCubeDeployment( CubeDeployment input) throws OLAPException;
public void removeCubeDeployment( CubeDeployment input) throws OLAPException;
public void setDimensionDeployment( Collection input) throws OLAPException;
public Collection getDimensionDeployment() throws OLAPException;
public void addDimensionDeployment( DimensionDeployment input) throws OLAPException;


public void removeDimensionDeployment( DimensionDeployment input) throws OLAPException;
// class operations
}
