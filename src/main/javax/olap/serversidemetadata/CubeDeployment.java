package javax. olap. serversidemetadata;
import java. util. List; import java. util. Collection;
import javax. olap. OLAPException; import org. omg. cwm. foundation. expressions.*;
import org. omg. cwm. objectmodel. core.*; import javax. olap. metadata.*;
import org. omg. cwm. analysis. transformation.*;
public interface CubeDeployment extends org. omg. cwm. objectmodel. core. Class
{
// class scalar attributes
// class references
public void setCubeRegion( CubeRegion input) throws OLAPException;


public CubeRegion getCubeRegion() throws OLAPException;
public void setDeploymentGroup( DeploymentGroup input) throws OLAPException;


public DeploymentGroup getDeploymentGroup() throws OLAPException;
public void setContentMap( Collection input) throws OLAPException;
public Collection getContentMap() throws OLAPException;
public void removeContentMap( ContentMap input) throws OLAPException;
// class operations
}


