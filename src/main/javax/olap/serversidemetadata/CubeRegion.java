package javax. olap. serversidemetadata;
import java. util. List; import java. util. Collection;
import javax. olap. OLAPException; import org. omg. cwm. foundation. expressions.*;
import org. omg. cwm. objectmodel. core.*; import javax. olap. metadata.*;
import org. omg. cwm. analysis. transformation.*;
public interface CubeRegion extends org. omg. cwm. objectmodel. core. Class {


// class scalar attributes
public Boolean getIsReadOnly() throws OLAPException;
public void setIsReadOnly( Boolean input) throws OLAPException;
public Boolean getIsFullyRealized() throws OLAPException;
public void setIsFullyRealized( Boolean input) throws OLAPException;
// class references
public void setMemberSelectionGroup( Collection input) throws
OLAPException;
public Collection getMemberSelectionGroup() throws OLAPException;
public void removeMemberSelectionGroup( MemberSelectionGroup input) throws OLAPException;


public void setCube( Cube input) throws OLAPException;
public Cube getCube() throws OLAPException;
public void setCubeDeployment( Collection input) throws OLAPException;
public List getCubeDeployment() throws OLAPException;
public void removeCubeDeployment( CubeDeployment input) throws OLAPException;


public void moveCubeDeploymentBefore( CubeDeployment before, CubeDeployment input) throws OLAPException;
public void moveCubeDeploymentAfter( CubeDeployment before, CubeDeployment input) throws OLAPException;
// class operations
}


