package javax. olap. serversidemetadata;
import java. util. List;
import java. util. Collection;
import javax. olap. OLAPException;
import org. omg. cwm. foundation. expressions.*;
import org. omg. cwm. objectmodel. core.*;
import javax. olap. metadata.*;
import org. omg. cwm. analysis. transformation.*;
public interface MemberSelectionGroup extends org. omg. cwm. objectmodel. core. Class
{
// class scalar attributes
// class references
public void setMemberSelection( Collection input) throws
OLAPException;
public Collection getMemberSelection() throws OLAPException;
public void addMemberSelection( MemberSelection input) throws OLAPException;


public void removeMemberSelection( MemberSelection input) throws OLAPException;
public void setCubeRegion( CubeRegion input) throws OLAPException;
public CubeRegion getCubeRegion() throws OLAPException;
// class operations
}
