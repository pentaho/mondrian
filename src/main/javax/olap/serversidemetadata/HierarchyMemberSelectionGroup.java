package javax. olap. serversidemetadata;
import java. util. *;
import org. omg. cwm. foundation. expressions.*;
import org. omg. cwm. objectmodel. core.*;
import javax. olap. metadata.*;
import javax. olap.*;
import org. omg. cwm. analysis. transformation.*;


public interface HierarchyMemberSelectionGroup extends MemberSelectionGroup
{
// class scalar attributes
// class references
public void setHierarchy( Collection input) throws OLAPException;
public Collection getHierarchy() throws OLAPException;
public void addHierarchy( Hierarchy input) throws OLAPException;
public void removeHierarchy( Hierarchy input) throws OLAPException;
// class operations
}


