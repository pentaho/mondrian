package javax. olap. query. edgefilters;
import java. util. List; import java. util. Collection;
import javax. olap. OLAPException; import javax. olap. metadata.*;
import javax. olap. query. querycoremodel.*; import javax. olap. query. enumerations.*;
import javax. olap. query. calculatedmembers.*; import javax. olap. query. dimensionfilters.*;
import javax. olap. query. querytransaction.*; import javax. olap. query. sorting.*;


public interface CurrentEdgeMember extends Tuple {
// class scalar attributes
public MemberQuantifierType getMemberQuantifer() throws
OLAPException;
public void setMemberQuantifer( MemberQuantifierType input) throws OLAPException;


// class references
public void setMemberReference( Collection input) throws
OLAPException;
public Collection getMemberReference() throws OLAPException;
public void addMemberReference( QualifiedEdgeMemberReference input) throws OLAPException;


public void removeMemberReference( QualifiedEdgeMemberReference input) throws OLAPException;
// class operations
}


