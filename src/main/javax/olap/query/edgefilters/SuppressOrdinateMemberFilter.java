package javax. olap. query. edgefilters;
import java. util. List; import java. util. Collection;
import javax. olap. OLAPException; import javax. olap. metadata.*;
import javax. olap. query. querycoremodel.*; import javax. olap. query. enumerations.*;
import javax. olap. query. calculatedmembers.*; import javax. olap. query. dimensionfilters.*;
import javax. olap. query. querytransaction.*; import javax. olap. query. sorting.*;


public interface SuppressOrdinateMemberFilter extends EdgeFilter {
// class scalar attributes
// class references
public void setEdgeMember( QualifiedEdgeMemberReference input) throws
OLAPException;
public QualifiedEdgeMemberReference getEdgeMember() throws OLAPException;


// class operations
public QualifiedEdgeMemberReference
createQualifiedEdgeMemberReference() throws OLAPException;
}
