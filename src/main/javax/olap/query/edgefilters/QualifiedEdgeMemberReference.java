package javax. olap. query. edgefilters;
import java. util. List; import java. util. Collection;
import javax. olap. OLAPException; import javax. olap. metadata.*;
import javax. olap. query. querycoremodel.*; import javax. olap. query. enumerations.*;
import javax. olap. query. calculatedmembers.*; import javax. olap. query. dimensionfilters.*;
import javax. olap. query. querytransaction.*; import javax. olap. query. sorting.*;
public interface QualifiedEdgeMemberReference extends NamedObject {
// class scalar attributes
// class references
public void setOwner( SuppressOrdinateMemberFilter input) throws
OLAPException;
public SuppressOrdinateMemberFilter getOwner() throws OLAPException;
public void setEdgeMember( Collection input) throws OLAPException;
public Collection getEdgeMember() throws OLAPException;
public void addEdgeMember( CurrentEdgeMember input) throws OLAPException;


public void removeEdgeMember( CurrentEdgeMember input) throws OLAPException;
// class operations
}


