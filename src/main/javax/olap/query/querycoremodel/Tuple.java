package javax. olap. query. querycoremodel;
import java. util. List; import java. util. Collection;
import javax. olap. OLAPException; import org. omg. cwm. objectmodel. core.*;
import javax. jmi. reflect.*; import javax. olap. query. calculatedmembers.*;
import javax. olap. query. dimensionfilters.*; import javax. olap. query. derivedattribute.*;
import javax. olap. query. enumerations.*; import javax. olap. cursor.*;
import javax. olap. metadata.*; import javax. olap. query. edgefilters.*;
import javax. olap. query. querytransaction.*; import javax. olap. query. sorting.*;


public interface Tuple extends NamedObject {
// class scalar attributes
// class references
public void setMember( Collection input) throws OLAPException;
public Collection getMember() throws OLAPException;
public void addMember( Member input) throws OLAPException;
public void removeMember( Member input) throws OLAPException;
public EdgeView getOwner() throws OLAPException;
// class operations
}