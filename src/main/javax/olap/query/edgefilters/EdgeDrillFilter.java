package javax. olap. query. edgefilters;
import java. util. List; import java. util. Collection;
import javax. olap. OLAPException; import javax. olap. metadata.*;
import javax. olap. query. querycoremodel.*; import javax. olap. query. enumerations.*;
import javax. olap. query. calculatedmembers.*; import javax. olap. query. dimensionfilters.*;
import javax. olap. query. querytransaction.*; import javax. olap. query. sorting.*;


public interface EdgeDrillFilter extends EdgeFilter {
// class scalar attributes
public DrillType getDrillType() throws OLAPException;
public void setDrillType( DrillType input) throws OLAPException;
// class references
public void setHierarchy( Hierarchy input) throws OLAPException;
public Hierarchy getHierarchy() throws OLAPException;
public void setDrillMember( Member input) throws OLAPException;
public Member getDrillMember() throws OLAPException;
public void setTuple( Tuple input) throws OLAPException;
public Tuple getTuple() throws OLAPException;
// class operations
}

