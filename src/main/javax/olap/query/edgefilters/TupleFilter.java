package javax. olap. query. edgefilters;
import java. util. List; import java. util. Collection;
import javax. olap. OLAPException; import javax. olap. metadata.*;
import javax. olap. query. querycoremodel.*; import javax. olap. query. enumerations.*;
import javax. olap. query. calculatedmembers.*; import javax. olap. query. dimensionfilters.*;
import javax. olap. query. querytransaction.*; import javax. olap. query. sorting.*;


public interface TupleFilter extends EdgeFilter {
// class scalar attributes
// class references
public void setTuple( Tuple input) throws OLAPException;


public Tuple getTuple() throws OLAPException;
public void setEdgeInsertOffset( EdgeInsertOffset input) throws OLAPException;


public EdgeInsertOffset getEdgeInsertOffset() throws OLAPException;
// class operations
public EdgeInsertOffset createEdgeInsertOffset( EdgeInsertOffsetType type) throws OLAPException;
}


