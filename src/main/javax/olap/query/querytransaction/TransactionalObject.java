package javax. olap. query. querytransaction;
import java. util. List; import java. util. Collection;
import javax. olap. OLAPException; import javax. olap. resource.*;
import javax. olap. query. querycoremodel.*; import javax. olap. query. enumerations.*;
import javax. olap. query. calculatedmembers.*; import javax. olap. query. dimensionfilters.*;
import javax. olap. query. edgefilters.*; import javax. olap. query. sorting.*;


public abstract interface TransactionalObject {
// class scalar attributes
// class references
public void setActiveIn( QueryTransaction input) throws OLAPException;


public QueryTransaction getActiveIn() throws OLAPException;
// class operations
}


