package javax. olap. query. dimensionfilters;
import java. util. List;
import java. util. Collection; import javax. olap. OLAPException;
import org. omg. cwm. objectmodel. core.*; import javax. olap. metadata.*;
import javax. olap. query. querycoremodel.*; import javax. olap. query. enumerations.*;
import javax. olap. query. calculatedmembers.*; import javax. olap. query. edgefilters.*;
import javax. olap. query. querytransaction.*; import javax. olap. query. sorting.*;


public abstract interface DataBasedMemberFilterInput {
// class scalar attributes
// class references
public void setDataBasedMemberFilter( DataBasedMemberFilter input)
throws OLAPException;
public DataBasedMemberFilter getDataBasedMemberFilter() throws OLAPException;


// class operations
}


