package javax. olap. query. dimensionfilters;
import java. util. List; import java. util. Collection;
import javax. olap. OLAPException; import org. omg. cwm. objectmodel. core.*;
import javax. olap. metadata.*; import javax. olap. query. querycoremodel.*;
import javax. olap. query. enumerations.*; import javax. olap. query. calculatedmembers.*;
import javax. olap. query. edgefilters.*; import javax. olap. query. querytransaction.*;
import javax. olap. query. sorting.*;
public interface DataBasedMemberFilter extends DimensionFilter {


// class scalar attributes
public Boolean getBasedOnPercent() throws OLAPException;
public void setBasedOnPercent( Boolean input) throws OLAPException;
// class references
public void setInput( DataBasedMemberFilterInput input) throws
OLAPException;
public DataBasedMemberFilterInput getInput() throws OLAPException;
// class operations
public DataBasedMemberFilterInput
createDataBasedMemberFilterInput( DataBasedMemberFilterInputType type) throws OLAPException;
}


