package javax. olap. query. dimensionfilters;
import java. util. List; import java. util. Collection;
import javax. olap. OLAPException; import org. omg. cwm. objectmodel. core.*;
import javax. olap. metadata.*; import javax. olap. query. querycoremodel.*;
import javax. olap. query. enumerations.*; import javax. olap. query. calculatedmembers.*;
import javax. olap. query. edgefilters.*; import javax. olap. query. querytransaction.*;
import javax. olap. query. sorting.*;
public interface ExceptionMemberFilter extends DataBasedMemberFilter {


// class scalar attributes
public OperatorType getOp() throws OLAPException;
public void setOp( OperatorType input) throws OLAPException;
public Object getRhs() throws OLAPException;
public void setRhs( Object input) throws OLAPException;


// class references
// class operations
}


