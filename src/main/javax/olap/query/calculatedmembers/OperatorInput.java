package javax. olap. query. calculatedmembers;
import java. util. List; import java. util. Collection;
import javax. olap. OLAPException; import org. omg. cwm. objectmodel. core.*;
import javax. olap. query. querycoremodel.*; import javax. olap. query. enumerations.*;
import javax. olap. metadata.*; import javax. olap. query. dimensionfilters.*;
import javax. olap. query. edgefilters.*; import javax. olap. query. querytransaction.*;
import javax. olap. query. sorting.*;
public abstract interface OperatorInput {


// class scalar attributes
// class references
public Ordinate getOrdinate() throws OLAPException;


public void setOrdinateOperator( OrdinateOperator input) throws OLAPException;
public OrdinateOperator getOrdinateOperator() throws OLAPException;
// class operations
}


