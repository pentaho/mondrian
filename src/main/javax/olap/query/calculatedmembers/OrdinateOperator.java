package javax. olap. query. calculatedmembers;
import java. util. List; import java. util. Collection;
import javax. olap. OLAPException; import org. omg. cwm. objectmodel. core.*;
import javax. olap. query. querycoremodel.*; import javax. olap. query. enumerations.*;
import javax. olap. metadata.*; import javax. olap. query. dimensionfilters.*;
import javax. olap. query. edgefilters.*; import javax. olap. query. querytransaction.*;
import javax. olap. query. sorting.*;
public interface OrdinateOperator extends NamedObject {


// class scalar attributes
public OperatorType getOperatorType() throws OLAPException;
public void setOperatorType( OperatorType input) throws OLAPException;
public Operator getOperator() throws OLAPException;
public void setOperator( Operator input) throws OLAPException;


// class references
public void setInput( Collection input) throws OLAPException;
public Collection getInput() throws OLAPException;
public void addInput( OperatorInput input) throws OLAPException;
public void removeInput( OperatorInput input) throws OLAPException;
public void setNullHandling( NullHandling input) throws OLAPException;
public NullHandling getNullHandling() throws OLAPException;
// class operations
public NullHandling createNullHandling() throws OLAPException;
}

