package javax. olap. query. calculatedmembers;
import java. util. List; import java. util. Collection;
import javax. olap. OLAPException; import org. omg. cwm. objectmodel. core.*;
import javax. olap. query. querycoremodel.*; import javax. olap. query. enumerations.*;
import javax. olap. metadata.*; import javax. olap. query. dimensionfilters.*;
import javax. olap. query. edgefilters.*; import javax. olap. query. querytransaction.*;
import javax. olap. query. sorting.*;

public interface CalculationRelationship extends NamedObject {

// class scalar attributes
// class references
public void setSuperiorPrecedence( CalculatedMember input) throws OLAPException;
public CalculatedMember getSuperiorPrecedence() throws OLAPException;
public void setInferiorPrecedence( CalculatedMember input) throws OLAPException;


public CalculatedMember getInferiorPrecedence() throws OLAPException;
// class operations
}
