package javax. olap. query. querycoremodel;
import java. util. List; import java. util. Collection;
import javax. olap. OLAPException; import org. omg. cwm. objectmodel. core.*;
import javax. jmi. reflect.*; import javax. olap. query. calculatedmembers.*;
import javax. olap. query. dimensionfilters.*; import javax. olap. query. derivedattribute.*;
import javax. olap. query. enumerations.*; import javax. olap. cursor.*;
import javax. olap. metadata.*; import javax. olap. query. edgefilters.*;
import javax. olap. query. querytransaction.*; import javax. olap. query. sorting.*;


public abstract interface Ordinate extends QueryObject {
// class scalar attributes
// class references
public void setCalculatedMember( Collection input) throws
OLAPException;
public Collection getCalculatedMember() throws OLAPException;
public void removeCalculatedMember( CalculatedMember input) throws OLAPException;


public void setOperatorInputs( Collection input) throws OLAPException;
public Collection getOperatorInputs() throws OLAPException;
public void removeOperatorInputs( OperatorInput input) throws OLAPException;
// class operations
public CalculatedMember createCalculatedMember() throws
OLAPException; public CalculatedMember createCalculatedMemberBefore( CalculatedMember
member) throws OLAPException; public CalculatedMember createCalculatedMemberAfter( CalculatedMember
member) throws OLAPException; public OperatorInput createOperatorInput( OperatorInputType type)
throws OLAPException;
}

