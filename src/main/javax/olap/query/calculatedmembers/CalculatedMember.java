package javax. olap. query. calculatedmembers;
import java. util. List; import java. util. Collection;
import javax. olap. OLAPException; import org. omg. cwm. objectmodel. core.*;
import javax. olap. query. querycoremodel.*; import javax. olap. query. enumerations.*;
import javax. olap. metadata.*; import javax. olap. query. dimensionfilters.*;
import javax. olap. query. edgefilters.*; import javax. olap. query. querytransaction.*;
import javax. olap. query. sorting.*;
public interface CalculatedMember extends Member {


// class scalar attributes
public Boolean getIsHidden() throws OLAPException;
public void setIsHidden( Boolean input) throws OLAPException;
// class references
public void setMasterPrecedence( Collection input) throws
OLAPException;
public Collection getMasterPrecedence() throws OLAPException;
public void addMasterPrecedence( CalculationRelationship input) throws OLAPException;


public void removeMasterPrecedence( CalculationRelationship input) throws OLAPException;
public void setSlavePrecedence( Collection input) throws OLAPException;
public Collection getSlavePrecedence() throws OLAPException;
public void addSlavePrecedence( CalculationRelationship input) throws OLAPException;


public void removeSlavePrecedence( CalculationRelationship input) throws OLAPException;
public void setAttributeValue( Collection input) throws OLAPException;
public Collection getAttributeValue() throws OLAPException;
public void removeAttributeValue( AttributeValue input) throws OLAPException;


// class operations
public AttributeValue createAttributeValue() throws OLAPException;
public OrdinateOperator createOrdinateOperator() throws OLAPException;
}


