package javax. olap. query. derivedattribute;
import java. util. List;
import java. util. Collection;
import javax. olap. OLAPException;
import javax. olap. query. querycoremodel.*;
import javax. olap. query. calculatedmembers.*;
import javax. olap. query. dimensionfilters.*;
import javax. olap. query. enumerations.*;
import javax. olap. query. edgefilters.*;
import javax. olap. query. querytransaction.*;
import javax. olap. query. sorting.*;


public interface DerivedAttribute extends NamedObject, DerivedAttributeComponent, SelectedObject, OperatorInput, DataBasedMemberFilterInput {


// class scalar attributes
public DimensionView getDimensionView() throws OLAPException;
public void setDimensionView( DimensionView input) throws OLAPException;
public DAOperator getOperator() throws OLAPException;
public void setOperator( DAOperator input) throws OLAPException;


// class references
public void setComponent( Collection input) throws OLAPException;
public List getComponent() throws OLAPException;
public void removeComponent( DerivedAttributeComponent input) throws OLAPException;


public void moveComponentBefore( DerivedAttributeComponent before, DerivedAttributeComponent input) throws OLAPException;
public void moveComponentAfter( DerivedAttributeComponent before, DerivedAttributeComponent input) throws OLAPException;
// class operations
public DerivedAttributeComponent
createComponent( DerivedAttributeComponentType componentType) throws OLAPException;
public DerivedAttributeComponent createComponentBefore( DerivedAttributeComponentType componentType,
DerivedAttributeComponent member) throws OLAPException; public DerivedAttributeComponent
createComponent( DerivedAttributeComponentType componentType, DerivedAttributeComponent member) throws OLAPException;
}

