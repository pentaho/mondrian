package javax. olap. query. querycoremodel;
import java. util. List;
import java. util. Collection;
import javax. olap. OLAPException;
import org. omg. cwm. objectmodel. core.*;
import javax. jmi. reflect.*;
import javax. olap. query. calculatedmembers.*;
import javax. olap. query. calculatedmembers.OperatorInput;
import javax. olap. query. dimensionfilters.*;
import javax. olap. query. derivedattribute.*;
import javax. olap. query. enumerations.*;
import javax. olap. cursor.*;
import javax. olap. metadata.*;
import javax. olap. query. edgefilters.*;
import javax. olap. query. querytransaction.*;
import javax. olap. query. sorting.*;


public interface AttributeReference extends OperatorInput, SelectedObject, DataBasedMemberFilterInput, DerivedAttributeComponent
{

// class scalar attributes
// class references
public void setAttribute( Attribute input) throws OLAPException;


public Attribute getAttribute() throws OLAPException;
// class operations
}


