package javax. olap. query. calculatedmembers;
import java. util. List; import java. util. Collection;
import javax. olap. OLAPException; import org. omg. cwm. objectmodel. core.*;
import javax. olap. query. querycoremodel.*; import javax. olap. query. enumerations.*;
import javax. olap. metadata.*; import javax. olap. query. dimensionfilters.*;
import javax. olap. query. edgefilters.*; import javax. olap. query. querytransaction.*;

import javax. olap. query. sorting.*;
public interface AttributeValue extends NamedObject {


// class scalar attributes
public Object getValue() throws OLAPException;
public void setValue( Object input) throws OLAPException;
// class references
public CalculatedMember getCalculatedMember() throws OLAPException;
public void setAttribute( Attribute input) throws OLAPException;
public Attribute getAttribute() throws OLAPException;
// class operations
}


