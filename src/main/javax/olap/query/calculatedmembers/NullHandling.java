package javax. olap. query. calculatedmembers;
import java. util. List; import java. util. Collection;
import javax. olap. OLAPException; import org. omg. cwm. objectmodel. core.*;
import javax. olap. query. querycoremodel.*; import javax. olap. query. enumerations.*;
import javax. olap. metadata.*; import javax. olap. query. dimensionfilters.*;
import javax. olap. query. edgefilters.*; import javax. olap. query. querytransaction.*;
import javax. olap. query. sorting.*;
public interface NullHandling extends NamedObject {


// class scalar attributes
public Boolean getNullAsZero() throws OLAPException;
public void setNullAsZero( Boolean input) throws OLAPException;
public Boolean getNullAsMissing() throws OLAPException;
public void setNullAsMissing( Boolean input) throws OLAPException;


// class references // class operations
}


