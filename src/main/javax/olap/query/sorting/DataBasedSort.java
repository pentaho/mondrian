package javax. olap. query. sorting;
import java. util. List; import java. util. Collection;
import javax. olap. OLAPException; import org. omg. cwm. objectmodel. core.*;
import javax. olap. metadata.*; import javax. olap. query. querycoremodel.*;
import javax. olap. query. enumerations.*; import javax. olap. query. calculatedmembers.*;
import javax. olap. query. dimensionfilters.*; import javax. olap. query. edgefilters.*;
import javax. olap. query. querytransaction.*;
public interface DataBasedSort extends DimensionSort {


// class scalar attributes
// class references
public void setQualifiedMember( QualifiedMemberReference input) throws
OLAPException;
public QualifiedMemberReference getQualifiedMember() throws OLAPException;


// class operations
public QualifiedMemberReference createQualifiedMember() throws
OLAPException;
}


