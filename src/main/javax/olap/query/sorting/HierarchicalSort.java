package javax. olap. query. sorting;
import java. util. List;
import java. util. Collection; import javax. olap. OLAPException;
import org. omg. cwm. objectmodel. core.*; import javax. olap. metadata.*;
import javax. olap. query. querycoremodel.*; import javax. olap. query. enumerations.*;
import javax. olap. query. calculatedmembers.*; import javax. olap. query. dimensionfilters.*;
import javax. olap. query. edgefilters.*; import javax. olap. query. querytransaction.*;


public interface HierarchicalSort extends DimensionSort {
// class scalar attributes
public HierarchicalSortType getHierarchicalSortType() throws
OLAPException;
public void setHierarchicalSortType( HierarchicalSortType input) throws OLAPException;


// class references
public void setHierarchy( Hierarchy input) throws OLAPException;
public Hierarchy getHierarchy() throws OLAPException;
// class operations
}


