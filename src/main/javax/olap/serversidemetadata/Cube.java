package javax. olap. serversidemetadata;
import java. util. List; import java. util. Collection;
import javax. olap. OLAPException; import org. omg. cwm. foundation. expressions.*;
import org. omg. cwm. objectmodel. core.*; import javax. olap. metadata.*;
import org. omg. cwm. analysis. transformation.*;
public interface Cube extends javax. olap. metadata. Cube {


// class scalar attributes
// class references
public void setCubeRegion( Collection input) throws OLAPException;


public Collection getCubeRegion() throws OLAPException;
public void removeCubeRegion( CubeRegion input) throws OLAPException;
// class operations
}
