package javax. olap. serversidemetadata;
import java. util. List; import java. util. Collection;
import javax. olap. OLAPException; import org. omg. cwm. foundation. expressions.*;
import org. omg. cwm. objectmodel. core.*; import javax. olap. metadata.*;
import org. omg. cwm. analysis. transformation.*;
public interface ContentMap extends TransformationMap {


// class scalar attributes
// class references
public void setCubeDeployment( CubeDeployment input) throws OLAPException;
public CubeDeployment getCubeDeployment() throws OLAPException;
// class operations
}


