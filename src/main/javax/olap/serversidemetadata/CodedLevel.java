package javax. olap. serversidemetadata;
import java. util. List; import java. util. Collection;
import javax. olap. OLAPException; import org. omg. cwm. foundation. expressions.*;
import org. omg. cwm. objectmodel. core.*; import javax. olap. metadata.*;
import org. omg. cwm. analysis. transformation.*;
public interface CodedLevel extends Level {


// class scalar attributes
public ExpressionNode getEncoding() throws OLAPException;
public void setEncoding( ExpressionNode input) throws OLAPException;
// class references // class operations
}

