package javax.olap.resource;
import java. util. List;
import java. util. Collection;
import javax. olap. OLAPException;
import javax. olap. metadata.*;
import javax. olap. query. querycoremodel.*;
import javax. jmi. reflect.*;
public interface ConnectionSpec {
	// jhyde added
	void setName(String name);
	// jhyde added
	void setPassword(String password);
	// jhyde added
	String getPassword();
	// jhyde added
	String getName();
// class scalar attributes
// class references // class operations
}


