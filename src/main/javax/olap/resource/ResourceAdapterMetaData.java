package javax. olap. resource;
import java. util. List;
import java. util. Collection;
import javax. olap. OLAPException;
import javax. olap. metadata.*;
import javax. olap. query. querycoremodel.*;
import javax. jmi. reflect.*;


public interface ResourceAdapterMetaData extends RefObject {
// class scalar attributes
// class references // class operations
public String getAdapterName() throws OLAPException;
public String getAdapterShortDescription() throws OLAPException;
public String getAdapterVendorName() throws OLAPException;
public String getAdapterVersion() throws OLAPException;
public String getSpecificationTitle() throws OLAPException;
public String getSpecificationVersion() throws OLAPException;
public String getSpecificationVendor() throws OLAPException;
public String getComplianceLevel() throws OLAPException;
public String getSpecVersion() throws OLAPException;
}


