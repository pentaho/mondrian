package javax. olap. query. dimensionfilters;
import java. util. List; import java. util. Collection;
import javax. olap. OLAPException; import org. omg. cwm. objectmodel. core.*;
import javax. olap. metadata.*; import javax. olap. query. querycoremodel.*;
import javax. olap. query. enumerations.*; import javax. olap. query. calculatedmembers.*;
import javax. olap. query. edgefilters.*; import javax. olap. query. querytransaction.*;
import javax. olap. query. sorting.*;
public interface RankingMemberFilter extends DataBasedMemberFilter {
// class scalar attributes
public Integer getTop() throws OLAPException;
public void setTop( Integer input) throws OLAPException;
public Boolean getTopPercent() throws OLAPException;
public void setTopPercent( Boolean input) throws OLAPException;
public Integer getBottom() throws OLAPException;
public void setBottom( Integer input) throws OLAPException;
public Boolean getBottomPercent() throws OLAPException;
public void setBottomPercent( Boolean input) throws OLAPException;
public RankingType getType() throws OLAPException;
public void setType( RankingType input) throws OLAPException;


// class references // class operations
}


