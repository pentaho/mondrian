package javax. olap. cursor; 
import java. util. List; import java. util. Collection; 
import javax. olap. OLAPException; import java. io.*; 
import javax. olap. query. querycoremodel.*; import java. math.*; 
import java. util.*; import javax. olap. query. querytransaction.*; 
public abstract interface RowDataNavigation { 
// class scalar attributes 
// class references // class operations 
public boolean next() throws OLAPException; public void close() throws OLAPException; 
public void beforeFirst() throws OLAPException; public void afterLast() throws OLAPException; 
public boolean first() throws OLAPException; public int getType() throws OLAPException; 
public boolean isAfterLast() throws OLAPException; public boolean isBeforeFirst() throws OLAPException; 
public boolean isFirst() throws OLAPException; public boolean isLast() throws OLAPException; 
public boolean last() throws OLAPException; public boolean previous() throws OLAPException; 
public boolean relative( int arg0) throws OLAPException; public void setFetchDirection( int arg0) throws OLAPException; 
public void setFetchSize( int arg0) throws OLAPException; public void clearWarnings() throws OLAPException; 
public void getWarnings() throws OLAPException; public int getFetchDirection() throws OLAPException; 
public int getFetchSize() throws OLAPException; public long getExtent() throws OLAPException; 
public void setPosition( long position) throws OLAPException; public long getPosition() throws OLAPException; 
} 

