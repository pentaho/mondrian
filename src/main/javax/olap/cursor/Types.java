package javax. olap. cursor;
import java. util. List; import java. util. Collection;
import javax. olap. OLAPException; import java. io.*;
import javax. olap. query. querycoremodel.*; import java. math.*;
import java. util.*; import javax. olap. query. querytransaction.*;


public interface Types {
// class scalar attributes
public int getBIT() throws OLAPException;
public void setBIT( int input) throws OLAPException;
public int getTINYINT() throws OLAPException;
public void setTINYINT( int input) throws OLAPException;
public int getSMALLINT() throws OLAPException;
public void setSMALLINT( int input) throws OLAPException;
public int getINTEGER() throws OLAPException;
public void setINTEGER( int input) throws OLAPException;
public int getBIGINT() throws OLAPException;
public void setBIGINT( int input) throws OLAPException;
public int getFLOAT() throws OLAPException;
public void setFLOAT( int input) throws OLAPException;
public int getREAL() throws OLAPException;
public void setREAL( int input) throws OLAPException;
public int getDOUBLE() throws OLAPException;
public void setDOUBLE( int input) throws OLAPException;
public int getNUMERIC() throws OLAPException;
public void setNUMERIC( int input) throws OLAPException;
public int getDECIMAL() throws OLAPException;
public void setDECIMAL( int input) throws OLAPException;
public int getCHAR() throws OLAPException;
public void setCHAR( int input) throws OLAPException;
public int getVARCHAR() throws OLAPException;
public void setVARCHAR( int input) throws OLAPException;
public int getLONGVARCHAR() throws OLAPException;
public void setLONGVARCHAR( int input) throws OLAPException;
public int getDATE() throws OLAPException;
public void setDATE( int input) throws OLAPException;
public int getTIME() throws OLAPException;
public void setTIME( int input) throws OLAPException;
public int getTIMESTAMP() throws OLAPException;
public void setTIMESTAMP( int input) throws OLAPException;
public int getBINARY() throws OLAPException;
public void setBINARY( int input) throws OLAPException;
public int getVARBINARY() throws OLAPException;
public void setVARBINARY( int input) throws OLAPException;
public int getLONGVARBINARY() throws OLAPException;
public void setLONGVARBINARY( int input) throws OLAPException;
public int getNULL() throws OLAPException;
public void setNULL( int input) throws OLAPException;
public int getOTHER() throws OLAPException;
public void setOTHER( int input) throws OLAPException;
public int getJAVA_OBJECT() throws OLAPException;
public void setJAVA_OBJECT( int input) throws OLAPException;
public int getDISTINCT() throws OLAPException;
public void setDISTINCT( int input) throws OLAPException;
public int getBLOB() throws OLAPException;
public void setBLOB( int input) throws OLAPException;
public int getCLOB() throws OLAPException;
public void setCLOB( int input) throws OLAPException;


// class references // class operations
public void Types() throws OLAPException; }
