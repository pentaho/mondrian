package javax.olap.query.querycoremodel;
import javax.olap.OLAPException;
import javax.olap.query.calculatedmembers.CalculatedMember;
import javax.olap.query.calculatedmembers.OperatorInput;
import javax.olap.query.enumerations.OperatorInputType;
import java.util.Collection;


public abstract interface Ordinate extends QueryObject {
// class scalar attributes
// class references
	public void setCalculatedMember( Collection input) throws OLAPException;
	public Collection getCalculatedMember() throws OLAPException;
	public void removeCalculatedMember( CalculatedMember input) throws OLAPException;
	public void setOperatorInputs( Collection input) throws OLAPException;
	public Collection getOperatorInputs() throws OLAPException;
	public void removeOperatorInputs( OperatorInput input) throws OLAPException;
// class operations
	public CalculatedMember createCalculatedMember() throws OLAPException;
	public CalculatedMember createCalculatedMemberBefore( CalculatedMember member) throws OLAPException;
	public CalculatedMember createCalculatedMemberAfter( CalculatedMember member) throws OLAPException;
	public OperatorInput createOperatorInput( OperatorInputType type) throws OLAPException;
}

