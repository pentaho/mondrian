package javax.olap.metadata;
import org.omg.cwm.objectmodel.core.Classifier;

import javax.olap.OLAPException;
import java.util.Collection;
import java.util.List;
public interface MemberList extends Classifier
{
// class scalar attributes
// class references
	public void setMember(Collection input) throws OLAPException;
	public List getMember() throws OLAPException;
	public void addMember(Member input) throws OLAPException;
	public void removeMember(Member input) throws OLAPException;
	public void addMemberBefore(Member before, Member input) throws OLAPException;
	public void addMemberAfter(Member before, Member input) throws OLAPException;
	public void moveMemberBefore(Member before, Member input) throws OLAPException;
	public void moveMemberAfter(Member before, Member input) throws OLAPException;
// class operations
}
