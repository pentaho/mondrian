package javax.olap.metadata;

public interface MemberObjectFactories {
	public Member createMember(Dimension owner);
	public javax.olap.query.CurrentMember createCurrentMember(Dimension owner);
	public MemberList createMemberList(Dimension owner);
	public MemberValue createMemberValue(Member owner);
	public MemberSet createMemberSet(Dimension owner);
}
