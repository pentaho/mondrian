package mondrian.rolap;

import java.util.List;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * <p>
 * Base helper class for the sql tuple readers
 *  {@link mondrian.rolap.HighCardSqlTupleReader}
 *  {@link mondrian.rolap.SqlTupleReader}
 * Keeps track of target levels and constraints for adding to sql query
 * The real work is done in the extending classes.
 *  {@link Target}
 *  {@link mondrian.rolap.SqlTupleReader.Target}
 * </p>
 *
 * @author Kurtis Walker
 * @since July 23, 2009
 * @version $Id$
 */
public abstract class TargetBase {
    final List<RolapMember> srcMembers;
    final RolapLevel level;
    private RolapMember currMember;
    private List<RolapMember> list;
    final Object cacheLock;
    final TupleReader.MemberBuilder memberBuilder;

    public TargetBase(
            List<RolapMember> srcMembers, RolapLevel level,
            TupleReader.MemberBuilder memberBuilder)
    {
        this.srcMembers = srcMembers;
        this.level = level;
        cacheLock = memberBuilder.getMemberCacheLock();
        this.memberBuilder = memberBuilder;
    }

    public void setList(final List<RolapMember> list) {
        this.list = list;
    }

    public List<RolapMember> getSrcMembers() {
        return srcMembers;
    }

    public RolapLevel getLevel() {
        return level;
    }

    public RolapMember getCurrMember() {
        return this.currMember;
    }

    public void removeCurrMember() {
        this.currMember = null;
    }

    public void setCurrMember(final RolapMember m) {
        this.currMember = m;
    }

    public List<RolapMember> getList() {
        return list;
    }

    public String toString() {
        return level.getUniqueName();
    }

    public int addRow(ResultSet resultSet, int column) throws SQLException {
        synchronized (cacheLock) {
            return internalAddRow(resultSet, column);
        }
    }

    public abstract void open();

    public abstract List<RolapMember> close();

    abstract int internalAddRow(ResultSet resultSet, int column)
            throws SQLException;

    public void add(final RolapMember member) {
        this.getList().add(member);
    }
}
// End TargetBase.java