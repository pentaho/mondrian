package mondrian.queryplan;

import java.io.PrintWriter;
import java.util.*;

import mondrian.olap.*;
import mondrian.rolap.*;
import org.olap4j.AllocationPolicy;
import org.olap4j.Scenario;

import mondrian.calc.TupleList;

public class QPResult implements Result {

    public static class QPAxis implements Axis {
        private PositionList poslist = new PositionList();

        @Override
        public List<Position> getPositions() {
            return poslist;
        }

        public PositionList getPositionList() {
            return poslist;
        }

        public final List<Column> relationalColumns = new ArrayList<Column>();
    }

    public static interface Column {
        public String alias();
    }
    public static class MemberColumn implements Column {
        public final int axisOrdinal;
        public final int hierarchyOrdinal;
        public final int levelOrdinal;
        public final String memberFqName;

        public MemberColumn(int axisOrdinal, int hierarchyOrdinal, int levelOrdinal, String memberFqName) {
            this.axisOrdinal = axisOrdinal;
            this.hierarchyOrdinal = hierarchyOrdinal;
            this.levelOrdinal = levelOrdinal;
            this.memberFqName = memberFqName;
        }


        public String alias() {
            return String.format("a%d_h%d_l%d", axisOrdinal, hierarchyOrdinal, levelOrdinal);
        }
    }
    public static class MeasureColumn implements Column {
        public final int measureOrdinal;
        public final RolapMeasure measureMember;

        public MeasureColumn(int measureOrdinal, RolapMeasure measureMember) {
            this.measureOrdinal = measureOrdinal;
            this.measureMember = measureMember;
        }

        public String alias() {
            return String.format("m%d", measureOrdinal);
        }
    }

    /**
     * List of positions.
     */
    public static class PositionList extends AbstractList<Position> implements List<Position> {
        private final List<PositionImpl> list = new ArrayList<PositionImpl>();

        private PositionList() {
            add(new PositionImpl());
        }

        public boolean isEmpty() {
            // may be considerably cheaper than computing size
            return false;
        }

        public int size() {
            return list.size();
        }

        public boolean add(PositionImpl position) {
            return list.add(position);
        }

        public void add(int index, PositionImpl position) {
            list.add(index, position);
        }

        public PositionImpl get(int index) {
            return list.get(index);
        }

        public void addPositionWithMember(PositionImpl pos, Member member) {
            if (pos == null) {
                pos = new PositionImpl();
                add(pos);
            }
            pos.add(member);
        }
    }

    /**
     * Implementation of {@link Position} that reads from a given location in
     * a {@link TupleList}.
     */
    public static class PositionImpl
        extends AbstractList<Member>
        implements Position
    {
        private final List<Member> tupleList = new ArrayList<Member>();

        public int size() {
            return tupleList.size();
        }

        public void add(int index, Member member) {
            tupleList.add(index, member);
        }

        public Member get(int index) {
            return tupleList.get(index);
        }
    }

    public QPResult(int size) {
        axes = new ArrayList<QPAxis>(size);
    }

    @Override
    public Query getQuery() {
        // TODO Auto-generated method stub
        return null;
    }

    public QPAxis slicerAxis = new QPAxis();

    public List<QPAxis> axes;

    @Override
    public Axis[] getAxes() {
        return axes.toArray(new Axis[axes.size()]);
    }

    @Override
    public QPAxis getSlicerAxis() {
        // TODO Auto-generated method stub
        return slicerAxis;
    }

    public Map<CellKey, QPCell> cells = new HashMap<CellKey, QPCell>();

    public static class QPCell implements Cell {

        public Object value;
        public String formattedValue;
        @Override
        public List<Integer> getCoordinateList() {
            // TODO Auto-generated method stub
            return null;
        }

        public String toString() { return formattedValue; }

        @Override
        public Object getValue() {
            // TODO Auto-generated method stub
            return value;
        }

        @Override
        public String getCachedFormatString() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public String getFormattedValue() {
            return formattedValue;
        }

        @Override
        public boolean isNull() {
            // TODO Auto-generated method stub
            return false;
        }

        @Override
        public boolean isError() {
            // TODO Auto-generated method stub
            return false;
        }

        @Override
        public String getDrillThroughSQL( boolean extendedContext ) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public boolean canDrillThrough() {
            // TODO Auto-generated method stub
            return false;
        }

        @Override
        public int getDrillThroughCount() {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public Object getPropertyValue( String propertyName ) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Member getContextMember( Hierarchy hierarchy ) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public void setValue( Scenario scenario, Object newValue, AllocationPolicy allocationPolicy,
                              Object... allocationArgs ) {
            // TODO Auto-generated method stub

        }

    }

    @Override
    public Cell getCell( int[] pos ) {

        // TODO Auto-generated method stub
        return cells.get( CellKey.Generator.newCellKey(pos) );
    }

    // implement Result
    public void print(PrintWriter pw) {
        for (int i = -1; i < axes.size(); i++) {
            pw.println("Axis #" + (i + 1) + ":");
            printAxis(pw, i < 0 ? slicerAxis : axes.get(i));
        }
        // Usually there are 3 axes: {slicer, columns, rows}. Position is a
        // {column, row} pair. We call printRows with axis=2. When it recurses
        // to axis=-1, it prints.
        int[] pos = new int[axes.size()];
        printRows(pw, axes.size() - 1, pos);
    }

    private void printRows(PrintWriter pw, int axis, int[] pos) {
        if (axis < 0) {
            printCell(pw, pos);
        } else {
            Axis _axis = axes.get(axis);
            List<Position> positions = _axis.getPositions();
            for (int i = 0; i < positions.size(); i++) {
                pos[axis] = i;
                if (axis == 0) {
                    int row =
                        axis + 1 < pos.length
                            ? pos[axis + 1]
                            : 0;
                    pw.print("Row #" + row + ": ");
                }
                printRows(pw, axis - 1, pos);
                if (axis == 0) {
                    pw.println();
                }
            }
        }
    }

    private void printAxis(PrintWriter pw, Axis axis) {
        List<Position> positions = axis.getPositions();
        for (Position position : positions) {
            boolean firstTime = true;
            pw.print("{");
            for (Member member : position) {
                if (! firstTime) {
                    pw.print(", ");
                }
                pw.print(member.getUniqueName());
                firstTime = false;
            }
            pw.println("}");
        }
    }

    private void printCell(PrintWriter pw, int[] pos) {
        Cell cell = getCell(pos);
        if (cell != null)
            pw.print(cell.getFormattedValue());
    }


    @Override
    public void close() {
        // TODO Auto-generated method stub
    }
}
