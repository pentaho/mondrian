package mondrian.queryplan;

import java.io.PrintWriter;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mondrian.rolap.CellKey;
import org.olap4j.AllocationPolicy;
import org.olap4j.Scenario;

import mondrian.calc.TupleList;
import mondrian.olap.Axis;
import mondrian.olap.Cell;
import mondrian.olap.Hierarchy;
import mondrian.olap.Member;
import mondrian.olap.Position;
import mondrian.olap.Query;
import mondrian.olap.Result;

public class QPResult implements Result {

    public static class QPAxis implements Axis {

        public PositionList poslist = null;
        public QPAxis() {
            this.poslist = new PositionList();
            this.poslist.list.add( new PositionImpl() );
        }

        @Override
        public List<Position> getPositions() {
            return poslist;
        }
    }

    /**
     * List of positions.
     */
    public static class PositionList extends AbstractList<Position> {
        public final List<PositionImpl> list = new ArrayList<PositionImpl>();

        PositionList() {
        }

        public boolean isEmpty() {
            // may be considerably cheaper than computing size
            return list.isEmpty();
        }

        public int size() {
            return list.size();
        }

        public Position get(int index) {
            return list.get(index);
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
        public final List<Member> tupleList = new ArrayList<Member>();

        public Member get(int index) {
            return tupleList.get(index);
        }

        public int size() {
            return tupleList.size();
        }
    }

    public QPResult() {

    }

    @Override
    public Query getQuery() {
        // TODO Auto-generated method stub
        return null;
    }

    public QPAxis slicerAxis = new QPAxis();

    public List<Axis> axes = new ArrayList<Axis>();

    @Override
    public Axis[] getAxes() {
        return axes.toArray(new Axis[0]);
    }

    @Override
    public Axis getSlicerAxis() {
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
