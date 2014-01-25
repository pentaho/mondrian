package mondrian.queryplan;

import mondrian.mdx.*;
import mondrian.olap.*;
import mondrian.rolap.*;
import mondrian.util.Format;
import org.apache.tajo.algebra.*;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.conf.TajoConf;

import java.sql.*;
import java.util.*;

public class QueryPlan {
    public static final QueryPlan DUMMY = new QueryPlan();
    private boolean valid;
    private boolean foundMeasure;
    private final Query mdxQuery;
    private String planContext;
    private QPResult qpResult;

    private QueryPlan() {
        valid = false;
        foundMeasure = false;
        mdxQuery = null;
        planContext = null;
    }

    public QueryPlan(Query mdxQuery) {
        this.mdxQuery = mdxQuery;
        this.planContext = null;

        // not yet validated
        this.valid = false;
    }


    private static class PlanParts {
        private PlanParts() {
            this.sortSpecs = new ArrayList<Sort.SortSpec>();
            this.relationSet = new LinkedHashSet<Relation>();
            this.targetSet = new LinkedHashSet<NamedExpr>();
            this.andExprs = new ArrayList<Expr>();
            this.groupElements = new ArrayList<Aggregation.GroupElement>();

            // Create the base Expr of the contextPlan
            this.rootProjection = new Projection();

            // Create the Aggregation since it is always (?) needed
            this.aggregation = new Aggregation();

            // Don't forget to set the quals at the end
            this.filter = new Selection(null);

            this.sort = new Sort(null);

            aggregation.setChild(filter);

            // Don't forget to set the SortSpec[] at the end
            sort.setChild(aggregation);
        }

        final Projection rootProjection;

        final Aggregation aggregation;

        final Selection filter;

        final Sort sort;

        final List<Sort.SortSpec> sortSpecs;
        final Set<Relation> relationSet;
        final Set<NamedExpr> targetSet;
        final List<Expr> andExprs;
        final List<Aggregation.GroupElement> groupElements;

        private Expr buildQual(List<Expr> andExprs) {
            Iterator<Expr> iter = andExprs.listIterator();
            if (iter.hasNext()) {
                Expr expr = iter.next();
                while (iter.hasNext()) {
                    Expr next = iter.next();
                    expr = new BinaryOperator(OpType.And, expr, next);
                }
                return expr;
            } else {
                return null;
            }
        }

        private String compileParts() {
            // Finalize the contextPlan
            rootProjection.setNamedExprs(targetSet.toArray(new NamedExpr[targetSet.size()]));
            RelationList relList = new RelationList(relationSet.toArray(new Relation[relationSet.size()]));
            if (groupElements.size() > 0) {
                aggregation.setGroups(groupElements.toArray(new Aggregation.GroupElement[groupElements.size()]));
                filter.setQual(buildQual(andExprs));
                rootProjection.setChild(aggregation);
                filter.setChild(relList);
            } else {
                rootProjection.setChild(relList);
            }
            if (sortSpecs.size() > 0) {
                sort.setSortSpecs(sortSpecs.toArray(new Sort.SortSpec[sortSpecs.size()]));
                rootProjection.setChild(sort);
            }

            return JsonHelper.toJson(rootProjection);
        }
    }

    public void build() {
        // Gather some useful objects from the query for inspection.
        QueryAxis[] axes = mdxQuery.getAxes();
        RolapCube cube = (RolapCube)mdxQuery.getCube();
        RolapSchema.PhysSchema physicalSchema = cube.getSchema().getPhysicalSchema();
        List<RolapStar> stars = cube.getStars();
        if (stars.size() > 1) {
            System.err.println("Multiple stars per cube not currently implemented");
        }
        RolapStar star = stars.get(0);

        // Create containers for Expr objects that must be converted to array form later
        PlanParts planParts = new PlanParts();

        // Add the relation for the fact table
        RolapSchema.PhysRelation factRelation = star.getFactTable().getRelation();
        planParts.relationSet.add(new Relation(factRelation.getAlias()));

        // Create the QPResult object for storing static metadata
        qpResult = new QPResult(axes.length);
        for (QueryAxis axis : axes) {
            QPResult.QPAxis qpAxis = new QPResult.QPAxis();
            qpResult.axes.add(qpAxis);
        }

        for (int axisOrdinal = axes.length - 1; axisOrdinal >= -1; axisOrdinal--) {
            QPResult.QPAxis qpAxis;

            QueryAxis axis;
            if (axisOrdinal == -1) {
                axis = mdxQuery.getSlicerAxis();
                qpAxis = qpResult.slicerAxis;
            } else {
                axis = axes[axisOrdinal];
                qpAxis = qpResult.axes.get(axisOrdinal);
            }
            if (axis == null) continue;
            QPResult.PositionList positionList = qpAxis.getPositionList();


            Exp axisSet = axis.getSet();
            if (axisSet instanceof ResolvedFunCall) {
                ResolvedFunCall axisSetResolvedFunCall = (ResolvedFunCall)axisSet;
                if (axisSetResolvedFunCall.getArgCount() != 1) {
                    System.err.println("Skipping QueryPlan: Too many args in axisSetResolvedFunCall");
                    return;
                }
                for (Exp arg : axisSetResolvedFunCall.getArgs()) {
                    if (arg instanceof MemberExpr) {
                        Member memberExprMember = ((MemberExpr) arg).getMember();
                        // I think we put in position 0 because in this case, there can only be a single position.
                        positionList.get(0).add(memberExprMember);

                        if (memberExprMember instanceof RolapBaseCubeMeasure) {
                            // Can't have Measures dim on multiple axes, so I think a 0 is right here.
                            planMeasureMember(planParts.targetSet,(RolapBaseCubeMeasure)memberExprMember, 0);

                        } else if (memberExprMember instanceof RolapMemberBase) {
                            RolapMemberBase rolapMemberBase = (RolapMemberBase) memberExprMember;
                            RolapAttribute attr = (rolapMemberBase.getLevel()).getAttribute();
                            RolapSchema.PhysColumn physColumn = attr.getNameExp();
                            String relationAlias = physColumn.relation.getAlias();

                            // Ensure we have this relation and its qual
                            if (planParts.relationSet.add(new Relation(relationAlias))) {
                                RolapSchema.PhysLink link = null;
                                Set<RolapSchema.PhysLink> physLinks = physicalSchema.getLinkSet();
                                for (RolapSchema.PhysLink physLink : physLinks) {
                                    if (physLink.getTo().equals(physColumn.relation) &&
                                        physLink.getFrom().equals(factRelation)) {
                                        link = physLink;
                                        break;
                                    }
                                }
                                if (link == null) {
                                    throw new NullPointerException("No relation link found");
                                }
                                RolapSchema.PhysColumn foreignKey = getOnlyAllowedElement(link.getColumnList());
                                RolapSchema.PhysColumn sourceKey = getOnlyAllowedElement(link.getSourceKey().getColumnList());
                                planParts.andExprs.add(new BinaryOperator(OpType.Equals,
                                    new ColumnReferenceExpr(factRelation.getAlias(), foreignKey.name),
                                    new ColumnReferenceExpr(relationAlias, sourceKey.name)));
                            }

                            QPResult.MemberColumn relColumn = new QPResult.MemberColumn(axisOrdinal, 0, 0, rolapMemberBase.getUniqueName());
//                            qpAxis.relationalColumns.add(relColumn);

                            // Add a target column reference
                            ColumnReferenceExpr colRefExpr = new ColumnReferenceExpr(relationAlias, physColumn.name);
                            planParts.targetSet.add(new NamedExpr(colRefExpr, relColumn.alias()));

                            // Add an Equals predicate for this member
                            LiteralValue value = new LiteralValue(String.valueOf(rolapMemberBase.getKey()), LiteralValue.LiteralType.String);
                            planParts.andExprs.add(new BinaryOperator(OpType.Equals, colRefExpr, value));

                            // Add grouping for this target
                            planParts.groupElements.add(new Aggregation.GroupElement(Aggregation.GroupType.OrdinaryGroup, new ColumnReferenceExpr[]{colRefExpr}));
                        } else {
                            System.err.println("Skipping QueryPlan: No implementation for member "+memberExprMember.getClass().getName());
                            return;
                        }
                    } else if (arg instanceof HierarchyExpr) {
                        if ("Children".equals(axisSetResolvedFunCall.getFunName())) {
                            RolapCubeHierarchy rolapCubeHierarchy = (RolapCubeHierarchy) ((HierarchyExpr)arg).getHierarchy();
                            List<? extends RolapCubeLevel> levelList = rolapCubeHierarchy.getLevelList();
                            // Get the level just below All.  If there isn't one, let it return an empty set.
                            if (levelList.size() < 2) continue;
                            RolapCubeLevel childrenLevel = levelList.get(1);
                            QPResult.MemberColumn relColumn = new QPResult.MemberColumn(axisOrdinal, 0, 0, childrenLevel.getUniqueName());
                            qpAxis.relationalColumns.add(relColumn);
                            planLevelMembers(planParts, childrenLevel, physicalSchema, factRelation, relColumn);

                        } else if ("Members".equals(axisSetResolvedFunCall.getFunName())) {
                            // Not ready yet. (Have you seen the output of that!?)
                            System.err.println("Skipping QueryPlan: Hierarchy.Members not yet implemented");
                            return;
                        }
                    } else if (arg instanceof ResolvedFunCall) {
                        ResolvedFunCall argResolvedFunCall = (ResolvedFunCall)arg;
                        if (argResolvedFunCall.getArgCount() != 1) {
                            System.err.println("Skipping QueryPlan: Too many args in argResolvedFunCall");
                            return;
                        }
                        if ("Members".equals(argResolvedFunCall.getFunName())) {
                            Exp membersArg = argResolvedFunCall.getArg(0);
                            if (membersArg instanceof DimensionExpr) {
                                Dimension dim = ((DimensionExpr) membersArg).getDimension();
                                if (dim.isMeasures()) {
                                    List<RolapMember> measuresMembers = cube.getMeasuresMembers();
                                    int i = 0;
                                    QPResult.PositionImpl pos = positionList.get(0);
                                    for (Member member : measuresMembers) {
                                        RolapBaseCubeMeasure measureMember = (RolapBaseCubeMeasure)member;
                                        planMeasureMember(planParts.targetSet, measureMember, i++);
                                        positionList.addPositionWithMember(pos, measureMember);
                                        pos = null;
                                    }
                                } else {
                                    System.err.println("Skipping QueryPlan: DimensionExpr Members is not Measures Dim");
                                    return;
                                }
                            } else if (membersArg instanceof LevelExpr) {
                                RolapCubeLevel level = (RolapCubeLevel) ((LevelExpr) membersArg).getLevel();
                                // Probably need to count the levels to make this value work.
                                QPResult.MemberColumn relColumn = new QPResult.MemberColumn(axisOrdinal, 0, 0, level.getUniqueName());
                                qpAxis.relationalColumns.add(relColumn);
                                planLevelMembers(planParts, level, physicalSchema, factRelation, relColumn);
                            }
                        } else {
                            System.err.println("Skipping QueryPlan: argResolvedFunCall not handled");
                            return;
                        }
                    }
                }

            } else {
                System.err.println("Skipping QueryPlan: axisSet is not ResolvedFunCall");
                return;
            }
        }

        // Check if we saw a measure in one of the axes
        if (!foundMeasure) {
            planMeasureMember(planParts.targetSet, (RolapBaseCubeMeasure) cube.getMeasuresHierarchy().getDefaultMember(), 0);
        }

        this.planContext = planParts.compileParts();
        this.valid = true;
    }

    private void planLevelMembers(PlanParts planParts, RolapCubeLevel level, RolapSchema.PhysSchema physicalSchema, RolapSchema.PhysRelation factRelation, QPResult.Column relColumn) {
        RolapAttribute attr = level.getAttribute();
        RolapSchema.PhysColumn physColumn = attr.getNameExp();
        String relationAlias = physColumn.relation.getAlias();

        // Add a target column reference
        ColumnReferenceExpr col = new ColumnReferenceExpr(relationAlias, physColumn.name);
        planParts.targetSet.add(new NamedExpr(col, relColumn.alias()));

        // Ensure we have this relation and its qual
        if (planParts.relationSet.add(new Relation(relationAlias))) {
            RolapSchema.PhysLink link = null;
            Set<RolapSchema.PhysLink> physLinks = physicalSchema.getLinkSet();
            for (RolapSchema.PhysLink physLink : physLinks) {
                if (physLink.getTo().equals(physColumn.relation) &&
                    physLink.getFrom().equals(factRelation)) {
                    link = physLink;
                    break;
                }
            }
            if (link == null) {
                throw new NullPointerException("No relation link found");
            }
            RolapSchema.PhysColumn foreignKey = getOnlyAllowedElement(link.getColumnList());
            RolapSchema.PhysColumn sourceKey = getOnlyAllowedElement(link.getSourceKey().getColumnList());
            planParts.andExprs.add(new BinaryOperator(OpType.Equals,
                new ColumnReferenceExpr(factRelation.getAlias(), foreignKey.name),
                new ColumnReferenceExpr(relationAlias, sourceKey.name)));
            planParts.sortSpecs.add(new Sort.SortSpec(col, true, false));
        }

        // Add grouping for this target
        planParts.groupElements.add(new Aggregation.GroupElement(Aggregation.GroupType.OrdinaryGroup, new ColumnReferenceExpr[]{col}));
    }

    private <T> T getOnlyAllowedElement(List<T> list) {
        if (list.size() > 1) {
            try {
                throw new Exception("Multiple elements in this list is not currently implemented");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return list.get(0);
    }

    private void planMeasureMember(Set<NamedExpr> targetSet, RolapBaseCubeMeasure measureMember, int measureOrdinal) {
        foundMeasure = true;
        RolapSchema.PhysColumn physColumn = measureMember.getExpr();
        RolapAggregator agg = measureMember.getAggregator();
        ColumnReferenceExpr col = new ColumnReferenceExpr(physColumn.name);
        GeneralSetFunctionExpr aggFun = new GeneralSetFunctionExpr(agg.getName(),agg.isDistinct(), col);

        QPResult.MeasureColumn relCol = new QPResult.MeasureColumn(measureOrdinal, measureMember);
        targetSet.add(new NamedExpr(aggFun, relCol.alias()));
    }

    public boolean isValid() {
        return valid;
    }

    public Query getMdxQuery() {
        return mdxQuery;
    }

    public QPResult execute() {
        if (!isValid()) { throw new IllegalStateException(); }

        TajoClient client = null;
        ResultSet rs = null;
        try {
            client = new TajoClient(new TajoConf());
            rs = client.executeQueryAndGetResult(planContext);

            // I need a silly number formatter just to be sane.
            Format numFormatter = new Format("#,###", (Format.FormatLocale) null);
            int[] cellKey = new int[qpResult.axes.size()];
            for (int axisOrdinal = qpResult.axes.size() - 1; axisOrdinal > -1; axisOrdinal--) {
                QPResult.QPAxis qpAxis = qpResult.axes.get(axisOrdinal);
                QPResult.PositionList positionList = qpAxis.getPositionList();
                QPResult.PositionImpl pos = positionList.get(0);
                rs.beforeFirst();
                while (rs.next()) {
                    for (QPResult.Column relationalColumn : qpAxis.relationalColumns) {
                        if (relationalColumn instanceof QPResult.MemberColumn) {
                            QPResult.MemberColumn memberRelationalColumn = (QPResult.MemberColumn) relationalColumn;
                            String memberCaption = rs.getString(relationalColumn.alias());
                            List<Id.Segment> memberFqName = Util.parseIdentifier(
                                Util.makeFqName(memberRelationalColumn.memberFqName, memberCaption));
                            MemberExpr memberExpr = (MemberExpr) Util.lookup(mdxQuery, memberFqName, true);
                            positionList.addPositionWithMember(pos, memberExpr.getMember());
                            pos = null;

                        } else if (relationalColumn instanceof QPResult.MeasureColumn) {

                        }
                    }

                    addCell(CellKey.Generator.newCellKey(cellKey), rs.getLong("m0"), numFormatter);
                    cellKey[cellKey.length-1]++;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (client != null) {
                client.close();
            }
        }

        return qpResult;
    }

    private void addCell(CellKey key, Object value, Format format) {
        QPResult.QPCell cell = new QPResult.QPCell();
        cell.value = value;
        cell.formattedValue = format == null ? String.valueOf(cell.value) : format.format(cell.value);
        qpResult.cells.put(key, cell);
    }
}
