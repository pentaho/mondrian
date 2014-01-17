package mondrian.queryplan;

import com.google.protobuf.ServiceException;
import mondrian.mdx.*;
import mondrian.olap.*;
import mondrian.rolap.*;
import mondrian.spi.*;
import mondrian.spi.MemberFormatter;
import mondrian.util.Format;
import org.apache.tajo.algebra.*;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.conf.TajoConf;

import java.io.IOException;
import java.sql.*;
import java.util.*;

public class QueryPlan {
    public static final QueryPlan DUMMY = new QueryPlan();
    private boolean valid;
    private final Query mdxQuery;
    private String planContext;

    private QueryPlan() {
        valid = false;
        mdxQuery = null;
        planContext = null;
    }

    public QueryPlan(Query mdxQuery) {
        this.mdxQuery = mdxQuery;
        this.planContext = null;

        // not yet validated
        this.valid = false;
    }

    public void build() {
        // Gather some useful objects from the query for inspection.
        QueryAxis[] axises = mdxQuery.getAxes();
        RolapCube cube = (RolapCube)mdxQuery.getCube();
        RolapSchema.PhysSchema physicalSchema = cube.getSchema().getPhysicalSchema();
        //RolapSchema.PhysSchemaGraph graph = physicalSchema.getGraph();
        List<RolapStar> stars = cube.getStars();
        if (stars.size() > 1) {
            System.err.println("Multiple stars per cube not currently implemented");
        }
        RolapStar star = stars.get(0);

        // Create the base Expr of the contextPlan
        Projection rootProjection = new Projection();

        // Create the Aggregation since it is always (?) needed
        Aggregation aggregation = new Aggregation();

        // Don't forget to set the quals at the end
        Selection filter = new Selection(null);

        aggregation.setChild(filter);

        // Don't forget to set the SortSpec[] at the end
        Sort sort = new Sort(null);
        sort.setChild(aggregation);
        List<Sort.SortSpec> sortSpecs = new ArrayList<>();


        // Create containers for Expr objects that must be converted to array form later
        Set<Relation> relationSet = new LinkedHashSet<>();
        Set<TargetExpr> targetSet = new LinkedHashSet<>();
        List<Expr> andExprs = new ArrayList<>();
        List<Aggregation.GroupElement> groupElements = new ArrayList<>();

            // Add the relation for the fact table
        RolapSchema.PhysRelation factRelation = star.getFactTable().getRelation();
        relationSet.add(new Relation(factRelation.getAlias()));

        // Special case for a query like "select from [SteelWheelSales]"
        if (axises.length == 0) {
            planDefaultMeasure(cube, targetSet);
        }

        for (int i = 0, axisesLength = axises.length; i < axisesLength; i++) {
            QueryAxis axis = axises[i];
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
                        if (memberExprMember instanceof RolapBaseCubeMeasure) {
                            RolapBaseCubeMeasure rolapBaseCubeMeasure = (RolapBaseCubeMeasure) memberExprMember;
                            planMeasureMember(targetSet, rolapBaseCubeMeasure);
                        } else if (memberExprMember instanceof RolapMemberBase) {
                            RolapMemberBase rolapMemberBase = (RolapMemberBase) memberExprMember;
                            RolapAttribute attr = (rolapMemberBase.getLevel()).getAttribute();
                            RolapSchema.PhysColumn physColumn = attr.getNameExp();
                            String relationAlias = physColumn.relation.getAlias();

                            // Ensure we have this relation and its qual
                            if (relationSet.add(new Relation(relationAlias))) {
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
                                andExprs.add(new BinaryOperator(OpType.Equals,
                                    new ColumnReferenceExpr(factRelation.getAlias(), foreignKey.name),
                                    new ColumnReferenceExpr(relationAlias, sourceKey.name)));
                            }

                            // Add a target column reference
                            ColumnReferenceExpr col = new ColumnReferenceExpr(relationAlias, physColumn.name);
                            targetSet.add(new TargetExpr(col, memberExprMember.getUniqueName()));

                            // Add an Equals predicate for this member
                            LiteralValue value = new LiteralValue(String.valueOf(rolapMemberBase.getKey()), LiteralValue.LiteralType.String);
                            andExprs.add(new BinaryOperator(OpType.Equals, col, value));

                            // Add grouping for this target
                            groupElements.add(new Aggregation.GroupElement(Aggregation.GroupType.OrdinaryGroup, new ColumnReferenceExpr[]{col}));

                            // TODO: I need the default measure here because none was specified.  Need to generify.
                            planDefaultMeasure(cube, targetSet);
                        } else {
                            System.err.println("Skipping QueryPlan: No implementation for member "+memberExprMember.getClass().getName());
                            return;
                        }
                    } else if (arg instanceof HierarchyExpr && "Children".equals(axisSetResolvedFunCall.getFunName())) {
                        RolapCubeHierarchy rolapCubeHierarchy = (RolapCubeHierarchy) ((HierarchyExpr)arg).getHierarchy();
                        // Get the level just below All.
                        RolapCubeLevel childrenLevel = rolapCubeHierarchy.getLevelList().get(1);
                        RolapAttribute attr = childrenLevel.getAttribute();
                        RolapSchema.PhysColumn physColumn = attr.getNameExp();
                        String relationAlias = physColumn.relation.getAlias();

                        // Add a target column reference
                        ColumnReferenceExpr col = new ColumnReferenceExpr(relationAlias, physColumn.name);
                        targetSet.add(new TargetExpr(col, childrenLevel.getUniqueName()));

                        // Ensure we have this relation and its qual
                        if (relationSet.add(new Relation(relationAlias))) {
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
                            andExprs.add(new BinaryOperator(OpType.Equals,
                                new ColumnReferenceExpr(factRelation.getAlias(), foreignKey.name),
                                new ColumnReferenceExpr(relationAlias, sourceKey.name)));
                            sortSpecs.add(new Sort.SortSpec(col, true, false));
                        }

                        // Add grouping for this target
                        groupElements.add(new Aggregation.GroupElement(Aggregation.GroupType.OrdinaryGroup, new ColumnReferenceExpr[]{col}));

                        // TODO: I need the default measure here because none was specified.  Need to generify.
                        planDefaultMeasure(cube, targetSet);
                    } else if (arg instanceof ResolvedFunCall) {
                        ResolvedFunCall argResolvedFunCall = (ResolvedFunCall)arg;
                        if (argResolvedFunCall.getArgCount() != 1) {
                            System.err.println("Skipping QueryPlan: Too many args in argResolvedFunCall");
                            return;
                        }
                        if ("Members".equals(argResolvedFunCall.getFunName())) {
                            Dimension dim = ((DimensionExpr) argResolvedFunCall.getArg(0)).getDimension();
                            if (dim.isMeasures()) {
                                for (RolapMember rolapMember : cube.getMeasuresMembers()) {
                                    if (rolapMember instanceof RolapBaseCubeMeasure) {
                                        planMeasureMember(targetSet,(RolapBaseCubeMeasure)rolapMember);
                                    } else {
                                        System.err.println("Skipping QueryPlan: MeasureMember is not RolapBaseCubeMeasure");
                                        return;
                                    }

                                }
                            } else {
                                System.err.println("Skipping QueryPlan: DimensionExpr Members is not Measures Dim");
                                return;
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

        // Finalize the contextPlan
        rootProjection.setTargets(targetSet.toArray(new TargetExpr[targetSet.size()]));
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
        this.planContext = JsonHelper.toJson(rootProjection);
        this.valid = true;
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

    private void planDefaultMeasure(RolapCube cube, Set<TargetExpr> targetSet) {
        planMeasureMember(targetSet, (RolapBaseCubeMeasure) cube.getMeasuresHierarchy().getDefaultMember());
    }

    private void planMeasureMember(Set<TargetExpr> targetSet, RolapBaseCubeMeasure measureMember) {
        RolapSchema.PhysColumn physColumn = measureMember.getExpr();
        RolapAggregator agg = measureMember.getAggregator();
        ColumnReferenceExpr col = new ColumnReferenceExpr(physColumn.name);
        GeneralSetFunctionExpr aggFun = new GeneralSetFunctionExpr(agg.getName(),agg.isDistinct(), col);
        targetSet.add(new TargetExpr(aggFun, measureMember.getName()));
    }

    public boolean isValid() {
        return valid;
    }

    public Query getMdxQuery() {
        return mdxQuery;
    }

    public QPResult execute() {
        if (!isValid()) { throw new IllegalStateException(); }

        // Gather some useful objects from the query for inspection.
        QueryAxis[] axises = mdxQuery.getAxes();
        RolapCube cube = (RolapCube)mdxQuery.getCube();
        RolapSchema.PhysSchemaGraph graph = cube.getSchema().getPhysicalSchema().getGraph();
        List<RolapStar> stars = cube.getStars();
        if (stars.size() > 1) {
            System.err.println("Multiple stars per cube not currently implemented");
        }
        RolapStar star = stars.get(0);

        // I need a silly number formatter just to be sane.
        Format numFormatter = new Format("#,###", (Format.FormatLocale) null);
        QPResult qpresult = new QPResult();

        try {
            TajoClient client = new TajoClient(new TajoConf());

            ResultSet rs = client.executeQueryAndGetResult(planContext);

            // Create an axis in case we need one.
            QPResult.QPAxis qpAxis = new QPResult.QPAxis();
            CellKey cellKey = CellKey.Generator.newCellKey(axises.length);

            // Special case for a query like "select from [SteelWheelSales]"
            if (axises.length == 0) {
                RolapMember defaultMember = cube.getMeasuresHierarchy().getDefaultMember();
                while (rs.next()) {
                    int measureValue = rs.getInt(defaultMember.getName());
                    addCell(qpresult, cellKey.copy(), measureValue, numFormatter);
                }
            } else {
                qpresult.axes.add(qpAxis);
            }

            for (int i = 0, axisesLength = axises.length; i < axisesLength; i++) {

                QueryAxis axis = axises[i];
                Exp axisSet = axis.getSet();
                if (axisSet instanceof ResolvedFunCall) {
                    ResolvedFunCall axisSetResolvedFunCall = (ResolvedFunCall)axisSet;
                    for (Exp arg : axisSetResolvedFunCall.getArgs()) {
                        if (arg instanceof MemberExpr) {
                            MemberExpr memberExpr = (MemberExpr)arg;
                            Member memberExprMember = memberExpr.getMember();
                            qpAxis.poslist.list.get(0).tupleList.add(memberExprMember);
                            if (memberExprMember instanceof RolapBaseCubeMeasure) {
                                RolapBaseCubeMeasure rolapBaseCubeMeasure = (RolapBaseCubeMeasure) memberExprMember;
                                int j = 0;
                                while (rs.next()) {
                                    int value = rs.getInt(rolapBaseCubeMeasure.getName());
                                    addCell(qpresult, cellKey.copy(), value, numFormatter);
                                    cellKey.setAxis(i,j++);
                                }
                            } else if (memberExprMember instanceof RolapMemberBase) {
                                RolapBaseCubeMeasure defaultMember = (RolapBaseCubeMeasure) cube.getMeasuresHierarchy().getDefaultMember();
                                int j = 0;
                                while (rs.next()) {
                                    int measureValue = rs.getInt(defaultMember.getName());
                                    addCell(qpresult, cellKey.copy(), measureValue, numFormatter);
                                    cellKey.setAxis(i,j++);
                                }
                            }
                        } else if (arg instanceof HierarchyExpr && "Children".equals(axisSetResolvedFunCall.getFunName())) {
                            RolapBaseCubeMeasure defaultMember = (RolapBaseCubeMeasure) cube.getMeasuresHierarchy().getDefaultMember();
                            RolapCubeHierarchy rolapCubeHierarchy = (RolapCubeHierarchy) ((HierarchyExpr)arg).getHierarchy();
                            // Get the level just below All.
                            RolapCubeLevel childrenLevel = rolapCubeHierarchy.getLevelList().get(1);
                            String memberLevelName = childrenLevel.getUniqueName();
                            QPResult.PositionImpl posMembers = qpAxis.poslist.list.get(0);
                            int j = 0;
                            while (rs.next()) {
                                String memberValue = rs.getString(memberLevelName);
                                List<Id.Segment> memberFqName = Util.parseIdentifier(Util.makeFqName(memberLevelName, memberValue));
                                MemberExpr memberExpr = (MemberExpr) Util.lookup(mdxQuery, memberFqName, true);
                                posMembers.tupleList.add(memberExpr.getMember());

                                int measureValue = rs.getInt(defaultMember.getName());
                                addCell(qpresult, cellKey.copy(), measureValue, numFormatter);

                                posMembers = new QPResult.PositionImpl();
                                qpAxis.poslist.list.add(posMembers);
                                cellKey.setAxis(i,++j);

                            }
                            posMembers.tupleList.add(rolapCubeHierarchy.getNullMember());
                        } else if (arg instanceof ResolvedFunCall) {
                            ResolvedFunCall argResolvedFunCall = (ResolvedFunCall)arg;
                            if ("Members".equals(argResolvedFunCall.getFunName())) {
                                while (rs.next()) {
                                    Dimension dim = ((DimensionExpr) argResolvedFunCall.getArg(0)).getDimension();
                                    if (dim.isMeasures()) {
                                        QPResult.PositionImpl posMembers = qpAxis.poslist.list.get(0);
                                        List<RolapMember> measuresMembers = ((RolapCube) mdxQuery.getCube()).getMeasuresMembers();
                                        for (int j = 0; j < measuresMembers.size(); j++) {
                                            RolapMember rolapMember = measuresMembers.get(j);
                                            if (rolapMember instanceof RolapBaseCubeMeasure) {
                                                posMembers.tupleList.add(rolapMember);
                                                if (rolapMember instanceof RolapBaseCubeMeasure) {
                                                    RolapBaseCubeMeasure rolapBaseCubeMeasure = (RolapBaseCubeMeasure) rolapMember;
                                                    int value = rs.getInt(rolapBaseCubeMeasure.getName());
                                                    addCell(qpresult, cellKey.copy(), value, numFormatter);
                                                }
                                            }
                                            if (j+1 < measuresMembers.size()) {
                                                posMembers = new QPResult.PositionImpl();
                                                qpAxis.poslist.list.add(posMembers);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                }
            }



        } catch (IOException e) {
            e.printStackTrace();
        } catch (ServiceException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return qpresult;
    }

    private void addCell(QPResult qpresult, CellKey key, Object value, Format format) throws SQLException {
        QPResult.QPCell cell = new QPResult.QPCell();
        cell.value = value;
        cell.formattedValue = format == null ? String.valueOf(cell.value) : format.format(cell.value);
        qpresult.cells.put(key, cell);
    }
}
