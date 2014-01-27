package mondrian.queryplan;

import mondrian.olap.Query;

public class QueryPlanner {

    public static QueryPlan build(Query query) {
        if (query != null &&
            query.getConnection() != null &&
            query.getCube() != null
            ) {
            QueryPlan plan = new QueryPlan(query);
            plan.build();
            return plan;
        } else {
            return QueryPlan.DUMMY;
        }
    }

}
