package mondrian.test;

public class AggCrossjoinNativeTest extends FoodMartTestCase {
  public void testCompoundAggCalcMemberInSlicer() {
    String query = "WITH member store.agg as " +
            "'Aggregate(CrossJoin(Store.[Store Name].members, Gender.F))' " +
            "SELECT filter(customers.[name].members, measures.[unit sales] > 100) on 0 " +
            "FROM sales where store.agg";

    verifySameNativeAndNot(
            query, "Compound aggregated member should return same results with native filter on/off",
            getTestContext());
    
  }
}
