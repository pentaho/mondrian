package mondrian.test.clearview;

import junit.framework.*;

import mondrian.test.*;

public class SummaryMetricPercentTest extends ClearViewBase {
	
	public SummaryMetricPercentTest() {
		super();
	}
	
	public SummaryMetricPercentTest(String name) {
		super(name);
	}

	protected DiffRepository getDiffRepos() {
		return getDiffReposStatic();
	}
	
	private static DiffRepository getDiffReposStatic() {
		return DiffRepository.lookup(SummaryMetricPercentTest.class);
	}
	
	public static TestSuite suite() {
		return constructSuite(getDiffReposStatic(), SummaryMetricPercentTest.class);
	}

}
