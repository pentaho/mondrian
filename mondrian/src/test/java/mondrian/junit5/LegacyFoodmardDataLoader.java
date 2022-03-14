package mondrian.junit5;

import mondrian.test.loader.MondrianFoodMartLoaderX;

public class LegacyFoodmardDataLoader implements DataLoader{
    
  

    @Override
    public boolean loadData(String jdbcUrl) {
	String[] args=new String[]{
		 "-verbose",
                "-tables",
                "-data",
                "-indexes",
                "-outputJdbcURL="+jdbcUrl,
//                "-outputJdbcUser="+mySQLContainer.getUsername(),
//                "-outputJdbcPassword="+mySQLContainer.getPassword(),
                "-outputJdbcBatchSize=500000",
                "-jdbcDrivers=com.mysql.cl.jdbc.Driver"		
	};
	 MondrianFoodMartLoaderX.main(args);
	return true;
    }
    }
