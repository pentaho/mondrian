package mondrian.junit5;

public interface DataLoader {
    /**
     * @param jdbcConnectionUrl - jdbcConnectionUrl
     * @return jdbc connection String
     * @throws Exception 
     */
    boolean loadData(String jdbcConnectionUrl) throws Exception;

}
