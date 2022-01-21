package mondrian.junit5;

public interface DataLoader {
    /**
     * @param jdbcConnectionUrl - jdbcConnectionUrl
     * @return jdbc connection String
     */
    boolean loadDataData(String jdbcConnectionUrl);

}
