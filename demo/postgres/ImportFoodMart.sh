dropdb foodmart
createdb foodmart
cat ../oracle/FoodMartTables.sql | ./fixtables | psql foodmart > psql.log
cat ../oracle/FoodMartData.sql | ./fixdata | psql foodmart >> psql.log
cat ../oracle/FoodMartIndexes.sql | ./fixindexes | psql foodmart >> psql.log

