
mysqladmin -u root -f drop foodmart
mysqladmin -u root -f create foodmart

cat ../oracle/FoodMartTables.sql | ./fixtables | mysql -u root foodmart
cat ../oracle/FoodMartData.sql | ./fixdata | mysql -u root foodmart
cat ../oracle/FoodMartIndexes.sql | ./fixindexes | mysql -u root foodmart


