# frozen_string_literal: true

require 'rake/testtask'

Rake::TestTask.new(:test) do |t|
  t.libs << 'test'
  t.pattern = 'test/**/*_test.rb'
  t.warning = false
end

namespace :test do
  %w(mysql postgresql oracle sqlserver).each do |driver|
    desc "Run tests with #{driver} driver"
    task driver do
      ENV['MONDRIAN_DRIVER'] = driver
      Rake::Task['test'].reenable
      Rake::Task['test'].invoke
    end
  end
end

desc "Build Mondrian JAR with Maven"
task :package do
  system("mvn package")
end

desc "Compile Mondrian Java test classes"
task :compile_java_tests do
  system("mvn -f mondrian/pom.xml test-compile -q 2>/dev/null") || raise("Maven test-compile failed")
end

namespace :db do
  desc "Create FoodMart database and user"
  task :create_foodmart do
    require_relative "support/database_setup"
    require_relative "support/database_admin"

    puts "==> Creating FoodMart database and user on #{MONDRIAN_DRIVER}..."
    DatabaseAdmin.create_foodmart!
    puts "==> Done."
  end

  desc "Drop FoodMart database and user"
  task :drop_foodmart do
    require_relative "support/database_setup"
    require_relative "support/database_admin"

    puts "==> Dropping FoodMart database and user on #{MONDRIAN_DRIVER}..."
    DatabaseAdmin.drop_foodmart!
    puts "==> Done."
  end

  desc "Load FoodMart data into database using MondrianFoodMartLoader"
  task :load_foodmart => :compile_java_tests do
    require_relative "support/database_setup"

    # Add compiled test classes and resources to the JRuby classpath
    $CLASSPATH << File.join(PROJECT_ROOT, "mondrian/target/test-classes")
    $CLASSPATH << File.join(PROJECT_ROOT, "mondrian/src/it/resources")

    # Configure Log4j to use the test log4j2.xml so output appears on the console
    log4j_config = File.join(PROJECT_ROOT, "mondrian/src/it/resources/log4j2.xml")
    Java::JavaLang::System.setProperty("log4j2.configurationFile", log4j_config)

    args = [
      "-verbose", "-tables", "-data", "-indexes",
      "-outputJdbcURL=#{DATABASE_JDBC_URL}",
      "-outputJdbcUser=#{DATABASE_USER}",
      "-outputJdbcPassword=#{DATABASE_PASSWORD}",
      "-outputJdbcBatchSize=50",
      "-jdbcDrivers=#{JDBC_DRIVER}"
    ].to_java(:string)

    puts "==> Loading FoodMart data into #{MONDRIAN_DRIVER} database..."
    Java::mondrian.test.loader.MondrianFoodMartLoader.main(args)
  end
end
