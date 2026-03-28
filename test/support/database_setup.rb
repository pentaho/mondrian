# frozen_string_literal: true

require 'bundler/setup'

# Autoload JDBC drivers
Java::JavaLang::System.setProperty('jdbc.driver.autoload', 'true')

MONDRIAN_DRIVER = ENV['MONDRIAN_DRIVER'] || 'mysql'
env_prefix = MONDRIAN_DRIVER.upcase

DATABASE_HOST     = ENV["#{env_prefix}_DATABASE_HOST"]     || ENV['DATABASE_HOST'] || 'localhost'
DATABASE_PORT     = ENV["#{env_prefix}_DATABASE_PORT"]     || ENV['DATABASE_PORT']
DATABASE_USER     = ENV["#{env_prefix}_DATABASE_USER"]     || ENV['DATABASE_USER'] || 'foodmart'
DATABASE_PASSWORD = ENV["#{env_prefix}_DATABASE_PASSWORD"] || ENV['DATABASE_PASSWORD'] || 'foodmart'
DATABASE_NAME     = ENV["#{env_prefix}_DATABASE_NAME"]     || ENV['DATABASE_NAME'] || 'foodmart'
DATABASE_INSTANCE = ENV["#{env_prefix}_DATABASE_INSTANCE"] || ENV['DATABASE_INSTANCE']

JDBC_DRIVER = case MONDRIAN_DRIVER
when 'mysql'
  require 'jdbc/mysql'
  'com.mysql.cj.jdbc.Driver'
when 'postgresql'
  require 'jdbc/postgres'
  'org.postgresql.Driver'
when 'oracle'
  Dir[File.expand_path("jars/ojdbc*.jar", __dir__)].each { |f| require f }
  'oracle.jdbc.OracleDriver'
when 'sqlserver'
  Dir[File.expand_path("jars/mssql-jdbc*.jar", __dir__)].each { |f| require f }
  'com.microsoft.sqlserver.jdbc.SQLServerDriver'
when 'clickhouse'
  Dir[File.expand_path("jars/{slf4j*,clickhouse*}.jar", __dir__)].each { |f| require f }
  'com.clickhouse.jdbc.ClickHouseDriver'
end

# Build JDBC URL for FoodMart loader
DATABASE_JDBC_URL = case MONDRIAN_DRIVER
when 'mysql'
  port = DATABASE_PORT || '3306'
  "jdbc:mysql://#{DATABASE_HOST}:#{port}/#{DATABASE_NAME}?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true"
when 'postgresql'
  port = DATABASE_PORT || '5432'
  "jdbc:postgresql://#{DATABASE_HOST}:#{port}/#{DATABASE_NAME}"
when 'oracle'
  port = DATABASE_PORT || '1521'
  if DATABASE_INSTANCE
    "jdbc:oracle:thin:@#{DATABASE_HOST}:#{port}:#{DATABASE_INSTANCE}"
  else
    "jdbc:oracle:thin:@#{DATABASE_HOST}:#{port}/#{DATABASE_NAME&.delete_prefix('/')}"
  end
when 'sqlserver'
  port = DATABASE_PORT || '1433'
  url = "jdbc:sqlserver://#{DATABASE_HOST}:#{port};databaseName=#{DATABASE_NAME}"
  url += ";instanceName=#{DATABASE_INSTANCE}" if DATABASE_INSTANCE
  url
when 'clickhouse'
  port = DATABASE_PORT || '8123'
  "jdbc:clickhouse://#{DATABASE_HOST}:#{port}/#{DATABASE_NAME}"
end

puts "==> Using #{MONDRIAN_DRIVER} driver"

# Point mondrian-olap to use the locally built Mondrian JAR
PROJECT_ROOT = File.expand_path('../..', __dir__)
# Use the latest JAR in case there are multiple (e.g. from multiple builds)
mondrian_jar = Dir[File.join(PROJECT_ROOT, 'mondrian/target/mondrian-olap-java-*.jar')].sort.last
if mondrian_jar
  ENV['MONDRIAN_OLAP_JAR_PATH'] = mondrian_jar
  puts "==> Using locally built JAR: #{File.basename(mondrian_jar)}"
else
  puts "WARNING: No locally built Mondrian JAR found in mondrian/target/."
  puts "         Run 'mvn package' first. Falling back to mondrian-olap gem's bundled JAR."
end

require 'mondrian/olap'

CATALOG_FILE = File.join(PROJECT_ROOT, 'demo/FoodMart.xml')

CONNECTION_PARAMS = {
  driver: MONDRIAN_DRIVER,
  host: DATABASE_HOST,
  port: DATABASE_PORT,
  database: DATABASE_NAME,
  username: DATABASE_USER,
  password: DATABASE_PASSWORD,
  catalog: CATALOG_FILE
}.compact

case MONDRIAN_DRIVER
when 'mysql'
  CONNECTION_PARAMS[:properties] = {useSSL: false, serverTimezone: 'UTC', allowPublicKeyRetrieval: true}
end
