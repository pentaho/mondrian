# frozen_string_literal: true

module DatabaseAdmin
  # Default admin credentials per driver
  ADMIN_DEFAULTS = {
    'mysql'      => { user: 'root',     password: '' },
    'postgresql' => { user: 'postgres', password: 'postgres' },
    'oracle'     => { user: 'system',   password: 'manager' },
    'sqlserver'  => { user: 'sa',       password: 'Password12!' },
    'clickhouse' => { user: 'default',  password: '' }
  }.freeze

  module_function

  def create_foodmart!
    execute_sql(admin_jdbc_url, admin_user, admin_password, create_sql)
    if (post_sql = post_create_sql)
      execute_sql(target_jdbc_url, admin_user, admin_password, post_sql)
    end
  end

  def drop_foodmart!
    execute_sql(admin_jdbc_url, admin_user, admin_password, drop_sql)
  end

  def admin_user
    env_prefix = MONDRIAN_DRIVER.upcase
    ENV["#{env_prefix}_ADMIN_USER"] || ADMIN_DEFAULTS.dig(MONDRIAN_DRIVER, :user)
  end

  def admin_password
    env_prefix = MONDRIAN_DRIVER.upcase
    ENV["#{env_prefix}_ADMIN_PASSWORD"] || ADMIN_DEFAULTS.dig(MONDRIAN_DRIVER, :password)
  end

  def admin_jdbc_url
    case MONDRIAN_DRIVER
    when 'mysql'
      port = DATABASE_PORT || '3306'
      "jdbc:mysql://#{DATABASE_HOST}:#{port}/?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true"
    when 'postgresql'
      port = DATABASE_PORT || '5432'
      "jdbc:postgresql://#{DATABASE_HOST}:#{port}/postgres"
    when 'oracle'
      port = DATABASE_PORT || '1521'
      if DATABASE_INSTANCE
        "jdbc:oracle:thin:@#{DATABASE_HOST}:#{port}:#{DATABASE_INSTANCE}"
      else
        "jdbc:oracle:thin:@#{DATABASE_HOST}:#{port}/#{DATABASE_NAME&.delete_prefix('/')}"
      end
    when 'sqlserver'
      port = DATABASE_PORT || '1433'
      url = "jdbc:sqlserver://#{DATABASE_HOST}:#{port};databaseName=master"
      url += ";instanceName=#{DATABASE_INSTANCE}" if DATABASE_INSTANCE
      url
    when 'clickhouse'
      port = DATABASE_PORT || '8123'
      "jdbc:clickhouse://#{DATABASE_HOST}:#{port}/default"
    end
  end

  # SQL Server needs a second connection to the newly created database
  def target_jdbc_url
    port = DATABASE_PORT || '1433'
    url = "jdbc:sqlserver://#{DATABASE_HOST}:#{port};databaseName=#{DATABASE_NAME}"
    url += ";instanceName=#{DATABASE_INSTANCE}" if DATABASE_INSTANCE
    url
  end

  def create_sql
    case MONDRIAN_DRIVER
    when 'mysql'
      [
        "CREATE DATABASE IF NOT EXISTS `#{DATABASE_NAME}` DEFAULT CHARACTER SET utf8 DEFAULT COLLATE utf8_general_ci",
        "CREATE USER IF NOT EXISTS '#{DATABASE_USER}'@'%' IDENTIFIED BY '#{DATABASE_PASSWORD}'",
        "GRANT ALL PRIVILEGES ON `#{DATABASE_NAME}`.* TO '#{DATABASE_USER}'@'%'"
      ]
    when 'postgresql'
      [
        "DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '#{DATABASE_USER}') " \
          "THEN CREATE ROLE #{DATABASE_USER} PASSWORD '#{DATABASE_PASSWORD}' LOGIN CREATEDB; END IF; END $$",
        "CREATE DATABASE #{DATABASE_NAME}",
        "ALTER DATABASE #{DATABASE_NAME} OWNER TO #{DATABASE_USER}"
      ]
    when 'oracle'
      [
        "CREATE USER #{DATABASE_USER} IDENTIFIED BY #{DATABASE_PASSWORD} DEFAULT TABLESPACE users",
        "GRANT CONNECT, RESOURCE TO #{DATABASE_USER}",
        "ALTER USER #{DATABASE_USER} QUOTA UNLIMITED ON users"
      ]
    when 'sqlserver'
      [
        "IF NOT EXISTS (SELECT * FROM sys.server_principals WHERE name = '#{DATABASE_USER}') " \
          "CREATE LOGIN #{DATABASE_USER} WITH PASSWORD = '#{DATABASE_PASSWORD}', CHECK_POLICY = OFF",
        "IF DB_ID('#{DATABASE_NAME}') IS NULL CREATE DATABASE #{DATABASE_NAME}"
      ]
    when 'clickhouse'
      sql = ["CREATE DATABASE IF NOT EXISTS #{DATABASE_NAME}"]
      # The 'default' user is built-in and cannot be modified via SQL
      unless DATABASE_USER == 'default'
        sql << "CREATE USER IF NOT EXISTS #{DATABASE_USER} IDENTIFIED BY '#{DATABASE_PASSWORD}'"
        sql << "GRANT ALL ON #{DATABASE_NAME}.* TO #{DATABASE_USER}"
      end
      sql
    end
  end

  # SQL Server requires creating the database user in the target database
  def post_create_sql
    return unless MONDRIAN_DRIVER == 'sqlserver'

    [
      "IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = '#{DATABASE_USER}') " \
        "CREATE USER #{DATABASE_USER} FOR LOGIN #{DATABASE_USER}",
      "ALTER ROLE db_owner ADD MEMBER #{DATABASE_USER}"
    ]
  end

  def drop_sql
    case MONDRIAN_DRIVER
    when 'mysql'
      [
        "DROP DATABASE IF EXISTS `#{DATABASE_NAME}`",
        "DROP USER IF EXISTS '#{DATABASE_USER}'@'%'"
      ]
    when 'postgresql'
      [
        "DROP DATABASE IF EXISTS #{DATABASE_NAME}",
        "DROP ROLE IF EXISTS #{DATABASE_USER}"
      ]
    when 'oracle'
      [
        "DROP USER #{DATABASE_USER} CASCADE"
      ]
    when 'sqlserver'
      [
        "IF DB_ID('#{DATABASE_NAME}') IS NOT NULL DROP DATABASE #{DATABASE_NAME}",
        "IF EXISTS (SELECT * FROM sys.server_principals WHERE name = '#{DATABASE_USER}') DROP LOGIN #{DATABASE_USER}"
      ]
    when 'clickhouse'
      sql = ["DROP DATABASE IF EXISTS #{DATABASE_NAME}"]
      sql << "DROP USER IF EXISTS #{DATABASE_USER}" unless DATABASE_USER == 'default'
      sql
    end
  end

  def execute_sql(jdbc_url, user, password, statements)
    props = java.util.Properties.new
    props.setProperty('user', user)
    props.setProperty('password', password)

    # Oracle SYSDBA connection
    if MONDRIAN_DRIVER == 'oracle' && user.upcase == 'SYS'
      props.setProperty('internal_logon', 'SYSDBA')
    end

    # Use the driver directly instead of DriverManager, because JRuby loads
    # JDBC driver JARs into its own classloader which DriverManager cannot see.
    driver = java.lang.Class.forName(JDBC_DRIVER, true, JRuby.runtime.jruby_class_loader)
      .getDeclaredConstructor.newInstance

    # Retry on transient connection errors (e.g. Oracle network adapter failures on CI)
    retries = 0
    begin
      connection = driver.connect(jdbc_url, props)
    rescue Java::JavaSql::SQLException => e
      if retries < 3 && e.message =~ /Network Adapter|IO Error|Connection refused/i
        retries += 1
        puts "  Connection failed (#{e.message.strip}), retry #{retries}/3 in 5s..."
        sleep 5
        retry
      end
      raise
    end
    connection.auto_commit = true

    statements.each do |sql|
      puts "  SQL: #{sql}"
      begin
        connection.create_statement.execute(sql)
      rescue Java::JavaSql::SQLException => e
        if already_exists_or_not_found?(e)
          puts "  (skipped: #{e.message.strip})"
        else
          raise
        end
      end
    end
  ensure
    connection&.close
  end

  def already_exists_or_not_found?(error)
    msg = error.message
    # PostgreSQL: database already exists (42P04), role already exists (42710)
    # Oracle: user already exists (ORA-01920), user does not exist (ORA-01918)
    # SQL Server: "already has an account under a different user name"
    msg =~ /already exists|already has an account|does not exist|you do not have permission|ORA-01920|ORA-01918/i
  end
end
