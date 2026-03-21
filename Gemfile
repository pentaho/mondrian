source 'https://rubygems.org'

ruby RUBY_VERSION, engine: 'jruby', engine_version: JRUBY_VERSION

gem 'jdbc-mysql', '~> 8.0.30'
gem 'jdbc-postgres', '~> 42.7.8'
gem 'minitest', '~> 5.25'
gem 'minitest-hooks', '~> 1.5'
gem 'minitest-reporters', '~> 1.7'
gem 'pry', '~> 0.14.1'
gem 'rake', '~> 13.3.0'

if mondrian_olap_path = ENV['MONDRIAN_OLAP_PATH']
  gem 'mondrian-olap', path: mondrian_olap_path, require: false
else
  gem 'mondrian-olap', git: 'https://github.com/rsim/mondrian-olap.git', branch: 'master', require: false
end
