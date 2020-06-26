require 'pp'
require 'optparse'
require './load.rb'
require './query_parser_with_sql.rb'
require './opt_check.rb'
require './fuzzy_opt_check.rb'


app_name = ARGV[0]
options = {}
app_dir = ""
$app_name = app_name.capitalize

def run_analysis(app_name, app_dir, options)
  #query_output_file = File.join(options[:tmp_dir], "query_output_#{app_name}")
  #exec("rm #{query_output_file}")
  raw_queries, scopes, schema = load_queries_and_schema(app_dir, options[:tmp_dir], options[:rails_best_practices_cmd])
  
  #meta_queries = derive_metadata(raw_queries, schema) 
  #exit

  print_detail_with_sql(raw_queries, scopes, schema)
  exit	

  #meta_queries = derive_metadata_with_sql(raw_queries, scopes, schema) 
  
  #constraints = load_constraints(app_dir, options[:tmp_dir], options[:constraint_analyzer_dir])
  # constraints.each do |c|
  #   puts "table = #{c[:table]}, type = #{c[:type]}, fields = #{c[:fields]}, in_db = #{c[:exists_in_db]}"
  # end
  # exit
	#fuzzy_check(meta_queries, constraints)

end


config = YAML.load_file('config.yml')
config.each do |key, value|
  if key == 'apps_dir' 
    app_dir = "#{value}/#{app_name}"
  elsif key == 'constraint_analyzer_dir'
    options[:constraint_analyzer_dir] = value
  elsif key == 'tmp_output_dir'
    options[:tmp_dir] = value
  elsif key == 'rails_best_practices_cmd'
	options[:rails_best_practices_cmd] = value
  end
end

run_analysis(app_name, app_dir, options)
