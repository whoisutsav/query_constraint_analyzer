require 'pp'
require 'optparse'
require './load.rb'
require './query_parser.rb'
require './opt_check.rb'



def run_analysis(app_dir, options)
  raw_queries = load_queries(app_dir, options[:tmp_dir])
  meta_queries = derive_metadata(raw_queries) 
  constraints = load_constraints(app_dir, options[:tmp_dir], options[:constraint_analyzer_dir]) 

  results = opt_check(meta_queries, constraints)
  pp results 
end


app_dir = ARGV[0]
options = {
  :constraint_analyzer_dir => "/Users/utsavsethi/workspace/data-format-project/formatchecker/constraint_analyzer",
  :tmp_dir => "/tmp"
}

run_analysis(app_dir, options)
