require 'pp'
require 'rails_best_practices'
require 'yard'

APPS_DIR = "/Users/utsavsethi/workspace/apps2" 
CONSTRAINT_ANALYZER_DIR = "/Users/utsavsethi/workspace/data-format-project/formatchecker/constraint_analyzer"

# return array of all statements containing queries in format
# [{:class => "CLASSNAME", :stmt => "stmt containing query"}]
def load_queries(app_name)
  output_file = "/tmp/query_output_#{app_name}"
  app_dir = File.join(APPS_DIR, "/#{app_name}") 
  `cd #{app_dir} && echo "PrintQueryCheck: { output_filename: \"#{output_file}\"}" &> ./config/rails_best_practices.yml && rails_best_practices . -c ./config/rails_best_practices.yml`
  return Marshal.load(File.binread(output_file)) 
end

# return array of all statements containing queries in format
# [{:class => "CLASSNAME", :stmt => "stmt containing query"}]
def load_constraints(app_name)
  output_file = "/tmp/constraint_output_#{app_name}"
  app_dir = File.join(APPS_DIR, "/#{app_name}") 
  `cd #{CONSTRAINT_ANALYZER_DIR} && ruby main.rb -a #{app_dir} --dump-constraints #{output_file}` 
  return Marshal.load(File.binread(output_file))
end

def extract_query(ast)
  node = ast.type == :list ? ast[0] : ast
  if node.type == :assign and node[1].type == :call
    return node[1] 
  elsif node.type == :call
    return node 
  end
end

def get_all_methods(node)
  return [] if node == nil

  if node.type == :call
    return get_all_methods(node[0]) + get_all_methods(node[2])
  elsif node.type == :ident
    return [node[0]]
  else
    return []
  end
end

def extract_string(node)
  if node.type == :string_literal 
    return node[0][0][0]
  elsif node.type == :symbol_literal
    return node[0][0][0] 
  elsif node.type == :label
    return node[0]
  end
end

def extract_null_fields_from_args(node)
  return [] if node == nil or node.type != :arg_paren

  output = []
  if node[0][0].type == :list
    node[0][0].each do |child|
      next if child.type != :assoc
      next if child[1].type != :var_ref or child[1][0].type != :kw and child[1][0][0] != "nil"

      key = extract_string(child[0])
      table_name = key.rpartition(".")[0]
      field_name = key.rpartition(".")[2] 
      output << field_name
    end
  end

  return output 
end

def get_not_null(node)
  return [] if node == nil

  if node.type == :call and node[2].type == :ident and node[2][0] == "not" and
      node[0] and node[0].type == :call and node[0][2].type == :ident and node[0][2][0] == "where"
    return get_not_null(node[0]) + extract_null_fields_from_args(node[3])
  elsif node.type == :call
    return get_not_null(node[0])
  else
    return []
  end
end

def extract_fields_from_args(node)
  return [] if node == nil or node.type != :arg_paren 

  output = []
  if node[0][0].type == :list
    node[0][0].each do |child|
      next if child.type != :assoc

      key = extract_string(child[0])
      return [] if key == nil # TODO - handle this in future (although case seems rare)
      table_name = key.rpartition(".")[0]
      field_name = key.rpartition(".")[2] 
      output << field_name
    end
  end

  return output 
end

def get_filter_fields(node)
  return [] if node == nil

  if node.type == :call and node[2].type == :ident and (node[2][0] == "where" or node[2][0] == "not" or node[2][0] == "find_by") 
    #puts "found where/not node with arguments: "
    #pp node[3]
    return get_filter_fields(node[0]) + extract_fields_from_args(node[3]) 
  elsif node.type == :call
    return get_filter_fields(node[0])
  else
    return []
  end
end

def infer_object_type(node)
  if node != nil
    if node.type == :call
      return infer_object_type(node[0])
    elsif node.type == :var_ref
      ref = node[0][0]
      return ref.start_with?('@') ? ref.slice(1,ref.length-1).capitalize : ref
    end
  end
end


# parse queries and add metadata fields to query info object
def process_queries(query_arr)
  output = []
  query_arr.each do |query_obj|
    #puts "processing query: " + query_obj.to_s 
    begin 
      ast = YARD::Parser::Ruby::RubyParser.parse(query_obj[:stmt]).root 
    rescue  
      next
    end
    query = extract_query(ast) 
    query_methods = get_all_methods(query)
    
    object_type = infer_object_type(query)
    is_find = query_methods.include?("find")
    filter_fields = (query_methods.include?("where") or query_methods.include? ("find_by")) ? get_filter_fields(query) : []
    not_null_fields = query_methods.include?("not") ? get_not_null(query) : []
    has_distinct = query_methods.include?("distinct") 
    has_limit = %w(find find_by first first! last last! first_or_create).any? {|method| query_methods.include?(method)} 
    only_raw_sql = (filter_fields.empty? and (query_methods.include?("where") or query_methods.include?("find_by_sql"))) 

    output << query_obj.dup.merge({
      :object_type => object_type,
      :is_find => is_find,
      :filter_fields => filter_fields,
      :not_null_fields => not_null_fields,
      :has_distinct => has_distinct,
      :has_limit => has_limit,
      :only_raw_sql => only_raw_sql
    })
  end

  return output
end

# turn constraints into map
def process_constraints(constraint_arr)
  output = {}
  constraint_arr.each do |constraint_obj|
    if output[constraint_obj[:table]]
      output[constraint_obj[:table]] << constraint_obj
    else
      output[constraint_obj[:table]] = [constraint_obj]
    end
  end
  return output
end

#queries = [
#  {:class => "ClassA", :stmt => "user = User.where(:remember_token => token).first"},
#  {:class => "ClassB", :stmt => "User.find(session[\"user_id\"])\n"},
#  {:class => "ClassC", :stmt => "User.where(\"preferences.sms_email\" => address.strip).includes(:preference).first"}
#  {:class=>"User", :stmt=>"Dependency.where(predecessor_id: ids).destroy_all\n"}
#  {:class => "ContextsController", :stmt => "@context.todos.deferred.includes(Todo::DEFAULT_INCLUDES)"}
#  {:class => "ClassD" , :stmt => "Person.where(id: member_ids, rejected: true).where.not(email: nil, phone: 123, address: nil)"},
#  {:class => "ClassE", :stmt => "Tracker.where(:id => tracker_id_was, :default_status_id => status_id_was).where.not(:user_id => nil, :folder_id => nil).any?"}
#  {:class => "ClassF", :stmt => "Post.with_deleted.find_by(id: target_id)"}
#]


app_name = ARGV[0].strip

queries = load_queries(app_name)
processed_queries = process_queries(queries) 

constraints = load_constraints(app_name)
processed_constraints = process_constraints(constraints)

query_metadata = {
  :num_total => 0,
  :num_find => 0,
  :num_has_distinct => 0,
  :num_has_filters => 0,
  :num_raw_sql => 0
}

processed_queries.each do |query_obj|
  query_metadata[:num_total] += 1
  query_metadata[:num_find] += 1 if query_obj[:is_find]
  query_metadata[:num_has_distinct] += 1 if query_obj[:has_distinct]
  query_metadata[:num_has_filters] += 1 if !(query_obj[:filter_fields].empty?)
  query_metadata[:num_raw_sql] += 1 if query_obj[:only_raw_sql]
end
  

puts "=== ANALYZER OUTPUT FOR APP #{app_name} ==="
puts "query_metadata:" 
pp query_metadata

output = {
  :filter_on_unique_with_distinct_total => [],
  :filter_on_unique_with_distinct_no_index => [],
  :filter_on_inclusion_field => [],
  :not_null_filter_on_field_with_presence_total => [],
  :not_null_filter_on_field_with_presence_no_index => [],
  :filter_on_unique_with_no_limit_total => [],
  :filter_on_unique_with_no_limit_no_index => []
}

processed_queries.each do |query_obj|
  object_type = query_obj[:object_type]
  next if !processed_constraints[object_type]

  # check for filtering on uniqueness fields + distinct
  #   also later add check for whether unique index exists in DB,
  #   as this may negate performance benefit of removing distinct
  processed_constraints[object_type].select {|constraint| constraint[:type] == :uniqueness}.each do |constraint|
    if !query_obj[:filter_fields].empty? and 
        (Array(constraint[:fields]) - query_obj[:filter_fields]).empty? and
        query_obj[:has_distinct]
      output[:filter_on_unique_with_distinct_total] << {:query => query_obj, :constraint => constraint}
      if !constraint[:exists_in_db] 
        output[:filter_on_unique_with_distinct_no_index] << {:query => query_obj, :constraint => constraint}
      end
    end
  end 

  # check for filtering on inclusion fields
  processed_constraints[object_type].select {|constraint| constraint[:type] == :inclusion}.each do |constraint|
    if !query_obj[:filter_fields].empty? and 
        query_obj[:filter_fields].include? constraint[:fields]
      output[:filter_on_inclusion_field] << {:query => query_obj, :constraint => constraint}
    end
  end 

  # check for not null filtering on field with presence constraint
  processed_constraints[object_type].select {|constraint| constraint[:type] == :presence}.each do |constraint|
    if !query_obj[:not_null_fields].empty? and 
        query_obj[:not_null_fields].include? constraint[:fields]
      output[:not_null_filter_on_field_with_presence_total] << {:query => query_obj, :constraint => constraint}
      if !constraint[:exists_in_db]
        output[:not_null_filter_on_field_with_presence_no_index] << {:query => query_obj, :constraint => constraint}
      end
    end
  end 
  
  # check for only one record exists and no limit 1
  processed_constraints[object_type].select {|constraint| constraint[:type] == :uniqueness}.each do |constraint|
    if !query_obj[:filter_fields].empty? and 
        (Array(constraint[:fields]) - query_obj[:filter_fields]).empty? and
        !query_obj[:has_limit]
      output[:filter_on_unique_with_no_limit_total] << {:query => query_obj, :constraint => constraint}
      if !constraint[:exists_in_db]
        output[:filter_on_unique_with_no_limit_no_index] << {:query => query_obj, :constraint => constraint}
      end
    end
  end 
end

puts "analyzer_output_count:" 
pp output.map {|k,v| [k, v.size]}.to_h
puts "analyzer_output_raw:" 
pp output
