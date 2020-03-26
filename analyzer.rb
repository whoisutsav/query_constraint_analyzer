require 'pp'
require 'rails_best_practices'
require 'yard'
require 'active_support'

#APPS_DIR = "" 
#CONSTRAINT_ANALYZER_DIR = ""
#TEMP_OUTPUT_DIR = ""
RAILS_BEST_PRACTICES_CMD = "rails_best_practices"

config = YAML.load_file('config.yml')
config.each do |key, value|
  if key == 'apps_dir' 
    APPS_DIR = value
  elsif key == 'constraint_analyzer_dir'
    CONSTRAINT_ANALYZER_DIR = value
  elsif key == 'temp_output_dir'
    TEMP_OUTPUT_DIR = value
  elsif key == 'rails_best_practices_cmd'
	RAILS_BEST_PRACTICES_CMD = value
  end
end

# return array of all statements containing queries in format
# [{:class => "CLASSNAME", :stmt => "stmt containing query"}]
def load_queries_and_scopes(app_name)
  query_output_file = "#{TEMP_OUTPUT_DIR}/query_output_#{app_name}"
  scope_output_file = "#{TEMP_OUTPUT_DIR}/scope_output_#{app_name}"
  app_dir = File.join(APPS_DIR, "/#{app_name}") 
  if !File.exist?(query_output_file) or !File.exist?(scope_output_file)
    `cd #{app_dir} && echo "PrintQueryCheck: { output_filename_query: \"#{query_output_file}\", output_filename_scope: \"#{scope_output_file}\"}" &> ./config/rails_best_practices.yml && #{RAILS_BEST_PRACTICES_CMD} . -c ./config/rails_best_practices.yml`
  end
  return Marshal.load(File.binread(query_output_file)), Marshal.load(File.binread(scope_output_file)) 
end

def load_constraints(app_name)
  constraint_output_file = "#{TEMP_OUTPUT_DIR}/constraint_output_#{app_name}"
  app_dir = File.join(APPS_DIR, "/#{app_name}") 
  if !File.exist?(constraint_output_file)
    `cd #{CONSTRAINT_ANALYZER_DIR} && ruby main.rb -a #{app_dir} --dump-constraints #{constraint_output_file}` 
  end
  return Marshal.load(File.binread(constraint_output_file))
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
  elsif node.type == :fcall || node.type == :vcall
    return [node[0][0]]
  elsif node.type == :ident
    return [node[0]]
  else
    return []
  end
end

def table_str_to_class(str)
  ActiveSupport::Inflector.camelize(ActiveSupport::Inflector.singularize(str)) 
end

def extract_string(node)
  if node.type == :string_literal 
    return node.source
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
      next if key == nil # TODO rare case that happens in gitlab, find better fix 
      table = key.rpartition(".")[0]
      field = key.rpartition(".")[2] 
      output << {:table => table, :column => field, :is_not_null => true}
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

      hash_key = child[1].type == :hash ? extract_string(child[1][0][0]) : nil 

      if hash_key != nil
        table = table_str_to_class(key)
        field = hash_key
      else    
        table = key.rpartition(".")[0]
        field = key.rpartition(".")[2] 
      end
      output << {:table => table, :column => field, :is_not_null => false} 
    end
  elsif node[0][0].type == :string_literal
    where_str = extract_string(node[0][0]).gsub(/\n/, "").gsub(/"/,"")
    raw_filters = where_str.split(/\s+and\s+|\s+or\s+/i).map(&:strip)
    
    raw_filters.each do |filter_str|
      is_not_null = filter_str.match(/is not null/i) ? true : false
      table, column = filter_str.split(/<|<=|>|>=|!=|=|\s+is not\s+|\s+is\s+/i)[0].rpartition('.').values_at(0,2)
      match_data = table.match(/{(.*)\.table_name}/)
      table = match_data ? match_data[1] : table
      column = column.strip
      output << {:table => table, :column => column, :is_not_null => is_not_null}
    end
  end

  return output.uniq
end

# TODO - handle multiple joins
def derive_filters_from_joins(node, base_object_type)
  return [] if node == nil or base_object_type == nil 

  if node.type == :call and node[2].type == :ident and node[2][0] == "joins"
    output = []
    fk_field = base_object_type.downcase + "_id" 
    if node[3][0].type == :list
      node[3][0].each do |child|
        next if !child or child.type != :symbol_literal
        join_table = table_str_to_class(extract_string(child))
        output << {:table => join_table, :column => fk_field, :is_not_null => false}
      end
    end
    return output
  elsif node.type == :call
    return derive_filters_from_joins(node[0], base_object_type)
  else
    return []
  end
end

def derive_pluck_values(node, base_object_type)
  return [] if node == nil or base_object_type == nil 

  if node.type == :call and node[2].type == :ident and node[2][0] == "pluck"
    output = []
    if node[3][0].type == :list
      node[3][0].each do |child|
        next if !child or child.type != :symbol_literal
        plucked_value = extract_string(child)
        table = table_str_to_class(plucked_value.rpartition(".")[0])
        field = plucked_value.rpartition(".")[2] 
        output << {:table => table, :column => field, :is_not_null => false}
      end
    end
    return output
  elsif node.type == :call
    return derive_filters_from_joins(node[0], base_object_type)
  else
    return []
  end
end

def get_filter_fields(node)
  return [] if node == nil

  if node.type == :call and node[2].type == :ident and (node[2][0] == "where" or node[2][0] == "not" or node[2][0] == "find_by") 
    return get_filter_fields(node[0]) + extract_fields_from_args(node[3]) 
  elsif node.type == :call
    return get_filter_fields(node[0])
  elsif node.type == :fcall
    return extract_fields_from_args(node[1]) 
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


def preprocess_queries(query_arr, scope_hash={})
  output_arr = []
  query_arr.each do |query_obj|
    class_name = query_obj[:class]
    begin 
      ast = YARD::Parser::Ruby::RubyParser.parse(query_obj[:stmt]).root 
    rescue  
      next
    end
    query_node = extract_query(ast) 
    next if !query_node
    query_methods = get_all_methods(query_node)
	base_object_type = infer_object_type(query_node)

    #Replace any methods with scopes
    #puts "==================="
    #puts "original query source: #{query_node.source}" 
    found_scopes = scope_hash[base_object_type] ? query_methods & scope_hash[base_object_type].keys : []
    if !found_scopes.empty?
      #puts "has scopes: #{found_scopes}"
      query_sources = found_scopes.sort_by(&:length).reverse.inject([query_node.source]) do |query_sources, found_scope|
        #puts "scope: #{found_scope}, sources: #{scope_hash[base_object_type][found_scope]}"
        output = []
        query_sources.each do |query_source|
          scope_hash[base_object_type][found_scope].each do |scope_source|
            output << query_source.gsub(/#{found_scope}(?:\(.*?\))?/, scope_source)  
          end 
        end
        output
      end
      #puts "new query sources: #{query_sources}" 
      query_sources.each do |query_source|
        output_arr << {:class => class_name, :stmt => query_source}
      end
    else
      output_arr << query_obj
    end 
  end

  scope_hash.each do |class_name, scopes|
    scopes.each do |scope_name, scope_sources|
      scope_sources.each do |scope_source|
        output_arr << {:class => class_name, :stmt => scope_source}
      end
    end
  end

  output_arr
end

# parse queries and add metadata fields to query info object
def process_queries(query_arr)
  output = []
  query_arr.each do |query_obj|
    begin 
      ast = YARD::Parser::Ruby::RubyParser.parse(query_obj[:stmt]).root 
    rescue  
      next
    end
    query_node = extract_query(ast) 
    query_methods = get_all_methods(query_node)
	base_object_type = nil
    if !(base_object_type = infer_object_type(query_node))
      base_object_type = query_obj[:class]
	end

    is_find = query_methods.include?("find")

    raw_filter_fields = (query_methods.include?("where") or query_methods.include? ("find_by")) ? get_filter_fields(query_node) : []
    raw_filter_fields += query_methods.include?("not") ? get_not_null(query_node) : []
    raw_filter_fields += query_methods.include?("joins") ? derive_filters_from_joins(query_node, base_object_type) : []
    raw_filter_fields += query_methods.include?("pluck") ? derive_pluck_values(query_node, base_object_type) : []

    processed_filter_fields = {}
    raw_filter_fields.each do |filter|
      object_type = filter[:table].blank? ? base_object_type : table_str_to_class(filter[:table])
      if processed_filter_fields[object_type]
        processed_filter_fields[object_type] << filter
      else
        processed_filter_fields[object_type] = [filter]
      end
    end 
    has_distinct = query_methods.include?("distinct") 
    has_limit = %w(find find_by first first! last last! first_or_create).any? {|method| query_methods.include?(method)} 
    only_raw_sql = (raw_filter_fields.empty? and (query_methods.include?("where") or query_methods.include?("find_by_sql"))) 

    output << query_obj.dup.merge({
      :query_methods => query_methods,
      :base_object_type => base_object_type,
      :is_find => is_find,
      :raw_filter_fields => raw_filter_fields,
      :processed_filter_fields => processed_filter_fields,
      :has_distinct => has_distinct,
      :has_limit => has_limit,
      :only_raw_sql => only_raw_sql
    })
  end

  return output
end

MULTI_QUERY_METHODS = %w[where pluck distinct eager_load from group having includes joins left_outer_joins limit offset order preload readonly reorder select reselect select_all reverse_order unscope find_each rewhere].freeze
SINGLE_QUERY_METHODS = %w[find find! take take! first first! last last! find_by find_by!].freeze

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

def extract_scope_calls(node)
  return [] if node == nil
  if node.type == :call or node.type == :fcall
    method_list = get_all_methods(node)
	  if !((MULTI_QUERY_METHODS + SINGLE_QUERY_METHODS) & get_all_methods(node)).empty?
      return [node.source]
    else
      return []
    end
  else
    output = []
    node.children.each do |child|
      output += extract_scope_calls(child)
    end
    return output
  end 

end

def process_scopes(scopes)
  output = {}
  # Phase 1: extract calls
  scopes.each do |key, source|
    class_name,scope_name = key.partition("-").values_at(0,2) 
	  begin
      ast = YARD::Parser::Ruby::RubyParser.parse(source).root
	  rescue
      next
    end
    valid_calls = []
    if ast.children.first.type == :fcall || ast.children.first.type == :call
      valid_calls += [ast.children.first.source] 
    elsif ast.children.last.type == :fcall or ast.children.last.type == :call 
      valid_calls += [ast.children.last.source]
    else
      valid_calls += extract_scope_calls(ast)
    end

    output[class_name] = {} if !output[class_name]
    output[class_name][scope_name] = valid_calls
  end

  # Phase 2: replace scope-within-scope calls
  replaced_output = {}
  output.each do |class_name, scopes|
    replaced_output[class_name] = {}
    scopes.each do |scope_name, sources|
      replaced_output[class_name][scope_name] = []
      sources.each do |call_source|
        ast = YARD::Parser::Ruby::RubyParser.parse(call_source).root 
        if ast.type == :list
          ast = ast[0]
        end
        processed_call_source = call_source
        referenced_scopes = get_all_methods(ast) & scopes.keys
        if !referenced_scopes.empty?
          referenced_scopes.each do |referenced_scope|
            # TODO - handle case where a referenced scope has multiple sources
            # (Right now we consider only the first source)
            processed_call_source = processed_call_source.gsub(referenced_scope, output[class_name][referenced_scope][0].to_s) 
          end
        end 
        replaced_output[class_name][scope_name] << processed_call_source
      end
    end
  end

  return replaced_output
end



#str = "editor.id > 0 AND editor.id != author.id AND post_id < ? or project.id = 3 and member_id IS NOT null and issue.user_id is null"
#str = "\#{Member.table_name}.project_id = ?" 
#where_sql_filters(str)

app_name = ARGV[0]

queries = [
#  {:class => "ClassA", :stmt => "user = User.where(:remember_token => token).first"},
#  {:class => "ClassB", :stmt => "User.find(session[\"user_id\"])\n"},
#  {:class => "ClassC", :stmt => "User.where(\"preferences.sms_email\" => address.strip).includes(:preference).first"},
#  {:class=>"User", :stmt=>"Dependency.where(predecessor_id: ids).destroy_all\n"},
#  {:class => "ContextsController", :stmt => "@context.todos.deferred.includes(Todo::DEFAULT_INCLUDES)"},
#  {:class => "ClassD" , :stmt => "Person.where(id: member_ids, rejected: true).where.not(email: nil, phone: 123, address: nil)"},
#  {:class => "ClassE", :stmt => "Tracker.where(:id => tracker_id_was, :default_status_id => status_id_was).where.not(:user_id => nil, :folder_id => nil).any?"},
#  {:class => "ClassF", :stmt => "Post.with_deleted.find_by(id: target_id)"},
#  {:class => "ClassG", :stmt => "Tracker.joins(projects: :enabled_modules).where(\"\#{Project.table_name}.status <> ?\", STATUS_ARCHIVED).where(:enabled_modules => {:name => 'issue_tracking'}).distinct.sorted"},
#  {:class => "ClassH", :stmt => "User.active.joins(:members, :cats).where(\"\#{Member.table_name}.project_id = ?\", id).distinct"},
#  {:class => "ClassJ", :stmt => "User.where(\"editor.id > 0 AND editor.id != author.id AND post_id < ? or project.id = 3 and member_id IS NOT null and issue.user_id is null\")"},
#  {:class => "ClassK", :stmt => "ChildTheme.where(parent_theme_id: theme_id).distinct.pluck(:child_theme_id)"},
#  {:class => "ClassL", :stmt => "User.where(\"username IS NOT NULL and created_at IS NOT NULL\")"} 
  {:class=>"ApiOpenidConnectAuthorization", :stmt=>"where(o_auth_application: app, user: user).all"},
]

scopes = []

queries,scopes = load_queries_and_scopes(app_name)
processed_scopes = process_scopes(scopes) 
preprocessed_queries = preprocess_queries(queries, processed_scopes) 
processed_queries = process_queries(preprocessed_queries) 

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
  query_metadata[:num_has_filters] += 1 if !(query_obj[:raw_filter_fields].empty?)
  query_metadata[:num_raw_sql] += 1 if query_obj[:only_raw_sql]
end
  

puts "=== ANALYZER OUTPUT FOR APP #{app_name} ==="
puts "query_metadata:" 
pp query_metadata

constraints = load_constraints(app_name)
processed_constraints = process_constraints(constraints)

output = {
  :filter_on_unique_with_distinct_total => [],
  :filter_on_unique_with_distinct_no_index => [],
  :filter_on_inclusion_field => [],
  :not_null_filter_on_field_with_presence_total => [],
  :not_null_filter_on_field_with_presence_no_index => [],
  :filter_on_unique_with_no_limit_total => [],
  :filter_on_unique_with_no_limit_no_index => [],
  :redundant_subquery => [],
}

processed_queries.each do |query_obj|
  query_obj[:processed_filter_fields].each do |object_type, filters|
    next if !processed_constraints[object_type] or filters.empty?

    # check for filtering on uniqueness fields + distinct
    #   also later add check for whether unique index exists in DB,
    #   as this may negate performance benefit of removing distinct
    processed_constraints[object_type].select {|constraint| constraint[:type] == :uniqueness}.each do |constraint|
      if (Array(constraint[:fields]) - filters.map{|f| f[:column]}).empty? and query_obj[:has_distinct]
        output[:filter_on_unique_with_distinct_total] << {:query => query_obj, :constraint => constraint}
        if !constraint[:exists_in_db] 
          output[:filter_on_unique_with_distinct_no_index] << {:query => query_obj, :constraint => constraint}
        end
      end
    end 

    # check for filtering on inclusion fields
    processed_constraints[object_type].select {|constraint| constraint[:type] == :inclusion}.each do |constraint|
      if filters.map{|f| f[:column]}.include? constraint[:fields]
        output[:filter_on_inclusion_field] << {:query => query_obj, :constraint => constraint}
      end
    end 

    # check for not null filtering on field with presence constraint
    processed_constraints[object_type].select {|constraint| constraint[:type] == :presence}.each do |constraint|
      if filters.select{|f| f[:is_not_null]}.map{|f| f[:column]}.include? constraint[:fields]
        output[:not_null_filter_on_field_with_presence_total] << {:query => query_obj, :constraint => constraint}
        if !constraint[:exists_in_db]
          output[:not_null_filter_on_field_with_presence_no_index] << {:query => query_obj, :constraint => constraint}
        end
      end
    end 
    
    # check for only one record exists and no limit 1
    processed_constraints[object_type].select {|constraint| constraint[:type] == :uniqueness}.each do |constraint|
      if (Array(constraint[:fields]) - filters.map{|f| f[:column]}).empty? and !query_obj[:has_limit]
        output[:filter_on_unique_with_no_limit_total] << {:query => query_obj, :constraint => constraint}
        if !constraint[:exists_in_db]
          output[:filter_on_unique_with_no_limit_no_index] << {:query => query_obj, :constraint => constraint}
        end
      end
    end 
  end

  # check for duplicate references
  downcased_singular_methods = query_obj[:query_methods].map{|str| ActiveSupport::Inflector.singularize(str).downcase}
  dupe_methods = downcased_singular_methods.select{|str| !(["join", "left_outer_join", "merge", "reference", "include", "not"] + MULTI_QUERY_METHODS + SINGLE_QUERY_METHODS).include?(str) and downcased_singular_methods.count(str) > 1}
  if !dupe_methods.empty?
    output[:redundant_subquery] << {:query => query_obj, :constraint => nil, :dupes => dupe_methods.uniq}
  end

end

puts "analyzer_output_count:" 
pp output.map {|k,v| [k, v.size]}.to_h
puts "analyzer_output_raw:" 
pp output
