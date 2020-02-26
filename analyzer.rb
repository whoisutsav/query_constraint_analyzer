require 'pp'
require 'rails_best_practices'
require 'yard'
require 'pg_query'
require 'active_support'

APPS_DIR = "/Users/utsavsethi/workspace/apps" 
CONSTRAINT_ANALYZER_DIR = "/Users/utsavsethi/workspace/data-format-project/formatchecker/constraint_analyzer"

# return array of all statements containing queries in format
# [{:class => "CLASSNAME", :stmt => "stmt containing query"}]
def load_queries(app_name)
  output_file = "/Users/utsavsethi/tmp/query_output_#{app_name}"
  app_dir = File.join(APPS_DIR, "/#{app_name}") 
  if !File.exist?(output_file)
    `cd #{app_dir} && echo "PrintQueryCheck: { output_filename: \"#{output_file}\"}" &> ./config/rails_best_practices.yml && rails_best_practices . -c ./config/rails_best_practices.yml`
  end
  return Marshal.load(File.binread(output_file)) 
end

def load_constraints(app_name)
  output_file = "/Users/utsavsethi/tmp/constraint_output_#{app_name}"
  app_dir = File.join(APPS_DIR, "/#{app_name}") 
  if !File.exist?(output_file)
    `cd #{CONSTRAINT_ANALYZER_DIR} && ruby main.rb -a #{app_dir} --dump-constraints #{output_file}` 
  end
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
    where_str = extract_string(node[0][0]).gsub(/\n/, "")
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
    
    base_object_type = infer_object_type(query)
    is_find = query_methods.include?("find")

    raw_filter_fields = (query_methods.include?("where") or query_methods.include? ("find_by")) ? get_filter_fields(query) : []
    raw_filter_fields += query_methods.include?("not") ? get_not_null(query) : []
    raw_filter_fields += query_methods.include?("joins") ? derive_filters_from_joins(query, base_object_type) : []
    raw_filter_fields += query_methods.include?("pluck") ? derive_pluck_values(query, base_object_type) : []

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
   {:class => "ClassK", :stmt => "ChildTheme.where(parent_theme_id: theme_id).distinct.pluck(:child_theme_id)"},
]


#str = "editor.id > 0 AND editor.id != author.id AND post_id < ? or project.id = 3 and member_id IS NOT null and issue.user_id is null"
#str = "\#{Member.table_name}.project_id = ?" 
#where_sql_filters(str)

app_name = ARGV[0]

queries = load_queries(app_name)
processed_queries = process_queries(queries) 

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
  :filter_on_unique_with_no_limit_no_index => []
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
      if filters.select{|f| f[:is_not_null]}.map{|f| f[:column]}.include? constraint[:columns]
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
end

puts "analyzer_output_count:" 
pp output.map {|k,v| [k, v.size]}.to_h
puts "analyzer_output_raw:" 
pp output
