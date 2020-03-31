require 'rails_best_practices'
require 'yard'
require 'open3'
require 'active_support'
require 'pg_query'
require './types.rb'
require './pg_extension.rb'


MULTI_QUERY_METHODS = %w[where pluck distinct eager_load from group having includes joins left_outer_joins limit offset order preload readonly reorder select reselect select_all reverse_order unscope find_each rewhere].freeze
SINGLE_QUERY_METHODS = %w[find find! take take! first first! last last! find_by find_by!].freeze

# global variable; avoid passing as parameter...
$schema = nil
def tablename_plural?(name)
	name[0]==name[0].downcase
end
def tablename_pluralize(name)
	tablename_plural?(name) ? name : name.downcase.pluralize
end
def tablename_singular?(name)
	name[0]==name[0].upcase
end
def tablename_singularize(name)
	tablename_singular?(name)? name : name.capitalize.singularize
end

def is_valid_table?(table)
	$schema.each do |t|
		if table.to_s == t[:class_name].to_s or tablename_singularize(table.to_s) == t[:class_name].to_s
			return true
		end
	end
	return false
end

def find_table_in_schema(table)
	$schema.each do |t|
		if table.to_s == t[:class_name].to_s or tablename_singularize(table.to_s) == t[:class_name].to_s
			return t
		end
	end	
	puts "Table #{table} (#{tablename_singularize(table)}) cannot be found in schema!"
	puts "schema: "
	$schema.each do |t|
		puts "\t #{t[:class_name]} --> #{table.to_s == t[:class_name].to_s} #{t.inspect}"
	end
	exit
	return nil
end

def extract_query(ast)
  node = ast.type == :list ? ast[0] : ast
  if node.type == :assign and node[1].type == :call
    return node[1] 
  elsif node.type == :call or node.type == :fcall
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

def get_filters(node)
  return [] if node == nil

	#pp node
  if node.type == :call and node[2].type == :ident and (node[2][0] == "where" or node[2][0] == "not" or node[2][0] == "find_by") 
    return get_filters(node[0]) + extract_fields_from_args(node[3]) 
  elsif node.type == :call
    return get_filters(node[0])
  elsif node.type == :fcall
    return extract_fields_from_args(node[1]) 
  else
    return []
  end
end

def find_first_string_in_node(node)
	if !node.class.method_defined? "type"
		return ""
	end
	if node.type == :string_content
		return node.source
	end
	node.each do |child|
		x = find_first_string_in_node(child)
		if !x.nil?
			return x
		end
	end
	""
end

def find_string_from_param(node)
	begin
		open('__temp_ruby_code_buffer.rbout', 'w') { |f|
  		f.puts "puts #{node.source}"
		}
		stdout, stderr, status = Open3.capture3("ruby __temp_ruby_code_buffer.rbout")
		return stdout
	rescue
		""
	end
end

def find_first_symbol_in_node(node)
	if !node.class.method_defined? "type"
		return ""
	end
	if node.type == :symbol_literal
		return node[0][0]
	end
	node.each do |child|
		x = find_first_symbol_in_node(child)
		if !x.nil?
			return x
		end
	end
	""
end

def parse_partial_predicate(query, base_table)
	base_table = tablename_pluralize(base_table)
	sql = "SELECT #{base_table}.id FROM #{base_table} WHERE #{query}"
	if query.start_with?("SELECT")
		sql = query
	elsif query.start_with?("INNER JOIN") or query.start_with?("LEFT") or query.start_with?("JOIN")
		sql = "SELECT #{base_table}.id FROM #{base_table} #{query}"
	end
	begin
		cols = PgQueryExtension.setup(PgQuery.parse(sql)).get_all_columns
		return cols.map{ |x| QueryColumn.new(x[:table].nil? ? base_table : x[:table], x[:column]) }
	rescue => error
		puts "QUERY \"#{sql}\" CANNOT PARSE!"
		puts error
	end
	[]
end

# return both a string and a list of QueryPredicate 
def extract_query_string_from_param(node, base_table)
	return "",[] if node == nil or node.type != :arg_paren 
	preds = []
  if node[0][0].type == :list
		fields = extract_fields_from_args(node)
		fields.each do |field|
			preds << QueryPredicate.new(QueryColumn.new(base_table, field[:column]), '=', '?')
		end
		return fields.map{ |x| "#{tablename_singularize(base_table)}.#{x[:column]}=?" }.join(' AND '), preds
	elsif node[0][0].type == :array
		array_node = node[0][0]
		if array_node[0][0].type == :binary
			# for find_by_sql case
			sql = find_string_from_param(array_node[0][0])
		else
			sql = find_string_from_param(node[0][0][0])
		end
		preds = parse_partial_predicate(sql, base_table)
		return sql, preds
  elsif node[0][0].type == :string_literal or node[0][0].type == :call 
		sql = find_first_string_in_node(node[0][0])
		preds = parse_partial_predicate(sql, base_table)
		return sql, preds
	end
end



# return SQL string, QueryPredicate list, and the new base table (e.g., user.project returns Project)
def extract_query_string_from_call(call_ident, arg_node, base_table)
	retp = extract_query_string_from_param(arg_node, base_table)
	str_param = ""
	components = []
	if !retp.nil?
		str_param, components = retp
	end
	ret_str = str_param
	node = call_ident
	associations = find_table_in_schema(base_table).associations

	# where
	if ["where", "find", "rewhere", "find_by"].include?(node[0].to_s)
		ret_str = " WHERE #{str_param}"

	# find_by_sql
	elsif node[0] == "find_by_sql"
		# concat all strings together
		ret_str = str_param


	# find_by ??
	elsif node[0].to_s.start_with?("find_by")
		node[0].to_s.sub!("find_by_","").split('_and_').each do |column|
			components << QueryPredicate.new(QueryColumn.new(base_table, column), '=', '?')
			ret_str += " WHERE #{column} = ?"
		end
	
	# explicit join or inexplicit join via association
	elsif ["joins","left_outer_joins","includes","eager_load","preload"].include?(node[0].to_s) or associations.select { |ax| ax[:field]==node[0].to_s }.length > 0
		is_explicit_join = ["joins","left_outer_joins","includes","eager_load","preload"].include?(node[0].to_s)
		if str_param.blank?
			column_symb = is_explicit_join ? find_first_symbol_in_node(node) : node[0].to_s
			assoc = associations.select { |ax| ax[:field]==column_symb }
			if !column_symb.nil? and assoc.length > 0
				assoc = assoc[0]
				assoc_db_table = tablename_pluralize(assoc[:class_name])
				base_db_table = tablename_pluralize(base_table)
				pk = assoc[:rel]=="has_many"? "#{base_db_table}.id" : "#{assoc_db_table}.id"
				fk = assoc[:rel]=="has_many"? "#{assoc_db_table}.#{base_db_table.singularize}_id" : "#{base_db_table}.#{assoc_db_table.singularize}_id"
				ret_str = " #{node[0]=='joins'? 'INNER':'LEFT OUTER'} JOIN #{assoc_db_table} ON #{pk} = #{fk}" 
				components << QueryPredicate.new(QueryColumn.new(base_db_table, assoc[:rel]=="has_many"? pk : fk), '=', QueryColumn.new(assoc_db_table, assoc[:rel]=="has_many"? fk : pk))
				if !is_explicit_join
					base_table = assoc[:class_name]
				end
			end
		else
			ret_param = str_param
		end

	# order
	elsif node[0] == "order" or node[0] == "reorder"
		if str_param.blank?
			column_symb = find_first_symbol_in_node(node)
			if !column_symb.nil?
				ret_str = " ORDER BY #{column_symb}"
				components << QueryComponent.new(base_table, column_symb)
			end
		else
			ret_str = " ORDER BY #{str_param}"
		end

	# group
	elsif node[0] == "group"
		if str_param.blank?
			column_symb = find_first_symbol_in_node(node)
			if !column_symb.nil?
				ret_str = " GROUP BY #{column_symb}"
				components << QueryComponent.new(base_table, column_symb)
			end
		else
			ret_str = " GROUP BY #{str_param}"
		end

	# first
	elsif ['first','first!','exists','exists?','take'].include?node[0].to_s
		ret_str = " LIMIT 1"
		components << QueryComponent.new(base_table, 1)

	# pluck
	#elsif ['pluck', 'select'].include?node[0].to_s
	

	end
	components.map { |x| 
		x.ruby_meth = node[0].to_s 
		x }
	
	return ret_str, components, base_table
end

def convert_to_query_string(node, base_table)
	if node.type == :call and node[2].type == :ident
		sql1, components1, t = convert_to_query_string(node[0], base_table)
		sql2, components2, t = extract_query_string_from_call(node[2], node[3], t)
		base_table = t
		#puts "sql = #{sql1}, #{sql2}"
		#puts "pred = #{preds1}, #{preds2}"
		return sql1+sql2, components1+components2, base_table
	elsif node.type == :fcall and node[0].type == :ident
		return extract_query_string_from_call(node[0], node[1], base_table)
	end
	return "",[],base_table
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


def derive_metadata(raw_queries, schema)
  output = []
	$schema = schema
  raw_queries.each do |raw_query|
    begin 
      ast = YARD::Parser::Ruby::RubyParser.parse(raw_query.stmt).root 
    rescue  
      next
    end
		base_table = raw_query[:caller_class_lst].length==0 ? raw_query[:class]: raw_query[:caller_class_lst][0][:class]
		if !is_valid_table?(base_table)
			puts "query = #{raw_query.stmt}"
			puts "Table #{base_table} does not exist!"
			next
		end

    query_node = extract_query(ast)
  	_node = ast.type == :list ? ast[0] : ast
		xx = convert_to_query_string(query_node, base_table)
		#puts "raw_query = #{raw_query.stmt}, base_table = #{base_table} "
		#if !xx[0].blank?
		#	puts "parsed: query = #{xx[0]}"
		#	puts "\tcomponents = #{(xx[1].map {|xxx| xxx.inspect.to_s}).join(', ')}"
		#else
		#	puts "query #{raw_query.stmt} cannot be handled"
		#end
  	#puts ""

    methods = get_all_methods(query_node)
		#puts "has_query? #{!query_node.blank?} methods = #{methods.inspect}"
		base_object_type = nil
    if !(base_object_type = infer_object_type(query_node))
      base_object_type = raw_query.class
		end

    filters = (methods.include?("where") or methods.include? ("find_by")) ? get_filters(query_node) : []
    filters += methods.include?("not") ? get_not_null(query_node) : []
    filters += methods.include?("joins") ? derive_filters_from_joins(query_node, base_object_type) : []
    filters += methods.include?("pluck") ? derive_pluck_values(query_node, base_object_type) : []
    filters.map! {|filter| filter[:table].blank? ? filter.merge({:table => base_object_type}) : filter}

    has_distinct = methods.include?("distinct") 
    has_limit = %w(find find_by first first! last last! first_or_create).any? {|method| methods.include?(method)} 

    meta = MetaQuery.new
    meta.raw_query = raw_query
    meta.methods = methods
    meta.base_object_type = base_object_type
    meta.filters = filters
    meta.has_distinct = has_distinct
    meta.has_limit = has_limit
	
		meta.components = xx[1]
		meta.sql = xx[0] 

    output << meta
  end

  return output
end

