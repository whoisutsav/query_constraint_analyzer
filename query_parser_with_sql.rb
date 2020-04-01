require 'rails_best_practices'
require 'yard'
require 'open3'
require 'active_support'
require 'pg_query'
require './types.rb'
require './pg_extension.rb'
require './query_parser.rb'

# global variable; avoid passing as parameter...
$schema = nil
def tablename_plural?(name)
	name[0]==name[0].downcase
end
def tablename_pluralize(name)
	tablename_plural?(name) ? name : class_str_to_table(name) 
end
def tablename_singular?(name)
	name[0]==name[0].upcase
end
def tablename_singularize(name)
	tablename_singular?(name)? name : table_str_to_class(name) 
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
	#puts "Table #{table} (#{tablename_singularize(table)}) cannot be found in schema!"
	#puts "schema: "
	#$schema.each do |t|
	#	puts "\t #{t[:class_name]} --> #{table.to_s == t[:class_name].to_s} #{t.inspect}"
	#end
	#exit
	return nil
end

def get_fields_and_tables_for_query(components)
	fields = []
	components.each do |component|
		if component.is_a?(QueryColumn)
				fields << component
		elsif component.is_a?(QueryPredicate)
			if component.lh.is_a?(QueryColumn)
				fields << component.lh
			end
			if component.rh.is_a?(QueryColumn)
				fields << component.rh
			end
		end
	end
	fields.uniq { |f| [f.table, f.column] }	
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
		return node[0][0].source
	end
	node.each do |child|
		x = find_first_symbol_in_node(child)
		if !x.nil?
			return x
		end
	end
	""
end

def parse_partial_predicate(query, table, call_ident)
	base_table = tablename_pluralize(table)
	sql = "SELECT #{base_table}.id FROM #{base_table} WHERE #{query}"
	if query.start_with?("SELECT")
		sql = query
	elsif query.start_with?("INNER JOIN") or query.start_with?("LEFT") or query.start_with?("JOIN")
		sql = "SELECT #{base_table}.id FROM #{base_table} #{query}"
	elsif ["order", "reorder"].include?call_ident[0].to_s and !query.start_with?("ORDER")
		sql = "SELECT #{base_table}.id FROM #{base_table} ORDER BY #{query}"
	end
	begin
		cols = PgQueryExtension.setup(PgQuery.parse(sql)).get_all_columns
		return cols.map{ |x| QueryColumn.new(x[:table].nil? ? base_table : x[:table], x[:column]) }
	rescue => error
		puts "QUERY \"#{sql}\" CANNOT PARSE! orig sql = #{query}"
		puts error
	end
	[]
end

# return both a string and a list of QueryPredicate 
def extract_query_string_from_param(call_ident, node, base_table)
	return "",[] if node == nil or node.type != :arg_paren 
	preds = []
  if node[0][0].type == :list
		fields = extract_fields_from_args(node)
		fields.each do |field|
			preds << QueryPredicate.new(QueryColumn.new(base_table, field[:column]), '=', '?')
		end
		return fields.map{ |x| "#{tablename_pluralize(base_table)}.#{x[:column]}=?" }.join(' AND '), preds
	elsif node[0][0].type == :array
		array_node = node[0][0]
		if array_node[0][0].type == :binary
			# for find_by_sql case
			sql = find_string_from_param(array_node[0][0])
		else
			sql = find_string_from_param(node[0][0][0])
		end
		preds = parse_partial_predicate(sql, base_table, call_ident)
		return sql, preds
  elsif node[0][0].type == :string_literal or node[0][0].type == :call 
		sql = find_first_string_in_node(node[0][0])
		preds = parse_partial_predicate(sql, base_table, call_ident)
		return sql, preds
	end
end

def prev_contains_where(prev_state)
	prev_state[:prev_calls].any? { |x| ["where", "find", "rewhere", "find_by", "find_by_sql"].include?(x) }
end

# return SQL string, QueryPredicate list, and the new state
# state contains base_table (e.g., user.project returns Project), and previous calls
def extract_query_string_from_call(call_ident, arg_node, prev_state)
	base_table = prev_state[:base_table]
	prev_calls = prev_state[:prev_calls]
	retp = extract_query_string_from_param(call_ident, arg_node, base_table)
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
		ret_str = " #{prev_contains_where(prev_state) ? 'AND' : 'WHERE'} #{str_param}"

	# find_by_sql
	elsif node[0] == "find_by_sql"
		# concat all strings together
		ret_str = " " + str_param


	# find_by ??
	elsif node[0].to_s.start_with?("find_by")
		connect = prev_contains_where(prev_state) ? 'AND' : 'WHERE'
		node[0].to_s.sub!("find_by_","").split('_and_').each do |column|
			components << QueryPredicate.new(QueryColumn.new(base_table, column), '=', '?')
			ret_str += " #{connect} #{column} = ?"
			connect = 'AND'
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
				ret_str = " #{node[0]=='joins'? ' INNER':' LEFT OUTER'} JOIN #{assoc_db_table} ON #{pk} = #{fk}" 
				components << QueryPredicate.new(QueryColumn.new(base_db_table, assoc[:rel]=="has_many"? pk : fk), '=', QueryColumn.new(assoc_db_table, assoc[:rel]=="has_many"? fk : pk))
				if !is_explicit_join
					base_table = assoc[:class_name]
				end
			end
		else
			ret_str = " " + str_param
		end

	# order
	elsif node[0] == "order" or node[0] == "reorder"
		if str_param.blank?
			column_symb = find_first_symbol_in_node(arg_node)
			if !column_symb.nil?
				ret_str = " ORDER BY #{column_symb}"
				components << QueryColumn.new(base_table, column_symb)
			end
		else
			ret_str = " ORDER BY #{str_param}"
		end

	# group
	elsif node[0] == "group"
		if str_param.blank?
			column_symb = find_first_symbol_in_node(arg_node)
			if !column_symb.nil?
				ret_str = " GROUP BY #{column_symb}"
				components << QueryColumn.new(base_table, column_symb)
			end
		else
			ret_str = " GROUP BY #{str_param}"
		end

	# first
	elsif ['first','first!','exists','exists?','take'].include?node[0].to_s
		ret_str = " LIMIT 1"
		#components << QueryComponent.new(base_table, 1)

	# pluck
	#elsif ['pluck', 'select'].include?node[0].to_s
	

	end
	components.map { |x| 
		x.ruby_meth = node[0].to_s 
		x }
	
	prev_state[:base_table] = base_table
	prev_state[:prev_calls] << node[0].to_s
	return ret_str, components, prev_state 
end

def convert_to_query_string(node, prev_state)
	base_table = prev_state[:base_table]
	if node.type == :call and node[2].type == :ident
		sql1, components1, state = convert_to_query_string(node[0], prev_state)
		sql2, components2, state = extract_query_string_from_call(node[2], node[3], state)
		#puts "sql = #{sql1}, #{sql2}"
		#puts "pred = #{preds1}, #{preds2}"
		return sql1+sql2, components1+components2, state 
	elsif node.type == :fcall and node[0].type == :ident
		return extract_query_string_from_call(node[0], node[1], prev_state)
	end
	return "",[],prev_state
end

# try to parse the complete query string and get all the missing columns
def post_process_sql_string(sql, base_table)
	base_table = tablename_pluralize(base_table)
	sql = "SELECT #{base_table}.id FROM #{base_table} #{sql}"
	begin
		cols = PgQueryExtension.setup(PgQuery.parse(sql)).get_all_columns
		return cols.map{ |x| QueryColumn.new(x[:table].nil? ? base_table : x[:table], x[:column]) }
	rescue => error
		puts "-------"
		puts "QUERY \"#{sql}\" CANNOT PARSE! orig sql = #{sql}"
		puts error
		puts "-------\n"
	end
	[]
end

def parse_one_query(raw_query)
	begin 
    ast = YARD::Parser::Ruby::RubyParser.parse(raw_query.stmt).root 
  rescue  
   	return nil 
  end
	base_table = raw_query[:caller_class_lst].length==0 ? raw_query[:class]: raw_query[:caller_class_lst][0][:class]
	if !is_valid_table?(base_table)
		return nil	
	end

  query_node = extract_query(ast)
	sql, components = convert_to_query_string(query_node, {:base_table => base_table, :prev_calls => []})
	components += post_process_sql_string(sql, base_table)

	fields = get_fields_and_tables_for_query(components)

	methods = get_all_methods(query_node)
	base_object_type = nil
  if !(base_object_type = infer_object_type(query_node))
    base_object_type = raw_query.class
	end

  has_distinct = methods.include?("distinct") 
  has_limit = %w(find find_by first first! last last! first_or_create).any? {|method| methods.include?(method)} 

  meta = MetaQuery2.new
  meta.raw_query = raw_query
  meta.has_distinct = has_distinct
  meta.has_limit = has_limit
	meta.fields = fields
	meta.sql = sql

	return meta
end

def derive_metadata_with_sql(raw_queries, schema)
  output = []
	$schema = schema
  raw_queries.each do |raw_query|
    
    meta = parse_one_query(raw_query) 
		
		if !meta.nil?
    	output << meta
		end
  end

  return output
end


def print_detail_with_sql(raw_queries, schema)
  output = []
	$schema = schema
  raw_queries.each do |raw_query|
		base_table = raw_query[:caller_class_lst].length==0 ? raw_query[:class]: raw_query[:caller_class_lst][0][:class]
		if !is_valid_table?(base_table)
			puts "query = #{raw_query.stmt}"
			puts "Table #{base_table} does not exist!"
			next
		end
    
		meta = parse_one_query(raw_query) 

		puts "raw_query = #{raw_query.stmt}, base_table = #{base_table} "
		if !meta.sql.blank?
			puts "\tparsed: query = #{meta.sql}"
			puts "\tcomponents = #{(meta.fields.map {|xxx| xxx.table+":"+xxx.column}).join(', ')}"
		else
			puts "\tquery cannot be handled"
		end
  	puts ""
	end
end
