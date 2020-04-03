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
$scopes = nil
$query_map = {}

def tablename_plural?(name)
	name[0]==name[0].downcase
end
def tablename_pluralize(name)
	return "" if name.nil? or name.length==0
	tablename_plural?(name) ? name : class_str_to_table(name) 
end
def tablename_singular?(name)
	name[0]==name[0].upcase
end
def tablename_singularize(name)
	return "" if name.nil? or name.length==0
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

def find_scope(table, meth)
	return $scopes[table].nil? ? nil : $scopes[table][meth]
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
		elsif component.is_a?(Hash) # placeholder for scope
			fields << component
		end
	end
	#fields.uniq { |f| [tablename_pluralize(f.table), f.column] }	
	fields
end

def resolve_scopes(query)
	more_fields = []
	remove_fields = []
	source = ""
	query.fields.each do |f|
		if f.is_a?(Hash)
			scope_query = find_scope(f[:class], f[:meth])
			more_fields += $query_map[scope_query].fields
			remove_fields << f
			source += "\n\tscope #{f[:class]}:#{scope_query[:method_name]}: #{scope_query.stmt}"
		end
	end
	query.fields = (query.fields - remove_fields + more_fields).uniq { |f| [tablename_pluralize(f.table), f.column] }	
	query.source += source
	if more_fields.length > 0
		puts "new source = #{query.source}"
		puts "fields = #{query.fields}"
		puts '"'
	end
	query
end

def extract_query(ast)
  node = ast.type == :list ? ast[0] : ast
  if node.type == :assign and node[1].type == :call
    return node[1] 
  elsif node.type == :call or node.type == :fcall
		return node 
	elsif node.type == :command 
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
	if !is_valid_node?(node)
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

def find_string_from_param(source)
	begin
		open('__temp_ruby_code_buffer.rbout', 'w') { |f|
  		f.puts "puts #{source}"
		}
		stdout, stderr, status = Open3.capture3("ruby __temp_ruby_code_buffer.rbout")
		return stdout
	rescue
		""
	end
end

def find_first_symbol_in_node(node)
	if !is_valid_node?(node)
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

# some adhoc method to fix issues in the sql query
def sql_query_fix(base_table, sql)
	# fix 1: #{Project.table_name} --> Project
	if sql.include?('#')
		sql = sql.gsub(/\#{(\W*)(\w+).table_name}/,'\2')
	end
	# fix 2: #{tablename} --> base_table
	if sql.include?('#')
		sql = sql.gsub(/\#{(\W*)table_name}/, base_table)
	end
	# fix 2: --> ?
	if sql.include?('#')
		sql = sql.gsub(/\#{[^}]+}/, '? ')
	end
	# fix 3: :lft --> ?
	if sql.include?(':')
		sql = sql.gsub(/:(\w+) /,'? ')
	end
	return sql
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
		return cols.map{ |x| QueryColumn.new(x[:table].nil? ? base_table : tablename_singularize(x[:table]), x[:column]) }
	rescue => error
		# puts "QUERY \"#{sql}\" CANNOT PARSE! orig sql = #{query}"
		# puts error
	end
	[]
end

# try to parse the complete query after collecting all sql componenets and get all the missing columns
def post_process_sql_string(sql, base_table)
	base_table = tablename_pluralize(base_table)
	sql = "SELECT #{base_table}.id FROM #{base_table} #{sql}"
	begin
		cols = PgQueryExtension.setup(PgQuery.parse(sql)).get_all_columns
		return cols.map{ |x| QueryColumn.new(x[:table].nil? ? base_table : tablename_singularize(x[:table]), x[:column]) }
	rescue => error
		# puts "-------"
		# puts "QUERY \"#{sql}\" CANNOT PARSE! orig sql = #{sql}"
		# puts error
		# puts "-------\n"
	end
	[]
end

# it's messy :( but here I need to set up the table correctly, e.g., joins(:issue => :project)
def extract_fields_from_args_for_join(base_table, node)
  return [] if node == nil
	output = []
	associations = find_table_in_schema(base_table).associations
	table = base_table
  if node.type == :list
    node.each do |child|
      next if !check_node_type(child, :assoc)
      child.each do |c|
        if c.type == :symbol_literal
          key = extract_string(c)
          if !key.nil?
						output << {:table => table, :column => key}
						assoc = associations.select { |ax| ax[:field]==key }
						if assoc.length > 0
							table = assoc[0][:class_name]
						end
          end 
        end
      end
    end
  end
  output
end

def is_query_predicate_func?(call_ident)
	call_ident[0].to_s.start_with?('find_by') or ["where","rewhere"].include?(call_ident[0].to_s)
end
def extract_query_string_from_list_node(call_ident, arg_node, base_table)
	preds = []
	if is_query_predicate_func?(call_ident)
		fields = extract_fields_from_args_for_where(arg_node)
		fields.each do |field|
			preds << QueryPredicate.new(QueryColumn.new(field[:table].blank? ? base_table : field[:table], field[:column]), '=', '?')
		end
		return fields.map{ |x| "#{tablename_pluralize(base_table)}.#{x[:column]}=?" }.join(' AND '), preds
	else
		fields = extract_fields_from_args_for_join(base_table, arg_node)
		return "", fields.map{ |x| QueryColumn.new(x[:table].blank? ? base_table : x[:table], x[:column])}
	end
end

def extract_query_string_from_array_node(call_ident, arg_node, base_table)
	preds = []
	if arg_node[0].nil? or arg_node[0][0].nil?
		return "",[]
	end
	sql = ""
	if arg_node[0][0].type == :binary
		# for cases of concatenating strings using '+'
		sql = find_string_from_param(sql_query_fix(base_table, arg_node[0][0].source))
	else
		sql = find_string_from_param(sql_query_fix(base_table, arg_node[0].source))
	end
	preds = parse_partial_predicate(sql, base_table, call_ident)
	return sql, preds
end

def extract_query_string_from_binary_node(call_ident, arg_node, base_table)
	sql = find_string_from_param(sql_query_fix(base_table, arg_node.source))
	preds = parse_partial_predicate(sql, base_table, call_ident)
	return sql, preds
end

def extract_query_string_from_string_node(call_ident, arg_node, base_table)
	sql = find_first_string_in_node(arg_node)
	sql = sql_query_fix(base_table, sql)
	preds = parse_partial_predicate(sql, base_table, call_ident)
	return sql, preds
end

def extract_query_string_from_symbol_node(call_ident, arg_node, base_table)
	preds = [QueryColumn.new(base_table, arg_node.source.gsub(/:/,''))]
	# arg_node[0].each do |n|
	# 	if is_valid_node?(n) && n.type == :symbol_literal
	# 		preds << QueryColumn.new(base_table, n.source.gsub(/:/,''))
	# 	end
	# end

	return "", preds
end

# return both a string and a list of QueryPredicate 
# only process string
def extract_query_string_from_param(call_ident, node, base_table)
	return "",[] if node == nil 
	arg_nodes = []
	if is_query_predicate_func?(call_ident)
		if node.type != :arg_paren 
			arg_nodes = [node]
		else
			arg_nodes = [node[0][0]]
		end
	else
		if check_node_type(node[0], :list)
			arg_nodes = node[0]
		end
	end
	sql,preds="",[]
	arg_nodes.each do |arg_node|
		next if !is_valid_node?(arg_node)
		ptype = arg_node.type
		s1,q1="",[]
		if ptype == :list
			s1,q1 = extract_query_string_from_list_node(call_ident, arg_node, base_table)
		elsif ptype == :array
			s1,q1 = extract_query_string_from_array_node(call_ident, arg_node, base_table)
		elsif ptype == :binary
			s1,q1 = extract_query_string_from_binary_node(call_ident, arg_node, base_table)
		elsif ptype == :string_literal || ptype == :call
			s1,q1 = extract_query_string_from_string_node(call_ident, arg_node, base_table)
		elsif ptype == :symbol_literal
			s1,q1 = extract_query_string_from_symbol_node(call_ident, arg_node, base_table)
		end
		sql += s1
		preds += q1
	end
	return sql,preds
end

def prev_contains_where(prev_state)
	prev_state[:prev_calls].any? { |x| ["where", "find", "rewhere", "find_by", "find_by_sql"].include?(x) }
end

# return SQL string, QueryPredicate list, and the new state
# state contains base_table (e.g., user.project returns Project), and previous calls
def extract_query_string_from_call(call_ident, arg_node, prev_state)
	base_table = prev_state[:base_table]
	prev_calls = prev_state[:prev_calls]
	node = call_ident
	associations = find_table_in_schema(base_table).associations
	table_schema = find_table_in_schema(base_table)
	str_param, components = extract_query_string_from_param(call_ident, arg_node, base_table)
	ret_str = str_param

	# where
	if ["where", "find", "rewhere", "find_by"].include?(node[0].to_s)
		ret_str = " #{prev_contains_where(prev_state) ? 'AND' : 'WHERE'} #{str_param}"

	# find_by_sql
	elsif node[0] == "find_by_sql"
		# concat all strings together
		ret_str = " " + str_param

	# find_by ??
	elsif node[0].to_s.start_with?("find_by")
		ret_str = str_param
		connect = prev_contains_where(prev_state) ? 'AND' : 'WHERE'
		node[0].to_s.sub!("find_by_","").split('_and_').each do |column|
			components << QueryPredicate.new(QueryColumn.new(base_table, column), '=', '?')
			ret_str += " #{connect} #{column} = ?"
			connect = 'AND'
		end

	# boolean field as filter
	#elsif table_schema.
	
	# explicit join or inexplicit join via association
	elsif ["joins","left_outer_joins","includes","eager_load","preload"].include?(node[0].to_s) or associations.select { |ax| ax[:field]==node[0].to_s }.length > 0
		is_explicit_join = ["joins","left_outer_joins","includes","eager_load","preload"].include?(node[0].to_s)
		if str_param.blank?
			columns = is_explicit_join ? get_fields_and_tables_for_query(components) : [QueryColumn(base_table, node[0].to_s)]
			ret_str = ""
			#puts "JOIN componenets = #{components}"
			components = []
			columns.each do |column|
				column_symb = column.column
				if !column.table.blank?
					associations = find_table_in_schema(column.table).associations
					base_table = column.table
				end
				assoc = associations.select { |ax| ax[:field]==column_symb }
				if assoc.length > 0
					assoc = assoc[0]
					assoc_db_table = tablename_pluralize(assoc[:class_name])
					base_db_table = tablename_pluralize(base_table)
					pk = assoc[:rel]=="has_many"? "#{base_db_table}.id" : "#{assoc_db_table}.id"
					fk = assoc[:rel]=="has_many"? "#{assoc_db_table}.#{base_db_table.singularize}_id" : "#{base_db_table}.#{assoc_db_table.singularize}_id"
					ret_str += " #{node[0]=='joins'? ' INNER':' LEFT OUTER'} JOIN #{assoc_db_table} ON #{pk} = #{fk}" 
					components << QueryPredicate.new(QueryColumn.new(base_table, assoc[:rel]=="has_many"? 'id' : "#{assoc_db_table.singularize}_id"), 
						'=', 
						QueryColumn.new(assoc[:class_name], assoc[:rel]=="has_many"? "#{base_db_table.singularize}_id" : 'id'))
				end
				#associations = find_table_in_schema(assoc[:class_name]).associations
				
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
			get_fields_and_tables_for_query(components).map{|xx| xx.column }.each do |column_symb|
				ret_str = " ORDER BY #{column_symb}"
			end
		else
			ret_str = " ORDER BY #{str_param}"
		end

	# group
	elsif node[0] == "group"
		if str_param.blank?
			get_fields_and_tables_for_query(components).map{|xx| xx.column }.each do |column_symb|
				ret_str = " GROUP BY #{column_symb}"
			end
		else
			ret_str = " GROUP BY #{str_param}"
		end

	# first
	elsif ['first','first!','exists','exists?','take'].include?node[0].to_s
		ret_str = " LIMIT 1"
		#components << QueryComponent.new(base_table, 1)

	# pluck
	elsif ['pluck', 'select'].include?node[0].to_s
		#componenets << QueryColumn()

	# scope...
	elsif find_scope(base_table, node[0].to_s)
		components << {:class => base_table, :meth => node[0].to_s}

	end
	components.map { |x| 
		if !x.is_a?(Hash)
			x.ruby_meth = node[0].to_s 
		end
		x }
	
	prev_state[:base_table] = base_table
	prev_state[:prev_calls] << node[0].to_s
	return ret_str, components, prev_state 
end

def convert_to_query_string(node, prev_state)
	base_table = prev_state[:base_table]
	if check_node_type(node, :call) and node.length>2 and node[2].type == :ident
		sql1, components1, state = convert_to_query_string(node[0], prev_state)
		sql2, components2, state = extract_query_string_from_call(node[2], node[3], state)
		#puts "sql = #{sql1}, #{sql2}"
		#puts "pred = #{components1}, #{components2}"
		return sql1+sql2, components1+components2, state 
	elsif check_node_type(node, :fcall) and node.length>0 and node[0].type == :ident
		return extract_query_string_from_call(node[0], node[1], prev_state)
	end
	return "",[],prev_state
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
	meta.source = raw_query.stmt
  meta.has_distinct = has_distinct
  meta.has_limit = has_limit
	meta.fields = fields
	meta.sql = sql

	$query_map[raw_query] = meta

	return meta
end

def first_pass(raw_queries)
	raw_queries.each do |raw_query|
    meta = parse_one_query(raw_query) 
  end
end

def second_pass(raw_queries)
	output = []
	raw_queries.each do |raw_query|
    meta = $query_map[raw_query]
		if !meta.nil?
			resolve_scopes(meta)
			output << meta
		end
	end
	output
end

def derive_metadata_with_sql(raw_queries, scopes, schema)
	output = []
	$schema = schema
	$scopes = scopes
	first_pass(raw_queries)
	return second_pass(raw_queries)
end


def print_detail_with_sql(raw_queries, scopes, schema)
  output = []
	$schema = schema
	$scopes = scopes
	succ_cnt = 0
	raw_queries.each do |raw_query|
		if raw_query[:method_name].blank?
			next
		end
		base_table = raw_query[:caller_class_lst].length==0 ? raw_query[:class]: raw_query[:caller_class_lst][0][:class]
		if !is_valid_table?(base_table)
			puts "query = #{raw_query.stmt} #{raw_query[:caller_class_lst]}"
			puts "Table #{base_table} does not exist!"
			next
		end
    
		puts "raw_query = #{raw_query.stmt}, base_table = #{base_table} "
		meta = parse_one_query(raw_query) 

		if meta.nil?
			next
		end

		ast = YARD::Parser::Ruby::RubyParser.parse(raw_query.stmt).root 
		query_node = extract_query(ast)
    methods = get_all_methods(query_node)
		base_object_type = nil
    if !(base_object_type = infer_object_type(query_node))
      base_object_type = raw_query.class
		end

    filters = (methods.include?("where") or methods.include? ("find_by")) ? get_filters(query_node) : []
    filters += methods.include?("not") ? get_not_null(query_node) : []
    filters += methods.include?("joins") ? derive_filters_from_joins(query_node, base_object_type) : []
    filters += methods.include?("pluck") ? derive_pluck_values(query_node, base_object_type) : []
    filters.map! {|filter| filter[:table].blank? ? filter.merge({:table => base_object_type}) : filter}
		
		#puts "\tfilters = #{filters.inspect}"
		if !meta.sql.blank?
			puts "\tparsed: query = #{meta.sql}"
			puts "\tcomponents = #{(meta.fields.map {|xxx| xxx.table+":"+xxx.column}).join(', ')}"

			if meta.fields.length > 1
				succ_cnt += 1
			end
		else
			puts "\tquery cannot be handled"
		end
		puts ""
		# if raw_query.stmt.start_with?("order(:name, :id)")
		# 	exit
		# end
	end
	puts "success: #{succ_cnt} / total #{raw_queries.length}"
end
