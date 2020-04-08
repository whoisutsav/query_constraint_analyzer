def table_str_to_class(str)
  ActiveSupport::Inflector.camelize(ActiveSupport::Inflector.singularize(str)) 
end

def class_str_to_table(str)
  ActiveSupport::Inflector.tableize(str) 
end

def canonicalize_classname(name)
  name = clean_prefix(name)
  if name.start_with?($app_name)
    name = name.gsub($app_name, '')
  end
  name
end

def clean_prefix(name)
  #name.include?('::') ? name.gsub(/[^:]+::/,'') : name
  name.demodulize
end
def tablename_plural?(name)
	name[0]==name[0].downcase
end
def tablename_pluralize(name)
	return "" if name.nil? or name.length==0 
	name = tablename_plural?(name) ? name : class_str_to_table(name) 
	clean_prefix(name)
end
def tablename_singular?(name)
	name[0]==name[0].upcase
end
def tablename_singularize(name)
	return "" if name.nil? or name.length==0
	name = tablename_singular?(name)? name : table_str_to_class(name) 
	canonicalize_classname(name)
end

# some adhoc method to fix issues in the sql query
def sql_query_fix(base_table, sql)
	# fix 0: #{tablename} --> base_table
	if sql.include?('#')
		sql = sql.gsub(/\#{(\W*)table_name}/, base_table)
	end
	# fix 1: #{xxx} --> ?
	if sql.include?('#')
		sql = sql.gsub(/\#{[^}]+}/, '? ')
	end
	# fix 1: :lft --> ?
	if sql.include?(':')
		sql = sql.gsub(/:(\w+) /,'? ')
	end
	return sql
end
