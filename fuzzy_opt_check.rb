require './query_parser.rb'

# This file includes checks that return a superset of the target, may include false positives

def get_fields_and_tables_for_query(query)
	fields = []
	query.components.each do |component|
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
	fields	
end

def fuzzy_check_for_constraint(query, fields, constraints, constraint_type, check_func)
	found = []
  constraints.select {|constraint| constraint.type == constraint_type}.each do |constraint|
		if Array(constraint.fields).select { |f| 
			fields.select { |qf| qf.table == constraint.table and qf.column == f }.any?
		}.length == constraint.fields.length
			puts "query : #{query.raw_query.stmt}, constraint : #{constraint.inspect}"
			if check_func.nil? or check_func.call(query, constraint)
				found << {:query => query, :constraint => constraint}
			end
		end
	end
	found
end

def fuzzy_check(meta_queries, constraints)
	meta_queries.each do |query|
		fields = get_fields_and_tables_for_query(query)
		r1 = fuzzy_check_for_constraint(query, fields, constraints, :uniqueness, Proc.new do |q, c|
			!q.sql.include?"LIMIT" and q.has_limit==false
  	end)
		print_result(r1, "Uniqueness opt:")
		r2 = fuzzy_check_for_constraint(query, fields, constraints, :inclusion, nil)
		print_result(r2, "Inclusion opt:")
		r3 = fuzzy_check_for_constraint(query, fields, constraints, :presence, nil)
		print_result(r2, "Presence opt:")
	end
end

def print_result(r, title)
	r = r.uniq {|x| x[:query].raw_query.stmt}
	if r.length == 0
		return
	end
	puts title
	r.each do |pair|
		puts "  query = #{pair[:query].raw_query.stmt}, constraint = #{pair[:constraint].inspect}"
	end
end

