require './query_parser.rb'
require './util.rb'
# This file includes checks that return a superset of the target, may include false positives



def fuzzy_check_for_constraint(query, fields, constraints, constraint_type, check_func)
	found = []
	constraints.select {|constraint| !constraint.table.nil? && constraint.type.to_s == constraint_type.to_s}.each do |constraint|
		if Array(constraint.fields).select { |f| 
			fields.select { |qf| tablename_singularize(qf.table) == (f.include?('.') ? f.split('.')[0] : canonicalize_classname(constraint.table)) and qf.column == f }.any?
		}.length == Array(constraint.fields).length
			if check_func.nil? or check_func.call(query, constraint)
				found << {:query => query, :constraint => constraint}
			end
		end
	end
	found
end

def fuzzy_check(meta_queries, constraints)
	count = 0
	meta_queries.each do |query|
		fields = query.fields 
		#puts "Query = #{query.raw_query.stmt}, fields = #{fields.map{|x| x.table+":"+x.column}.join(', ')}"
		# r1 = fuzzy_check_for_constraint(query, fields, constraints, :uniqueness, Proc.new do |q, c|
		# 	!q.has_limit #&& (Array(c.fields).length > 1 || Array(c.fields)[0].column != "id")
  	# end)
		# print_result(r1, "Uniqueness opt:")

		r2 = fuzzy_check_for_constraint(query, fields, constraints, :inclusion, nil)
		print_result(r2, "Inclusion opt:")
		if r2.length > 0
			count += 1
		end

		# r3 = fuzzy_check_for_constraint(query, fields, constraints, :presence, Proc.new do |q, c|
		# 	(!q.sql.blank? && (q.sql.include?("JOIN") || q.sql.include?("join"))) || (q.methods.include?("joins")) \
		# 	|| (q.raw_query.stmt.include?("NULL")) || (q.raw_query.stmt.include?("nil"))
		# end)
		# print_result(r3, "Presence opt:")

		# r4 = fuzzy_check_for_constraint(query, fields, constraints, :custom, Proc.new do |q, c|
		# 	true
		# end)
		# print_result(r4, "Custom opt:")

		# r4 = fuzzy_check_for_constraint(query, fields, constraints, :format, Proc.new do |q, c|
		# 	true
		# end)
		# print_result(r4, "Format opt:")

	end
	puts "Total count = #{count}"
end

def print_result(r, title)
	r = r.uniq {|x| x[:query].raw_query.stmt}
	if r.length == 0
		return
	end
	puts title
	r.each do |pair|
		puts "  query = #{pair[:query].raw_query.stmt}"
		puts "  constraint = #{pair[:constraint].type} -> #{pair[:constraint].table} : #{pair[:constraint].fields}; in_db? #{pair[:constraint].exists_in_db}"
		puts "  #{pair[:query].raw_query.stmt}\t#{pair[:query].raw_query.filename}\t#{pair[:constraint].type}\t#{pair[:constraint].table}\t#{pair[:constraint].fields}\t#{pair[:constraint].exists_in_db}"
	end
end

