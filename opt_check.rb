# Check for filtering on uniqueness fields + distinct
def check_filter_unique_distinct(meta_query, constraints)
  found = []
  meta_query.filters.group_by{|filter| filter[:table]}.each do |object_type, filters|
    next if !constraints.any?{|cons| cons.table == object_type} or filters.empty?

    constraints.select {|constraint| constraint.table == object_type and constraint.type == :uniqueness}.each do |constraint|
      if (Array(constraint.fields) - filters.map{|f| f[:column]}).empty? and meta_query.has_distinct
        found << {:query => meta_query, :constraint => constraint}
      end
    end 
  end

  return found 
end

# Check if query filters on inclusion fields
def check_filter_inclusion(meta_query, constraints)
  found = []
  meta_query.filters.group_by{|filter| filter[:table]}.each do |object_type, filters|
    next if !constraints.any?{|cons| cons.table == object_type} or filters.empty?

    constraints.select {|constraint| constraint.table == object_type and constraint.type == :inclusion}.each do |constraint|
      if filters.map{|f| f[:column]}.include? constraint.fields
        found << {:query => meta_query, :constraint => constraint}
      end
    end 
  end

  return found
end

# Check if query filters are unique and query is missing limit
def check_missing_limit(meta_query, constraints)
  found = []
  meta_query.filters.group_by{|filter| filter[:table]}.each do |object_type, filters|
    next if !constraints.any?{|cons| cons.table == object_type} or filters.empty?

    # check for only one record exists and no limit 1
    constraints.select {|constraint| constraint.table == object_type and constraint[:type] == :uniqueness}.each do |constraint|
      if (Array(constraint.fields) - filters.map{|f| f[:column]}).empty? and !meta_query.has_limit
        found << {:query => meta_query, :constraint => constraint}
      end
    end 
  end

  return found
end

# Check for not null filtering on field with presence constraint
def check_not_null_presence(meta_query, constraints)
  found = []
  meta_query.filters.group_by{|filter| filter[:table]}.each do |object_type, filters|
    next if !constraints.any?{|cons| cons.table == object_type} or filters.empty?

    constraints.select {|constraint| constraint.table == object_type and constraint.type == :presence}.each do |constraint|
      if filters.select{|f| f[:is_not_null]}.map{|f| f[:column]}.include? constraint.fields
        not_null_in_db = constraint.exists_in_db 
        found << {:query => meta_query, :constraint => constraint}
      end
    end 
  end

  return found
end

def check_duplicate_method(meta_query)
    downcased_singular_methods = meta_query[:methods].map{|str| ActiveSupport::Inflector.singularize(str).downcase}
    dupe_methods = downcased_singular_methods.select{|str| !(["join", "left_outer_join", "merge", "reference", "include", "not", "map", "collect", "flat_map", "and", "compact", "split", "try", "or"] + MULTI_QUERY_METHODS + SINGLE_QUERY_METHODS).include?(str) and downcased_singular_methods.count(str) > 1}
    return dupe_methods.empty? ? [] : [{:query => meta_query, :constraint => nil, :dupes => dupe_methods.uniq}]
end

def opt_check(meta_queries, constraints)
  check_results = {
    :redundant_distinct => [],
    :filter_inclusion => [],
    :missing_limit => [],
    :not_null_presence => [],
    :duplicate_method => [],
  }

  meta_queries.each do |meta_query|
    check_results[:redundant_distinct] += check_filter_unique_distinct(meta_query, constraints) 
    check_results[:filter_inclusion] += check_filter_inclusion(meta_query, constraints)
    check_results[:missing_limit] += check_missing_limit(meta_query, constraints)
    check_results[:not_null_presence] += check_not_null_presence(meta_query, constraints)
    check_results[:duplicate_method] += check_duplicate_method(meta_query)
  end

  return check_results
end
