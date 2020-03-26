require './types.rb'


def extract_queries_and_scopes(app_dir, output_dir)
  app_name = File.basename(app_dir)
  query_output_file = File.join(output_dir, "query_output_#{app_name}")
  scope_output_file = File.join(output_dir, "scope_output_#{app_name}")
  if !File.exist?(query_output_file) or !File.exist?(scope_output_file)
    `cd #{app_dir} && echo "PrintQueryCheck: { output_filename_query: \"#{query_output_file}\", output_filename_scope: \"#{scope_output_file}\"}" &> ./config/rails_best_practices.yml && rails_best_practices . -c ./config/rails_best_practices.yml`
  end

  queries = Marshal.load(File.binread(query_output_file)).map do |obj|
    RawQuery.new(obj[:class], obj[:stmt]) 
  end
  scopes = Marshal.load(File.binread(scope_output_file))

  return queries,scopes
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

def process_queries(query_arr, scope_hash={})
  output_arr = []
  query_arr.each do |query_obj|
    class_name = query_obj.class
    begin 
      ast = YARD::Parser::Ruby::RubyParser.parse(query_obj.stmt).root 
    rescue  
      next
    end
    query_node = extract_query(ast) 
    next if !query_node
    methods = get_all_methods(query_node)
	base_object_type = infer_object_type(query_node)

    found_scopes = scope_hash[base_object_type] ? methods & scope_hash[base_object_type].keys : []
    if !found_scopes.empty?
      query_sources = found_scopes.sort_by(&:length).reverse.inject([query_node.source]) do |query_sources, found_scope|
        output = []
        query_sources.each do |query_source|
          scope_hash[base_object_type][found_scope].each do |scope_source|
            output << query_source.gsub(/#{found_scope}(?:\(.*?\))?/, scope_source)  
          end 
        end
        output
      end
      query_sources.each do |query_source|
        output_arr << RawQuery.new(class_name, query_source, false)
      end
    else
      output_arr << query_obj
    end 
  end

  scope_hash.each do |class_name, scopes|
    scopes.each do |scope_name, scope_sources|
      scope_sources.each do |scope_source|
        output_arr << RawQuery.new(class_name, scope_source, true)
      end
    end
  end

  output_arr
end

def load_constraints(app_dir, output_dir, constraint_analyzer_dir)
  app_name = File.basename(app_dir)
  output_file = File.join(output_dir, "constraint_output_#{app_name}")
  if !File.exist?(output_file)
    `cd #{constraint_analyzer_dir} && ruby main.rb -a #{app_dir} --dump-constraints #{output_file}` 
  end
  return Marshal.load(File.binread(output_file)).map do |obj|
    Constraint.new(obj[:table], obj[:type], obj[:fields], obj[:exists_in_db]) 
  end
end


def load_queries(app_dir, output_dir)
  queries,scopes = extract_queries_and_scopes(app_dir, output_dir)
  processed_scopes = process_scopes(scopes) 
  return process_queries(queries, processed_scopes) 
end
