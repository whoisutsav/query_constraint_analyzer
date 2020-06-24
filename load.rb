require './types.rb'
require 'json'
require './util.rb'


def extract_queries_and_scopes(app_dir, output_dir, rails_best_practices_cmd)
  app_name = File.basename(app_dir)
  query_output_file = File.join(output_dir, "query_output_#{app_name}")
  scope_output_file = File.join(output_dir, "scope_output_#{app_name}")
  schema_output_file = File.join(output_dir, "schema_output_#{app_name}")
  if !File.exist?(query_output_file) or !File.exist?(scope_output_file) or !File.exist?(schema_output_file)
    `cd #{app_dir} && echo "PrintQueryCheck: { output_filename_query: \"#{query_output_file}\", output_filename_scope: \"#{scope_output_file}\", output_filename_schema: \"#{schema_output_file}\"}" &> ./config/rails_best_practices.yml && #{rails_best_practices_cmd} . -c ./config/rails_best_practices.yml`
  end

  queries = Marshal.load(File.binread(query_output_file)).map do |obj|
    RawQuery.new(canonicalize_classname(obj[:class]), obj[:stmt], false, 
              obj[:caller_class_lst].map! {|x|
                                  x[:class_name]=canonicalize_classname(x[:class_name].to_s)
                                          x }.to_a, 
                  obj[:method_name], obj[:filename]) 
  end

  scopes = {} # key: class_name; value: {key: method_name, value: RawQuery}
  queries.each do |obj|
    next if obj[:method_name].blank?
    classname = canonicalize_classname(obj[:class])
    if scopes[classname].nil?
      scopes[classname] = {}
    end
    scopes[classname][obj[:method_name]] = obj
  end

  #scopes = Marshal.load(File.binread(scope_output_file)) 
  schema = Marshal.load(File.binread(schema_output_file)).map do |klass, hmap|
    TableSchema.new(klass, hmap[:fields], hmap[:associations]) 
  end

  return queries,scopes,schema
end


def extract_scope_calls(node)
  return [] if node == nil
  pp node
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
    puts "Call: #{class_name}:#{scope_name} --> #{source} "
    puts ""

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
    class_name = query_obj[:caller_class_lst][0][:class] #query_obj.class
    begin 
      ast = YARD::Parser::Ruby::RubyParser.parse(query_obj.stmt).root 
    rescue  
      next
    end
    query_node = extract_query(ast) 
    next if !query_node
    methods = get_all_methods(query_node)
    base_object_type = query_obj[:caller_class_lst]

    found_scopes = scope_hash[base_object_type] ? methods & scope_hash[base_object_type].keys : []
    if !found_scopes.empty?
      query_sources = found_scopes.sort_by(&:length).reverse.inject([query_node.source]) do |query_sources, found_scope|
        output = []
        query_sources.each do |query_source|
          scope_hash[base_object_type][found_scope].each do |scope_source|
            puts "old source = #{query_source}"
            output << query_source.gsub(/#{found_scope}(?:\(.*?\))?/, scope_source)  
            puts "new source = #{query_source.gsub(/#{found_scope}(?:\(.*?\))?/, scope_source)}"
          end 
        end
        output
      end
      query_sources.each do |query_source|
        output_arr << RawQuery.new(class_name, query_source, false, query_obj[:caller_class_lst])
      end
    else
      output_arr << query_obj
    end 
  end

  scope_hash.each do |class_name, scopes|
    scopes.each do |scope_name, scope_sources|
      scope_sources.each do |scope_source|
        output_arr << RawQuery.new(class_name, scope_source, true, [])
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
  r = Marshal.load(File.binread(output_file)).map do |obj|
    Constraint.new(obj[:table], obj[:type], obj[:fields], obj[:exists_in_db], nil) 
  end
	r += load_extra_constraints(app_name)
	r
end

def load_extra_constraints(app_name)
  r = []
  constraint_file = "./extra_constraint/inclusion.csv"
  start_processing = false
  File.open(constraint_file).read.each_line do |line|
    words = line.gsub(/[\r\n]+/,'').split(',')
    if words[1].nil?
      if words[0]==app_name
        start_processing = true
      else
        start_processing = false
      end
    elsif start_processing
      r << Constraint.new(words[0], :inclusion, [words[1]], false, '')
    end
  end
	constraint_file = "./extra_constraint/#{app_name}_constraint.json"
	if !File.exists?(constraint_file)
		return r
	end
	extra_constraints = JSON.parse(File.read(constraint_file))
	extra_constraints.each do |constraint|
		r << Constraint.new(constraint["table"], constraint["type"], constraint["fields"], false, constraint["source_code"])
	end
	r
end


def load_queries_and_schema(app_dir, output_dir, rails_best_practices_cmd)
  queries,scopes,schema = extract_queries_and_scopes(app_dir, output_dir, rails_best_practices_cmd)
  #processed_scopes = process_scopes(scopes) 
  #puts "processed scopes = #{processed_scopes.inspect}"
  #return process_queries(queries, processed_scopes),schema 
  return queries, scopes, schema
end
