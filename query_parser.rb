require 'rails_best_practices'
require 'yard'
require 'open3'
require 'active_support'
require 'pg_query'
require './types.rb'


MULTI_QUERY_METHODS = %w[where pluck distinct eager_load from group having includes joins left_outer_joins limit offset order preload readonly reorder select reselect select_all reverse_order unscope find_each rewhere].freeze
SINGLE_QUERY_METHODS = %w[find find! take take! first first! last last! find_by find_by!].freeze


def extract_string(node)
  if node.type == :string_literal 
    return node.source
  elsif node.type == :symbol_literal
    return node[0][0][0] 
  elsif node.type == :label
    return node[0]
  end
end

def is_valid_node?(node)
  return node.class <= YARD::Parser::Ruby::AstNode
end
def check_node_type(node, type)
  return !node.nil? && node.class <= YARD::Parser::Ruby::AstNode && node.type == type
end
def check_node_in_types(node, types)
  return node.class <= YARD::Parser::Ruby::AstNode && types.include?(node.type)
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


def extract_fields_from_args_for_where(node)
  return [] if node == nil

  output = []
  if node.type == :list
    node.each do |child|
      next if !check_node_type(child, :assoc)

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
  end
  return output.uniq
end

def extract_fields_from_args(node)
  return [] if node == nil
  output = []
  if node.type == :string_literal
    where_str = extract_string(node).gsub(/\n/, "").gsub(/"/,"")
    raw_filters = where_str.split(/\s+and\s+|\s+or\s+/i).map(&:strip)
    
    raw_filters.each do |filter_str|
      is_not_null = filter_str.match(/is not null/i) ? true : false
      table, column = filter_str.split(/<|<=|>|>=|!=|=|\s+is not\s+|\s+is\s+/i)[0].rpartition('.').values_at(0,2)
      match_data = table.match(/{(.*)\.table_name}/)
      table = match_data ? match_data[1] : table
      column = column.strip
      output << {:table => table, :column => column, :is_not_null => is_not_null}
    end
  else 
    return extract_fields_from_args_for_where(node)
  end
  output.uniq
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
    return get_filters(node[0]) + extract_fields_from_args(node[3][0][0]) 
  elsif node.type == :call
    return get_filters(node[0])
  elsif node.type == :fcall
    return extract_fields_from_args(node[1][0][0]) 
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


def derive_metadata(raw_queries, schema)
  output = []
  raw_queries.each do |raw_query|
    begin 
      ast = YARD::Parser::Ruby::RubyParser.parse(raw_query.stmt).root 
    rescue  
      next
    end

    query_node = extract_query(ast)
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

    output << meta
  end
  puts "succ = #{output.map{|x| x.filters.length>0 ? 1 : 0 }.sum} / total #{raw_queries.length}"
  return output
end

