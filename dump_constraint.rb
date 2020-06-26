require 'pp'
require 'optparse'
require './load.rb'
require './query_parser_with_sql.rb'
require './opt_check.rb'
require './fuzzy_opt_check.rb'


app_name = ARGV[0]
options = {}
app_dir = ""
$app_name = app_name.capitalize


def find_table_in_schema(table, schema)
  #puts "table = #{table}"
  schema.each do |t|
    #puts "\tschema table = #{t[:class_name]}"
		tclass = canonicalize_classname(t[:class_name].class_name)
		if table.to_s == tclass or tablename_singularize(table.to_s) == tclass
			return t
		end
	end	
	return nil
end

def dump_constraints(app_name, app_dir, options)
  schema_output_file = File.join(options[:tmp_dir], "schema_output_#{app_name}")
  schema = Marshal.load(File.binread(schema_output_file)).map do |klass, hmap|
    TableSchema.new(klass, hmap[:fields], hmap[:associations]) 
  end
  constraints = load_constraints(app_dir, options[:tmp_dir], options[:constraint_analyzer_dir])

  output = File.open('#{app_name}_constraint.txt', 'w')
  dump_constraints = []

  # fk constraint
  schema.each do |t|
    t.associations.each do |assoc|
      if assoc[:rel] == "has_one" or assoc[:rel] == "belongs_to"
        dump_constraints << "Constraint(#{t[:class_name].class_name}, fk(#{assoc[:field]}_id, #{assoc[:class_name]}))"
      end
    end
  end
  constraints.each do |c|
    if c[:table].blank?
      puts "Constraint table blank: #{c}"
      next
    end
    if c[:type] == :uniqueness
      dump_constraints << "Constraint(#{c[:table]}, unique([#{c[:fields].join(', ')}]))"
    elsif c[:type] == :presence
      assoc = find_table_in_schema(c[:table], schema).associations.select { |ax| ax[:field]==c[:fields][0] }
      if assoc.length() > 0
        field = "#{Array(c[:fields])[0]}_id"
      else
        field = Array(c[:fields])[0]
      end
      dump_constraints << "Constraint(#{c[:table]}, presence(#{field}))"
    elsif c[:type] == "custom"
      expr = c[:source]
      dump_constraints << "Constraint(#{c[:table]}, #{expr})"
    end
  end
  dump_constraints.each do |c|
    puts c
  end
end



config = YAML.load_file('config.yml')
config.each do |key, value|
  if key == 'apps_dir' 
    app_dir = "#{value}/#{app_name}"
  elsif key == 'constraint_analyzer_dir'
    options[:constraint_analyzer_dir] = value
  elsif key == 'tmp_output_dir'
    options[:tmp_dir] = value
  elsif key == 'rails_best_practices_cmd'
	options[:rails_best_practices_cmd] = value
  end
end

dump_constraints(app_name, app_dir, options)