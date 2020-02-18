require "shellwords"
require "securerandom"

#reports_schema = {
#  "id" => :auto,
#  "item_id" => :int,
#  #"item_type" => :enum,
#  "item_type_id" => :range,
#  "reviewed" => :bool,
#  "text" => :text,
#  "created_at" => :datetime,
#  "updated_at" => :datetime,
#  "user_id" => :int
#}

people_schema = {
  "guid" => :guid,
  "diaspora_handle" => :guid,
  "serialized_public_key" => :text,
  "created_at" => :datetime,
  "updated_at" => :datetime
}

def generate_value(type)
  case type
  when :int
    rand(2147483648)
  when :varchar
    n = rand(254) + 1
    (0...n).map { ('a'..'z').to_a[rand(26)] }.join 
  when :enum
    ["valueA", "valueB", "valueC", "valueD"].shuffle.first
  when :range
    rand(4) + 1
  when :text
    n = rand(254) + 1
    (0...n).map { ('a'..'z').to_a[rand(26)] }.join
  when :bool
    rand(2)
  when :datetime
    "1970-01-01 00:00:01"
  when :guid
    SecureRandom.uuid.to_s

  end
end

def generate_data(schema)
  data = [] 
  schema.each do |field, type|
    data << generate_value(type)
  end

  return data
end

#def generate_query(database, table, schema, num_records)
#  fields = schema.keep_if { |k, v| v != :auto }
#  query = "INSERT IGNORE INTO #{database}.#{table} (#{fields.keys.join(",")}) VALUES "
#  query += (1..num_records).map { |i| "(" + generate_data(fields).join(",") + ")" }.join(",") 
#  query += ";"
#  return query
#end

def write_to_csv(schema, path, num_records)
  open(path, 'w') do |f|
    print "0 records written" 
    (1..num_records).each do |i|
	  f << generate_data(schema).join(",") + "\n"
	  print "\r#{i} records written" if i % 1000 == 0 
    end
	print "\n"
  end
end

def execute_insert(username="", password="", query)
  `mysql #{"-u" + username if !username.empty?} \
                  #{"-p" + password if !password.empty?} \
                  -e #{Shellwords.escape(query)}`

end

def execute_load_csv(username ="", password="", path, database, table, schema)
  command = "LOAD DATA INFILE '#{path}' INTO TABLE #{database}.#{table} FIELDS TERMINATED BY ',' (#{schema.keys.join(",")});"
  `mysql #{"-u" + username if !username.empty?} \
                  #{"-p" + password if !password.empty?} \
				  -e #{Shellwords.escape(command)}`
end

num_records = 1000000
#path = "/Users/utsavsethi/tmp/randgen_data.csv"
path = ARGV[0]
fields = people_schema.keep_if { |k, v| v != :auto }
puts "Writing CSV file #{path}..."
write_to_csv(fields, path, num_records)
#puts "Loading CSV..."
#execute_load_csv("root", "", path, "diaspora_development", "reports", fields)
#puts "Done"





