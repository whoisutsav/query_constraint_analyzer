RawQuery = Struct.new(:class, :stmt, :from_scope)

MetaQuery = Struct.new(:raw_query, :methods, :base_object_type,
                        :filters, :has_distinct, :has_limit)

Constraint = Struct.new(:table, :type, :fields, :exists_in_db)
