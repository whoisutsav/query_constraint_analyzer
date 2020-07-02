RawQuery = Struct.new(:class, :stmt, :from_scope, :caller_class_lst, :method_name, :filename)

MetaQuery = Struct.new(:raw_query, :methods, :base_object_type,
                        :filters, :has_distinct, :has_limit)

Constraint = Struct.new(:table, :type, :fields, :exists_in_db, :source)

QueryPredicate = Struct.new(:lh, :cmp, :rh, :ruby_meth)
QueryColumn = Struct.new(:table, :column, :ruby_meth)
MetaQuery2 = Struct.new(:raw_query, :has_distinct, :has_limit, :fields, :sql, :source, :methods, :components, :table)
QueryComponent = Struct.new(:meth, :param)
#QueryComponent = Struct.new(:table, :arg, :ruby_meth)

TableSchema = Struct.new(:class_name, :fields, :associations)
#association includes (:rel, :class_name, :field)
