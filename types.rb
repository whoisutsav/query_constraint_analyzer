RawQuery = Struct.new(:class, :stmt, :from_scope, :caller_class_lst)

MetaQuery = Struct.new(:raw_query, :methods, :base_object_type,
                        :filters, :has_distinct, :has_limit, :components, :sql)

Constraint = Struct.new(:table, :type, :fields, :exists_in_db, :source)

QueryPredicate = Struct.new(:lh, :cmp, :rh, :ruby_meth)
QueryComponent = Struct.new(:table, :arg, :ruby_meth)
QueryColumn = Struct.new(:table, :column, :ruby_meth)

TableSchema = Struct.new(:class_name, :fields, :associations)
#association includes (:rel, :class_name, :field)
