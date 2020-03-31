require 'pg_query'
class PgQueryExtension < PgQuery 
  # Returns a list of columns that the query filters by - this excludes the
  # target list, but includes things like JOIN condition and WHERE clause.
  #
  # Note: This also traverses into sub-selects.
	def self.setup(pgparse)
		@tree = pgparse.tree
		@aliases = pgparse.aliases
		return self
	end

	def self.get_all_columns
		columns = []
		exprs = @tree.dup
		loop do
			expr = exprs.shift
			if expr
				#puts "Expr = #{expr} #{expr.is_a?(Hash)} #{expr.is_a?(Array)} \n"
				if expr.is_a?(Hash)
					#if expr.size == 1 && expr.keys[0][/^[A-Z]+/]
						if !expr[COLUMN_REF].nil? 
							column, table = expr[COLUMN_REF]['fields'].select{ |f| !f['String'].nil? and !f['String']['str'].nil? }.map { |f| f['String']['str'] }.reverse
							if !column.nil?
								columns << {:table => @aliases[table] || table, :column => column}
							end
							next
						end
					exprs += expr.values.compact
				elsif expr.is_a?(Array)
					exprs += expr
				end
			end
			break if exprs.empty?
		end
		columns.uniq
	end

  def self.filter_predicates 
    load_tables_and_aliases! if @aliases.nil?

    # Get condition items from the parsetree
    statements = @tree.dup
    condition_items = []
    filter_columns = []
		filter_preds = []
    loop do
      statement = statements.shift
      if statement
        if statement[RAW_STMT]
          statements << statement[RAW_STMT][STMT_FIELD]
        elsif statement[SELECT_STMT]
          case statement[SELECT_STMT]['op']
          when 0
            if statement[SELECT_STMT][FROM_CLAUSE_FIELD]
              # FROM subselects
              statement[SELECT_STMT][FROM_CLAUSE_FIELD].each do |item|
                next unless item['RangeSubselect']
                statements << item['RangeSubselect']['subquery']
              end

              # JOIN ON conditions
              condition_items += conditions_from_join_clauses(statement[SELECT_STMT][FROM_CLAUSE_FIELD])
            end

            # WHERE clause
            condition_items << statement[SELECT_STMT]['whereClause'] if statement[SELECT_STMT]['whereClause']

            # CTEs
            if statement[SELECT_STMT]['withClause']
              statement[SELECT_STMT]['withClause']['WithClause']['ctes'].each do |item|
                statements << item['CommonTableExpr']['ctequery'] if item['CommonTableExpr']
              end
            end
          when 1
            statements << statement[SELECT_STMT]['larg'] if statement[SELECT_STMT]['larg']
            statements << statement[SELECT_STMT]['rarg'] if statement[SELECT_STMT]['rarg']
          end
        elsif statement['UpdateStmt']
          condition_items << statement['UpdateStmt']['whereClause'] if statement['UpdateStmt']['whereClause']
        elsif statement['DeleteStmt']
          condition_items << statement['DeleteStmt']['whereClause'] if statement['DeleteStmt']['whereClause']
        end
      end

      # Process both JOIN and WHERE conditions here
      next_item = condition_items.shift
      if next_item
        if next_item[A_EXPR]
					lhrh = []
          %w[lexpr rexpr].each do |side|
            expr = next_item.values[0][side]
            next unless expr && expr.is_a?(Hash)
						lhrh << expr
          end
					filter_preds << {:lh => lhrh[0], :rh => lhrh[1], :op => next_item.values[0][:name]} 
        elsif next_item[NULL_TEST]
					filter_preds << {:lh => expr, :rh => "NULL", :op => '='} 
          condition_items << next_item[NULL_TEST]['arg']
        end
      end

      break if statements.empty? && condition_items.empty?
    end

		filter_preds
  end

  protected

  def self.conditions_from_join_clauses(from_clause)
    condition_items = []
    from_clause.each do |item|
      next unless item[JOIN_EXPR]

      joinexpr_items = [item[JOIN_EXPR]]
      loop do
        next_item = joinexpr_items.shift
        break unless next_item
        condition_items << next_item['quals'] if next_item['quals']
        %w[larg rarg].each do |side|
          next unless next_item[side][JOIN_EXPR]
          joinexpr_items << next_item[side][JOIN_EXPR]
        end
      end
    end
    condition_items
  end
end
