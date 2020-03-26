#TODO - test query parsing and opt checker


#queries = [
#  {:class => "ClassA", :stmt => "user = User.where(:remember_token => token).first"},
#  {:class => "ClassB", :stmt => "User.find(session[\"user_id\"])\n"},
#  {:class => "ClassC", :stmt => "User.where(\"preferences.sms_email\" => address.strip).includes(:preference).first"},
#  {:class=>"User", :stmt=>"Dependency.where(predecessor_id: ids).destroy_all\n"},
#  {:class => "ContextsController", :stmt => "@context.todos.deferred.includes(Todo::DEFAULT_INCLUDES)"},
#  {:class => "ClassD" , :stmt => "Person.where(id: member_ids, rejected: true).where.not(email: nil, phone: 123, address: nil)"},
#  {:class => "ClassE", :stmt => "Tracker.where(:id => tracker_id_was, :default_status_id => status_id_was).where.not(:user_id => nil, :folder_id => nil).any?"},
#  {:class => "ClassF", :stmt => "Post.with_deleted.find_by(id: target_id)"},
#  {:class => "ClassG", :stmt => "Tracker.joins(projects: :enabled_modules).where(\"\#{Project.table_name}.status <> ?\", STATUS_ARCHIVED).where(:enabled_modules => {:name => 'issue_tracking'}).distinct.sorted"},
#  {:class => "ClassH", :stmt => "User.active.joins(:members, :cats).where(\"\#{Member.table_name}.project_id = ?\", id).distinct"},
#  {:class => "ClassJ", :stmt => "User.where(\"editor.id > 0 AND editor.id != author.id AND post_id < ? or project.id = 3 and member_id IS NOT null and issue.user_id is null\")"},
#  {:class => "ClassK", :stmt => "ChildTheme.where(parent_theme_id: theme_id).distinct.pluck(:child_theme_id)"},
#  {:class => "ClassL", :stmt => "User.where(\"username IS NOT NULL and created_at IS NOT NULL\")"} 
#  {:class=>"ApiOpenidConnectAuthorization", :stmt=>"where(o_auth_application: app, user: user).all"},
#]
