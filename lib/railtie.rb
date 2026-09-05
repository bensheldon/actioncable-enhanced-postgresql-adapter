class ActionCable::SubscriptionAdapter::EnhancedPostgresql
  class Railtie < ::Rails::Railtie
    initializer "action_cable.enhanced_postgresql_adapter" do
      ActiveSupport.on_load(:active_record) do
        adapter = ActionCable::SubscriptionAdapter::EnhancedPostgresql
        ActiveRecord::SchemaDumper.ignore_tables << adapter::BROADCASTS_TABLE
        ActiveRecord::SchemaDumper.ignore_tables << adapter::PRESENCES_TABLE
        ActiveRecord::SchemaDumper.ignore_tables << "action_cable_large_payloads" # legacy table, no longer used
      end

      ActiveSupport.on_load(:action_controller_base) do
        include ActionCable::SubscriptionAdapter::EnhancedPostgresql::Controller
        helper ActionCable::SubscriptionAdapter::EnhancedPostgresql::Helper
      end
    end
  end
end
