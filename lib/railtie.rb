class ActionCable::SubscriptionAdapter::EnhancedPostgresql
  class Railtie < ::Rails::Railtie
    initializer "action_cable.enhanced_postgresql_adapter" do
      ActiveSupport.on_load(:active_record) do
        adapter = ActionCable::SubscriptionAdapter::EnhancedPostgresql
        ActiveRecord::SchemaDumper.ignore_tables << adapter::BROADCASTS_TABLE
        ActiveRecord::SchemaDumper.ignore_tables << adapter::LEGACY_LARGE_PAYLOADS_TABLE
        ActiveRecord::SchemaDumper.ignore_tables << adapter::PRESENCES_TABLE
      end

      ActiveSupport.on_load(:action_controller_base) do
        include ActionCable::SubscriptionAdapter::EnhancedPostgresql::ReliableBroadcasting::Controller
        helper ActionCable::SubscriptionAdapter::EnhancedPostgresql::ReliableBroadcasting::Helper
      end
    end
  end
end
