# frozen_string_literal: true

Rails.application.config.to_prepare do
  Turbo::StreamsChannel.include(
    ActionCable::SubscriptionAdapter::EnhancedPostgresql::ReliableBroadcasting::Channel
  )
  Turbo::StreamsChannel.include(
    ActionCable::SubscriptionAdapter::EnhancedPostgresql::Presence::Channel
  )
end
