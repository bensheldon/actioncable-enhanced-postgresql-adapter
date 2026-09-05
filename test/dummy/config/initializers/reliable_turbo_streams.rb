# frozen_string_literal: true

Rails.application.config.to_prepare do
  Turbo::StreamsChannel.include(ActionCable::SubscriptionAdapter::EnhancedPostgresql::Channel)

  # Demonstrates computing the presence value in Ruby instead of trusting the frontend-supplied
  # `enhanced-presence` param - a real app would use `current_user` here. Returning nil (no
  # `server_presence` param) falls back to the frontend-supplied param.
  Turbo::StreamsChannel.presence_identity serialize: -> { params[:server_presence] }
end
