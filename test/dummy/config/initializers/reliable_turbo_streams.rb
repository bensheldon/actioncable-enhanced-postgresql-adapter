# frozen_string_literal: true

# Demonstrates a channel computing its own presence value in Ruby instead of trusting the
# frontend-supplied `enhanced-presence` param - see the README's "Computing presence on the
# server" subsection. A real app would read `current_user` here (available directly on the
# channel when the connection declares `identified_by :current_user`); this dummy app has no
# authentication to demonstrate that with, so it stands in with a plain channel param instead.
module DummyServerPresence
  def enhanced_presence
    params[:server_presence].presence || super
  end
end

Rails.application.config.to_prepare do
  Turbo::StreamsChannel.include(
    ActionCable::SubscriptionAdapter::EnhancedPostgresql::ReliableBroadcasting::Channel
  )
  Turbo::StreamsChannel.include(
    ActionCable::SubscriptionAdapter::EnhancedPostgresql::Presence::Channel
  )
  # Included after Presence::Channel so its #enhanced_presence takes precedence in the ancestor
  # chain - `super` above falls back to Presence::Channel's default (frontend-token) behavior.
  Turbo::StreamsChannel.include(DummyServerPresence)
end
