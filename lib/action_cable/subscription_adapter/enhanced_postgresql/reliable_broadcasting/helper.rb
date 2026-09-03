# frozen_string_literal: true

class ActionCable::SubscriptionAdapter::EnhancedPostgresql
  module ReliableBroadcasting
    # View helper exposing the timestamp captured by ReliableBroadcasting::Controller (done
    # automatically by the gem's Railtie for ActionController::Base, see README for how to
    # register it by hand elsewhere).
    module Helper
      # A plain ISO 8601 UTC timestamp (the time captured by ReliableBroadcasting::Controller),
      # suitable for a channel param or a data-* attribute. It's not secret and isn't encrypted:
      # it only ever selects a window of already-authorized messages to replay, and the server
      # clamps it to message_retention regardless of what value it's given - see
      # EnhancedPostgresql#messages_since and the README's "Reliable broadcasting" section.
      def action_cable_enhanced_since_param
        time = controller.action_cable_since if respond_to?(:controller) && controller.respond_to?(:action_cable_since)
        ReliableBroadcasting.format_timestamp(time || (@action_cable_since ||= Time.now.utc))
      end

      # An encrypted-and-signed token of `value.to_s`, suitable for a channel param or a data-*
      # attribute (typically `turbo_stream_from @room, data: { enhanced_presence:
      # action_cable_enhanced_presence_param(current_user.name) }`), for use with
      # Presence::Channel. A client can see the token but can't read or forge its content - see
      # #action_cable_param_encryptor and Presence.encrypt.
      def action_cable_enhanced_presence_param(value)
        Presence.encrypt(value, action_cable_param_encryptor)
      end

      private

      # The ActiveSupport::MessageEncryptor used to encrypt values embedded in the page - only
      # `enhanced-presence`, if the Presence concern is in use (`enhanced-since` is a plain,
      # unencrypted timestamp - see #action_cable_enhanced_since_param). Defined as its own
      # method - rather than reaching for ActionCable.server.pubsub inline - so tests can
      # override it on a view object to inject an encryptor built from a known secret without
      # booting a real adapter.
      def action_cable_param_encryptor
        pubsub = ActionCable.server.pubsub

        unless pubsub.respond_to?(:payload_encryptor)
          raise "ActionCable.server.pubsub (#{pubsub.class}) does not support encrypted channel params. " \
            "This helper requires the enhanced_postgresql adapter - set `adapter: enhanced_postgresql` in cable.yml."
        end

        pubsub.payload_encryptor
      end
    end
  end
end
