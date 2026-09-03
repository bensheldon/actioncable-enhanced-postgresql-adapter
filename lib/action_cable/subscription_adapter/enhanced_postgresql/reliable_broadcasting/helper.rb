# frozen_string_literal: true

class ActionCable::SubscriptionAdapter::EnhancedPostgresql
  module ReliableBroadcasting
    # View helper exposing the timestamp captured by ReliableBroadcasting::Controller (done
    # automatically by the gem's Railtie for ActionController::Base, see README for how to
    # register it by hand elsewhere).
    module Helper
      # An encrypted-and-signed token embedding the timestamp captured by
      # ReliableBroadcasting::Controller, suitable for a channel param or a data-* attribute. A
      # client can see the token but can't read or forge its content - see
      # #action_cable_param_encryptor and ReliableBroadcasting.encrypt_since.
      def action_cable_enhanced_since_param
        time = controller.action_cable_since if respond_to?(:controller) && controller.respond_to?(:action_cable_since)
        ReliableBroadcasting.encrypt_since(time || (@action_cable_since ||= Time.now.utc), action_cable_param_encryptor)
      end

      # <meta name="action-cable-enhanced-since" content="..."> - read from JS via
      # ActionCable.getConfig("enhanced-since").
      def action_cable_enhanced_since_meta_tag
        tag("meta", name: ReliableBroadcasting::META_TAG_NAME, content: action_cable_enhanced_since_param)
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

      # The ActiveSupport::MessageEncryptor used to encrypt values embedded in the page (the
      # `enhanced-since` param, and, if the Presence concern is also in use, `enhanced-presence`).
      # Defined as its own method - rather than reaching for ActionCable.server.pubsub inline -
      # so tests can override it on a view object to inject an encryptor built from a known
      # secret without booting a real adapter.
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
