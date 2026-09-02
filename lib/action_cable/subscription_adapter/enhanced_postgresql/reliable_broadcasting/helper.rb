# frozen_string_literal: true

class ActionCable::SubscriptionAdapter::EnhancedPostgresql
  module ReliableBroadcasting
    # View helper exposing the timestamp captured by ReliableBroadcasting::Controller (done
    # automatically by the gem's Railtie for ActionController::Base, see README for how to
    # register it by hand elsewhere).
    module Helper
      # ISO 8601 string (UTC, microsecond precision) suitable for a channel param or a
      # data-* attribute.
      def action_cable_since_param
        time = controller.action_cable_since if respond_to?(:controller) && controller.respond_to?(:action_cable_since)
        ReliableBroadcasting.format_timestamp(time || (@action_cable_since ||= Time.now.utc))
      end

      # <meta name="action-cable-since" content="..."> - read from JS via
      # ActionCable.getConfig("since").
      def action_cable_since_meta_tag
        tag("meta", name: ReliableBroadcasting::META_TAG_NAME, content: action_cable_since_param)
      end
    end
  end
end
