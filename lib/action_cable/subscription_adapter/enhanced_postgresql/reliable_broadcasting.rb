# frozen_string_literal: true

require "time"

# Rails integration for reliable broadcasting: a Channel concern that replays messages stored
# by the adapter (see EnhancedPostgresql#messages_since) once a subscription's pubsub #subscribe
# has been confirmed, plus Controller/Helper concerns that capture and expose the timestamp a
# client needs to send back as the `since` channel param. See the "Reliable broadcasting"
# section of the README for the full picture and setup instructions.
#
# These files only depend on active_support/concern (and, transitively, whatever the adapter
# file already required) - they load fine without actionpack/actionview present, and only touch
# Rails constants (ActionController::Base, ActionView helpers) once actually included into them.
class ActionCable::SubscriptionAdapter::EnhancedPostgresql
  module ReliableBroadcasting
    SINCE_PARAM = "since"
    META_TAG_NAME = "action-cable-since"

    class << self
      # Time -> "2026-09-02T18:55:12.123456Z" (UTC, microsecond precision).
      def format_timestamp(time)
        time.utc.iso8601(6)
      end

      # Inverse of .format_timestamp. Accepts a Time (returned as-is, converted to UTC) or a
      # String (parsed as ISO 8601). Returns nil for nil/blank/unparseable input instead of
      # raising, since this is ultimately fed by client-controlled channel params.
      def parse_timestamp(value)
        return value.utc if value.is_a?(Time)
        return nil unless value.respond_to?(:to_str)

        string = value.to_str
        return nil if string.empty?

        Time.iso8601(string).utc
      rescue ArgumentError, TypeError
        nil
      end
    end
  end
end

require_relative "reliable_broadcasting/channel"
require_relative "reliable_broadcasting/controller"
require_relative "reliable_broadcasting/helper"
