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
    # The primary wire name for the channel param.
    SINCE_PARAM = "enhanced-since"
    # turbo-rails forwards a `data-enhanced-since` attribute (see the README's Hotwire
    # walkthrough) as the channel param `enhanced_since` (it snake_cases dasherized data
    # attribute names), so that spelling has to be accepted too.
    SINCE_PARAM_ALTERNATIVES = ["enhanced_since"].freeze

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

      # Looks up the `since` param under SINCE_PARAM or any of SINCE_PARAM_ALTERNATIVES, trying
      # both a string and a symbol key for each (channel params are normally a
      # HashWithIndifferentAccess already, but this is defensive for anything that isn't).
      # Returns the raw value (nil if none of the keys are present) - callers parse it with
      # .parse_timestamp.
      def since_param_value(params)
        ([SINCE_PARAM] + SINCE_PARAM_ALTERNATIVES).each do |key|
          value = params[key]
          value = params[key.to_sym] if value.nil?
          return value unless value.nil?
        end

        nil
      end
    end
  end
end

require_relative "reliable_broadcasting/channel"
require_relative "reliable_broadcasting/controller"
require_relative "reliable_broadcasting/helper"
