# frozen_string_literal: true

require "active_support/concern"

class ActionCable::SubscriptionAdapter::EnhancedPostgresql
  module ReliableBroadcasting
    # Include into ActionController::Base (done automatically by the gem's Railtie) or
    # ActionController::API (by hand, see README) to capture the time an action started
    # rendering, so it can be embedded in the response and sent back as the `since` channel
    # param. See ReliableBroadcasting::Helper for the view-facing half of this.
    module Controller
      extend ActiveSupport::Concern

      included do
        prepend_before_action :capture_action_cable_since if respond_to?(:prepend_before_action)
        helper_method :action_cable_since if respond_to?(:helper_method)
      end

      # Time (UTC) captured just before the action ran. Falls back to Time.now.utc if called
      # before the prepend_before_action callback has run (e.g. from within a `before_action`
      # that runs earlier for some other reason, or when this concern was included into a
      # controller that doesn't support before_action callbacks at all).
      def action_cable_since
        @action_cable_since ||= Time.now.utc
      end

      private

      def capture_action_cable_since
        @action_cable_since = Time.now.utc
      end
    end
  end
end
