# frozen_string_literal: true

require "active_support/concern"

class ActionCable::SubscriptionAdapter::EnhancedPostgresql
  module ReliableBroadcasting
    # Include into an ActionCable::Channel::Base subclass - typically ApplicationCable::Channel,
    # or Turbo::StreamsChannel via an initializer - to replay, right after the subscription is
    # confirmed, any messages that were broadcast before the client managed to subscribe.
    #
    #   module ApplicationCable
    #     class Channel < ActionCable::Channel::Base
    #       include ActionCable::SubscriptionAdapter::EnhancedPostgresql::ReliableBroadcasting::Channel
    #     end
    #   end
    module Channel
      extend ActiveSupport::Concern

      # The Time (UTC) parsed from the `since` channel param (accepts both string and symbol
      # keys), or nil if it's absent or unparseable.
      def replay_since
        return @replay_since if defined?(@replay_since)

        @replay_since = ReliableBroadcasting.parse_timestamp(
          params[ReliableBroadcasting::SINCE_PARAM] || params[ReliableBroadcasting::SINCE_PARAM.to_sym]
        )
      end

      # Streams#stop_stream_from / #stop_all_streams are public in ActionCable, so these overrides
      # stay public too (defining them below `private` would hide them on the including channel).
      def stop_stream_from(broadcasting)
        super
        reliable_broadcasting_handlers.delete(String(broadcasting))
      end

      def stop_all_streams
        super
        reliable_broadcasting_handlers.clear
      end

      private

      # Streams#stream_handler builds the inner, synchronous handler (decode + transmit) that
      # Streams#worker_pool_stream_handler then wraps for dispatch to the worker pool. We keep
      # our own reference to that inner handler, per broadcasting, so replay can call it
      # directly and in id order once it's running on a worker thread of its own.
      def stream_handler(broadcasting, user_handler, coder: nil)
        handler = super
        reliable_broadcasting_handlers[String(broadcasting)] = handler
        handler
      end

      # ActionCable only calls this once the subscription confirmation counter has reached zero,
      # i.e. once every pubsub#subscribe (LISTEN) for this channel has actually succeeded (see
      # ActionCable::Channel::Base#ensure_confirmation_sent and Streams#stream_from), so nothing
      # broadcast from this point on can be missed. It runs on the connection's event-loop
      # thread, so the replay - which does a DB read per stream - is dispatched to the worker
      # pool rather than run here.
      def transmit_subscription_confirmation
        already_sent = subscription_confirmation_sent?
        super
        schedule_replay_of_missed_messages unless already_sent
      end

      def schedule_replay_of_missed_messages
        since = replay_since
        return if since.nil? || reliable_broadcasting_handlers.empty?

        unless pubsub.respond_to?(:messages_since)
          logger.warn "#{self.class.name} received a `since` param, but #{pubsub.class} does not support message replay"
          return
        end

        if pubsub.respond_to?(:reliable_broadcasting?) && !pubsub.reliable_broadcasting?
          logger.warn "#{self.class.name} received a `since` param, but reliable_broadcasting is not enabled - " \
            "set `reliable_broadcasting: true` in cable.yml to enable replay"
          return
        end

        connection.worker_pool.async_invoke(self, :replay_missed_messages, since, connection: connection)
      end

      # Runs on a worker pool thread (see #schedule_replay_of_missed_messages above).
      def replay_missed_messages(since)
        reliable_broadcasting_handlers.each do |broadcasting, handler|
          messages = pubsub.messages_since(broadcasting, since)
          messages.each { |message| handler.call(message.payload) }

          if messages.any?
            logger.info "#{self.class.name} replayed #{messages.size} message(s) from #{broadcasting} " \
              "since #{ReliableBroadcasting.format_timestamp(since)}"
          end
        end
      end

      def reliable_broadcasting_handlers
        @reliable_broadcasting_handlers ||= {}
      end
    end
  end
end
