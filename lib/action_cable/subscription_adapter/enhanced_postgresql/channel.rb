# frozen_string_literal: true

require "active_support/concern"
require "securerandom"

module ActionCable
  module SubscriptionAdapter
    class EnhancedPostgresql
      # Include into an ActionCable::Channel::Base subclass - typically ApplicationCable::Channel,
      # or Turbo::StreamsChannel via an initializer - to replay missed messages and/or register
      # presence for every stream the channel is streaming from.
      #
      #   module ApplicationCable
      #     class Channel < ActionCable::Channel::Base
      #       include ActionCable::SubscriptionAdapter::EnhancedPostgresql::Channel
      #       presence_identity serialize: -> { current_user&.id }
      #     end
      #   end
      module Channel
        extend ActiveSupport::Concern

        included do
          class_attribute :presence_identity_serializer, instance_writer: false, default: nil
          after_unsubscribe :stop_enhanced_presence
        end

        class_methods do
          # serialize: a Proc/lambda (instance_exec'd on the channel), a Symbol (a method name to
          # call on the channel), or any other object responding to #call (called with the channel).
          def presence_identity(serialize:)
            unless serialize.is_a?(Symbol) || serialize.respond_to?(:call)
              raise ArgumentError, "presence_identity serialize: must be a Proc, Symbol, or object responding to #call"
            end

            self.presence_identity_serializer = serialize
          end
        end

        # Time (UTC) parsed from the `enhanced-since` / `enhanced_since` channel param, or nil.
        def replay_since
          return @replay_since if defined?(@replay_since)
          value = param(EnhancedPostgresql::SINCE_PARAM, "enhanced_since")
          @replay_since = value && EnhancedPostgresql.parse_timestamp(value)
          logger.warn "#{self.class.name} received an invalid `enhanced-since` param - ignoring it" if value && @replay_since.nil?
          @replay_since
        end

        # This subscription's presence value, memoized. A declared `presence_identity` serializer
        # wins; a nil from it (or no serializer declared at all) falls back to decrypting the
        # `enhanced-presence` frontend param.
        def presence_identity
          return @presence_identity if defined?(@presence_identity)

          value = run_presence_identity_serializer
          value = decrypt_enhanced_presence_param if value.nil?
          normalized = EnhancedPostgresql.normalize_presence(value)

          if !value.nil? && normalized.nil?
            logger.warn "#{self.class.name}#presence_identity resolved a blank or over " \
              "#{EnhancedPostgresql::PRESENCE_MAX_LENGTH}-character value - ignoring it"
          end

          @presence_identity = normalized
        end

        # Streams#stop_stream_from / #stop_all_streams are public, so these overrides stay public too.
        def stop_stream_from(broadcasting)
          super
          broadcasting = String(broadcasting)
          enhanced_handlers.delete(broadcasting)
          forget_and_remove_presence(broadcasting)
        end

        def stop_all_streams
          super
          enhanced_handlers.clear
          enhanced_presence_streams.dup.each { |broadcasting| forget_and_remove_presence(broadcasting) }
        end

        private

        def param(*names)
          names.each do |name|
            value = params[name] || params[name.to_sym]
            return value unless value.nil?
          end
          nil
        end

        def run_presence_identity_serializer
          serializer = presence_identity_serializer
          return nil if serializer.nil?

          case serializer
          when Proc then instance_exec(&serializer)
          when Symbol then send(serializer)
          else serializer.call(self)
          end
        end

        def decrypt_enhanced_presence_param
          token = param(EnhancedPostgresql::PRESENCE_PARAM, "enhanced_presence")
          return nil if token.nil?

          unless pubsub.respond_to?(:decrypt_presence)
            logger.warn "#{self.class.name} received an `enhanced-presence` param, but #{pubsub.class} does not support it"
            return nil
          end

          value = pubsub.decrypt_presence(token)
          logger.warn "#{self.class.name} received an invalid or forged `enhanced-presence` param - ignoring it" if value.nil?
          value
        end

        def enhanced_presence_subscription_key
          @enhanced_presence_subscription_key ||= SecureRandom.hex(16)
        end

        # Streams#stream_handler builds the inner, synchronous handler that
        # Streams#worker_pool_stream_handler wraps for worker pool dispatch. Keep our own reference
        # per broadcasting so replay can call it directly, in id order, from the worker pool.
        def stream_handler(broadcasting, user_handler, coder: nil)
          super.tap { |handler| enhanced_handlers[String(broadcasting)] = handler }
        end

        # Confirmation is only transmitted once every LISTEN has succeeded, which is why replay and
        # presence registration hook in here; it runs on the event-loop thread, so both are
        # dispatched to the worker pool rather than run here.
        def transmit_subscription_confirmation
          already_sent = subscription_confirmation_sent?
          super
          return if already_sent
          schedule_replay
          schedule_presence_registration
        end

        def schedule_replay
          since = replay_since
          return if since.nil? || enhanced_handlers.empty?
          unless pubsub.respond_to?(:messages_since) && pubsub.reliable_broadcasting?
            logger.warn "#{self.class.name} received an `enhanced-since` param, but replay isn't available - set `reliable_broadcasting: true` in cable.yml"
            return
          end
          connection.worker_pool.async_invoke(self, :replay_missed_messages, since, connection: connection)
        end

        def replay_missed_messages(since)
          enhanced_handlers.each do |broadcasting, handler|
            messages = pubsub.messages_since(broadcasting, since)
            messages.each { |message| handler.call(message.payload) }
            logger.info "#{self.class.name} replayed #{messages.size} message(s) from #{broadcasting} since #{EnhancedPostgresql.format_timestamp(since)}" if messages.any?
          end
        end

        def enhanced_handlers
          @enhanced_handlers ||= {}
        end

        def schedule_presence_registration
          return if presence_identity.nil?

          unless pubsub.respond_to?(:touch_presence)
            logger.warn "#{self.class.name} resolved an enhanced presence identity, but #{pubsub.class} does not support presence"
            return
          end

          connection.worker_pool.async_exec(self, connection: connection) { start_enhanced_presence }
        end

        def start_enhanced_presence
          return if @enhanced_presence_stopped # a late worker pool job may run after unsubscribe
          @enhanced_presence_streams = streams.keys.dup
          touch_enhanced_presence_streams
        ensure
          start_enhanced_presence_timer unless @enhanced_presence_stopped
        end

        def start_enhanced_presence_timer
          @enhanced_presence_timer = connection.server.event_loop.timer(pubsub.presence_heartbeat_interval) do
            connection.worker_pool.async_exec(self, connection: connection) { touch_enhanced_presence_streams }
          end
        end

        def touch_enhanced_presence_streams
          enhanced_presence_streams.each do |broadcasting|
            pubsub.touch_presence(broadcasting, presence_identity, enhanced_presence_subscription_key)
          end
        rescue => e
          logger.error "#{self.class.name} failed to touch enhanced presence: #{e.class}: #{e.message}"
        end

        def enhanced_presence_streams
          @enhanced_presence_streams ||= []
        end

        def forget_and_remove_presence(broadcasting)
          return unless enhanced_presence_streams.delete(broadcasting)
          return if presence_identity.nil? || !pubsub.respond_to?(:remove_presence)
          pubsub.remove_presence(broadcasting, presence_identity, enhanced_presence_subscription_key)
        rescue => e
          logger.error "#{self.class.name} failed to remove enhanced presence: #{e.class}: #{e.message}"
        end

        # Rails' own stop_all_streams unsubscribe callback runs before ours and already empties
        # `streams`, which is why we keep our own stream lists - by now everything above has
        # typically already cleaned up; this is the backstop.
        def stop_enhanced_presence
          @enhanced_presence_stopped = true
          @enhanced_presence_timer.shutdown if @enhanced_presence_timer.respond_to?(:shutdown)
          @enhanced_presence_timer = nil
          enhanced_presence_streams.dup.each { |broadcasting| forget_and_remove_presence(broadcasting) }
        end
      end
    end
  end
end
