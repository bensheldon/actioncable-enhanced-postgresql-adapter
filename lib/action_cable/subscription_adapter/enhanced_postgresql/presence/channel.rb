# frozen_string_literal: true

require "active_support/concern"
require "securerandom"

class ActionCable::SubscriptionAdapter::EnhancedPostgresql
  module Presence
    # Include into an ActionCable::Channel::Base subclass - typically ApplicationCable::Channel,
    # or Turbo::StreamsChannel via an initializer - alongside ReliableBroadcasting::Channel if
    # you want both, to register (and heartbeat) an `enhanced-presence` value for every stream
    # the channel is streaming from, for as long as the subscription stays open, so that
    # #{ActionCable::SubscriptionAdapter::EnhancedPostgresql}#presences can list who's there.
    #
    #   module ApplicationCable
    #     class Channel < ActionCable::Channel::Base
    #       include ActionCable::SubscriptionAdapter::EnhancedPostgresql::Presence::Channel
    #     end
    #   end
    module Channel
      extend ActiveSupport::Concern

      included do
        after_unsubscribe :stop_enhanced_presence
      end

      # The value to register as this subscription's presence - nil means no presence at all
      # for this subscription (nothing is stored, no timer starts).
      #
      # The default implementation decrypts and returns the `enhanced-presence` (or
      # `enhanced_presence`) channel param - see Presence::PARAM / PARAM_ALTERNATIVES - accepting
      # both string and symbol keys. A forged or otherwise garbage token warns via #logger rather
      # than raising (see Presence.decrypt) and is treated the same as no param at all.
      #
      # Override this in an including channel to compute presence in Ruby instead of trusting
      # the frontend - e.g. from something the connection already knows:
      #
      #   class ChatChannel < ApplicationCable::Channel
      #     def enhanced_presence
      #       current_user&.name
      #     end
      #   end
      #
      # Whatever your override returns wins outright - the frontend's `enhanced-presence` param
      # is only consulted if your override calls `super`. The return value need not already be a
      # String: anything is normalized with Presence.normalize (nil stays nil; anything else is
      # converted with #to_s and dropped - with a #logger warning - if it's blank or longer than
      # Presence::MAX_LENGTH characters). See #resolved_enhanced_presence, which every internal
      # call site (registration, heartbeat touches, removal) uses instead of calling this method
      # directly, so an override runs exactly once per subscription no matter how many streams
      # it's touching or how many heartbeats go by.
      def enhanced_presence
        decrypt_enhanced_presence
      end

      # Streams#stop_stream_from / #stop_all_streams are public in ActionCable, so these
      # overrides stay public too (defining them below `private` would hide them on the
      # including channel) - see ReliableBroadcasting::Channel for the same pattern.
      def stop_stream_from(broadcasting)
        super
        forget_and_remove_enhanced_presence_stream(String(broadcasting))
      end

      def stop_all_streams
        super
        enhanced_presence_streams.dup.each { |broadcasting| forget_and_remove_enhanced_presence_stream(broadcasting) }
      end

      private

      # Memoized wrapper around #enhanced_presence - the public, overridable hook above. Every
      # internal call site uses this instead, so a channel's (possibly overridden) #enhanced_presence
      # - which might be an expensive or otherwise side-effecting call - runs exactly once per
      # subscription, and every call site (registration, each heartbeat, removal) agrees on the
      # same resolved value for the life of the subscription.
      #
      # Normalizes whatever #enhanced_presence returned via Presence.normalize - see there for
      # the exact rules. A value that came back non-nil but was normalized away to nil (blank
      # after stripping, or over Presence::MAX_LENGTH characters) is warned about here, since
      # #enhanced_presence's default implementation already warns for its own reason (a forged
      # token) and an overriding channel gets no other feedback that its return value was dropped.
      def resolved_enhanced_presence
        return @resolved_enhanced_presence if defined?(@resolved_enhanced_presence)

        value = enhanced_presence
        normalized = Presence.normalize(value)

        if !value.nil? && normalized.nil?
          logger.warn "#{self.class.name}#enhanced_presence returned a blank or over " \
            "#{Presence::MAX_LENGTH}-character value - ignoring it"
        end

        @resolved_enhanced_presence = normalized
      end

      # A random identifier unique to this channel instance (i.e. this one subscription), so the
      # same +enhanced_presence+ value from two different subscriptions (two tabs, two devices,
      # or just two connections) is tracked - and expires - independently, while #presences still
      # only lists it once.
      def enhanced_presence_subscription_key
        @enhanced_presence_subscription_key ||= SecureRandom.hex(16)
      end

      # Decrypts the raw `enhanced-presence` / `enhanced_presence` param, if present, against the
      # pubsub adapter's own payload_encryptor - see ReliableBroadcasting::Channel for the
      # matching `since` implementation this mirrors. Never raises.
      def decrypt_enhanced_presence
        token = Presence.param_token(params)
        return nil if token.nil?

        encryptor = enhanced_presence_encryptor
        return nil if encryptor.nil?

        value = Presence.decrypt(token, encryptor)

        if value.nil?
          logger.warn "#{self.class.name} received an invalid or forged `#{Presence::PARAM}` param - ignoring it"
        end

        value
      end

      def enhanced_presence_encryptor
        return pubsub.payload_encryptor if pubsub.respond_to?(:payload_encryptor)

        logger.warn "#{self.class.name} received a `#{Presence::PARAM}` param, but #{pubsub.class} " \
          "does not support encrypted params (no #payload_encryptor)"
        nil
      end

      # ActionCable only calls this once the subscription confirmation counter has reached zero,
      # i.e. once every pubsub#subscribe (LISTEN) for this channel has actually succeeded (see
      # ActionCable::Channel::Base#ensure_confirmation_sent and Streams#stream_from), so
      # #streams.keys below is the complete, final list of streams for this subscription. It
      # runs on the connection's event-loop thread, so registering presence - a DB write per
      # stream - is dispatched to the worker pool rather than run here.
      def transmit_subscription_confirmation
        already_sent = subscription_confirmation_sent?
        super
        schedule_enhanced_presence_registration unless already_sent
      end

      def schedule_enhanced_presence_registration
        return if resolved_enhanced_presence.nil?

        unless pubsub.respond_to?(:touch_presence)
          unless @enhanced_presence_unsupported_warned
            logger.warn "#{self.class.name} received an `#{Presence::PARAM}` param, but #{pubsub.class} does not support presence"
            @enhanced_presence_unsupported_warned = true
          end
          return
        end

        connection.worker_pool.async_exec(self, connection: connection) { start_enhanced_presence }
      end

      # Runs on a worker pool thread (see #schedule_enhanced_presence_registration above). The
      # worker pool itself rescues and logs any exception raised here (see
      # ActionCable::Server::Worker#invoke) so a failure never kills the underlying thread, but
      # we still rescue locally for a clearer log message and so a failed initial touch doesn't
      # prevent the heartbeat timer from starting.
      def start_enhanced_presence
        # The subscription may already have been torn down (client disconnected right after the
        # confirmation was sent) by the time this runs on the worker pool - in that case
        # #stop_enhanced_presence has run and there is nothing to register or heartbeat.
        return if @enhanced_presence_stopped

        @enhanced_presence_streams = streams.keys.dup
        touch_enhanced_presence_streams
      rescue => e
        logger.error "#{self.class.name} failed to register enhanced presence: #{e.class}: #{e.message}"
      ensure
        start_enhanced_presence_timer unless @enhanced_presence_stopped
      end

      def start_enhanced_presence_timer
        @enhanced_presence_timer = connection.server.event_loop.timer(pubsub.presence_heartbeat_interval) do
          connection.worker_pool.async_exec(self, connection: connection) { enhanced_presence_heartbeat }
        end
      end

      # Runs on a worker pool thread, once per #{presence_heartbeat_interval} seconds, for as
      # long as the subscription (and its timer) is alive. Rescues and logs so a transient DB
      # error (a dropped connection, a momentarily unreachable database) doesn't stop future
      # heartbeats from being attempted.
      def enhanced_presence_heartbeat
        touch_enhanced_presence_streams
      rescue => e
        logger.error "#{self.class.name} failed to send enhanced presence heartbeat: #{e.class}: #{e.message}"
      end

      def touch_enhanced_presence_streams
        enhanced_presence_streams.each do |broadcasting|
          pubsub.touch_presence(broadcasting, resolved_enhanced_presence, enhanced_presence_subscription_key)
        end
      end

      def enhanced_presence_streams
        @enhanced_presence_streams ||= []
      end

      def forget_and_remove_enhanced_presence_stream(broadcasting)
        return unless enhanced_presence_streams.delete(broadcasting)

        remove_enhanced_presence(broadcasting)
      end

      def remove_enhanced_presence(broadcasting)
        return if resolved_enhanced_presence.nil? || !pubsub.respond_to?(:remove_presence)

        pubsub.remove_presence(broadcasting, resolved_enhanced_presence, enhanced_presence_subscription_key)
      rescue => e
        logger.error "#{self.class.name} failed to remove enhanced presence: #{e.class}: #{e.message}"
      end

      # Registered via `after_unsubscribe` in `included do`. Streams#stop_all_streams (registered
      # via its own `on_unsubscribe` when ActionCable::Channel::Base itself was defined, i.e.
      # before this concern is ever included into anything) already runs first and empties
      # `streams` - which is exactly why #enhanced_presence_streams is our own list, tracked
      # independently since #start_enhanced_presence - so by the time this runs, #stop_all_streams
      # above has typically already removed and forgotten every stream. This is the backstop: it
      # always shuts the timer down, and removes anything that's somehow still left (e.g. a crash
      # recovery path that shut the timer down without going through #stop_all_streams).
      def stop_enhanced_presence
        @enhanced_presence_stopped = true

        if @enhanced_presence_timer.respond_to?(:shutdown)
          @enhanced_presence_timer.shutdown
        end
        @enhanced_presence_timer = nil

        enhanced_presence_streams.dup.each { |broadcasting| forget_and_remove_enhanced_presence_stream(broadcasting) }
      end
    end
  end
end
