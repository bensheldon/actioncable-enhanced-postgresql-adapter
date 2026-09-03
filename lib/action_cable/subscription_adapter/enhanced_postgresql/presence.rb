# frozen_string_literal: true

# Rails integration for presence: a Channel concern that registers (and heartbeats) an
# `enhanced-presence` value for every stream a subscription is streaming from, backed by the
# adapter's #touch_presence / #remove_presence / #presences API, so an app can ask "who is
# present on this stream right now" at any moment. See the "Presence" section of the README for
# the full picture and setup instructions.
#
# This file only depends on active_support/concern (and, transitively, whatever the adapter file
# already required) - it loads fine without actionpack/actionview present, exactly like
# ReliableBroadcasting.
class ActionCable::SubscriptionAdapter::EnhancedPostgresql
  module Presence
    # The wire name for the channel param. Also used as the MessageEncryptor `purpose:` the
    # token is confined to - distinct from ReliableBroadcasting::SINCE_PARAM's purpose, so a
    # `since` token can never be mistaken for (or replayed as) a presence value, or vice versa.
    PARAM = "enhanced-presence"
    # turbo-rails forwards a `data-enhanced-presence` attribute as the channel param
    # `enhanced_presence` (it snake_cases dasherized data attribute names), so that spelling has
    # to be accepted too - see ReliableBroadcasting::SINCE_PARAM_ALTERNATIVES for the same thing.
    PARAM_ALTERNATIVES = ["enhanced_presence"].freeze
    PURPOSE = "enhanced-presence"
    # Presence values are stored in, and returned from, a plain TEXT column with no separate
    # length constraint - this is an application-level sanity limit on what a client can ask to
    # be listed as present.
    MAX_LENGTH = 255

    class << self
      # The single place the "is this a usable presence value" rules live - used both to
      # validate a decrypted frontend token (.decrypt below) and, by Channel#resolved_enhanced_presence,
      # to validate whatever a channel's (possibly overridden) #enhanced_presence returns.
      #
      # nil stays nil (that's "no presence" - not an error). Anything else is converted with
      # #to_s, then nil'd out if it's blank after stripping, or longer than MAX_LENGTH characters
      # - either way, returns a String or nil, never raises.
      def normalize(value)
        return nil if value.nil?

        string = value.to_s
        return nil if string.strip.empty? || string.length > MAX_LENGTH

        string
      end

      # value.to_s -> an encrypted-and-signed token safe to embed in HTML (a data-* attribute).
      # +encryptor+ is an ActiveSupport::MessageEncryptor - in practice the adapter's own
      # #payload_encryptor, so a client can't forge or read the presence value it sends back.
      def encrypt(value, encryptor)
        encryptor.encrypt_and_sign(value.to_s, purpose: PURPOSE)
      end

      # Inverse of .encrypt. Returns a String, or nil if +token+ is missing, blank, doesn't
      # decrypt/verify against +encryptor+ (wrong secret, wrong purpose - e.g. a `since` token
      # accidentally passed here - tampered with, or simply not a token this method produced),
      # or decrypts to something blank or longer than MAX_LENGTH characters (see .normalize).
      # Never raises: this is ultimately fed by a client-controlled channel param.
      def decrypt(token, encryptor)
        return nil unless token.respond_to?(:to_str)

        string = token.to_str
        return nil if string.empty?

        value = encryptor.decrypt_and_verify(string, purpose: PURPOSE)
        return nil unless value.is_a?(String)

        normalize(value)
      rescue ActiveSupport::MessageEncryptor::InvalidMessage, ActiveSupport::MessageVerifier::InvalidSignature, ArgumentError, TypeError
        nil
      end

      # Looks up the `enhanced-presence` param under PARAM or PARAM_ALTERNATIVES, trying both a
      # string and a symbol key for each (channel params are normally a
      # HashWithIndifferentAccess already, but this is defensive for anything that isn't).
      # Returns the raw value (nil if none of the keys are present) - callers decrypt it.
      def param_token(params)
        ([PARAM] + PARAM_ALTERNATIVES).each do |key|
          value = params[key]
          value = params[key.to_sym] if value.nil?
          return value unless value.nil?
        end

        nil
      end
    end
  end
end

require_relative "presence/channel"
