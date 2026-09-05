# freeze_string_literal: true

require "action_cable/subscription_adapter/postgresql"
require "active_support/concern"
require "connection_pool"
require "time"

module ActionCable
  module SubscriptionAdapter
    class EnhancedPostgresql < PostgreSQL
      MAX_NOTIFY_SIZE = 7997 # documented as 8000 bytes, but there appears to be some overhead in transit
      LARGE_PAYLOAD_PREFIX = "__large_payload:"
      BROADCAST_PAYLOAD_PURPOSE = "enhanced-broadcast"
      INSERTS_PER_DELETE = 100 # execute DELETE query every N inserts
      DEFAULT_MESSAGE_RETENTION = 120 # seconds
      DEFAULT_PRESENCE_TTL = 90 # seconds
      DEFAULT_PRESENCE_HEARTBEAT_INTERVAL = 30 # seconds

      BROADCASTS_TABLE = "action_cable_enhanced_broadcasts"

      CREATE_BROADCASTS_TABLE_QUERY = <<~SQL
        CREATE UNLOGGED TABLE IF NOT EXISTS #{BROADCASTS_TABLE} (
          id BIGSERIAL PRIMARY KEY,
          channel TEXT NOT NULL,
          payload TEXT NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
      SQL
      CREATE_CREATED_AT_INDEX_QUERY = <<~SQL
        CREATE INDEX IF NOT EXISTS index_action_cable_enhanced_broadcasts_on_created_at
        ON #{BROADCASTS_TABLE} (created_at)
      SQL
      CREATE_CHANNEL_INDEX_QUERY = <<~SQL
        CREATE INDEX IF NOT EXISTS index_action_cable_enhanced_broadcasts_on_channel_and_created_at
        ON #{BROADCASTS_TABLE} (channel, created_at)
      SQL
      INSERT_MESSAGE_QUERY = "INSERT INTO #{BROADCASTS_TABLE} (channel, payload, created_at) VALUES ($1, $2, CURRENT_TIMESTAMP) RETURNING id"
      SELECT_LARGE_PAYLOAD_QUERY = "SELECT payload FROM #{BROADCASTS_TABLE} WHERE id = $1"
      SELECT_MESSAGES_SINCE_QUERY = "SELECT id, channel, payload, created_at FROM #{BROADCASTS_TABLE} WHERE channel = $1 AND created_at >= $2::timestamptz ORDER BY id ASC"
      DELETE_STALE_MESSAGES_QUERY = "DELETE FROM #{BROADCASTS_TABLE} WHERE created_at < CURRENT_TIMESTAMP - ($1::int * INTERVAL '1 second')"

      PRESENCES_TABLE = "action_cable_enhanced_presences"

      CREATE_PRESENCES_TABLE_QUERY = <<~SQL
        CREATE UNLOGGED TABLE IF NOT EXISTS #{PRESENCES_TABLE} (
          channel TEXT NOT NULL,
          presence TEXT NOT NULL,
          subscription_key TEXT NOT NULL,
          last_seen_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY (channel, presence, subscription_key)
        )
      SQL
      CREATE_PRESENCES_LAST_SEEN_INDEX_QUERY = <<~SQL
        CREATE INDEX IF NOT EXISTS index_action_cable_enhanced_presences_on_last_seen_at
        ON #{PRESENCES_TABLE} (last_seen_at)
      SQL
      TOUCH_PRESENCE_QUERY = <<~SQL
        INSERT INTO #{PRESENCES_TABLE} (channel, presence, subscription_key, last_seen_at)
        VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
        ON CONFLICT (channel, presence, subscription_key) DO UPDATE SET last_seen_at = CURRENT_TIMESTAMP
      SQL
      REMOVE_PRESENCE_QUERY = "DELETE FROM #{PRESENCES_TABLE} WHERE channel = $1 AND presence = $2 AND subscription_key = $3"
      SELECT_PRESENCES_QUERY = <<~SQL
        SELECT DISTINCT presence FROM #{PRESENCES_TABLE}
        WHERE channel = $1 AND last_seen_at >= CURRENT_TIMESTAMP - ($2::int * INTERVAL '1 second')
        ORDER BY presence
      SQL
      DELETE_STALE_PRESENCES_QUERY = "DELETE FROM #{PRESENCES_TABLE} WHERE last_seen_at < CURRENT_TIMESTAMP - ($1::int * INTERVAL '1 second')"

      SINCE_PARAM = "enhanced-since"
      PRESENCE_PARAM = "enhanced-presence"
      PRESENCE_PURPOSE = "enhanced-presence"
      PRESENCE_MAX_LENGTH = 255

      Message = Struct.new(:id, :channel, :payload, :created_at, keyword_init: true)

      def initialize(*)
        super

        @url = @server.config.cable[:url]
        @connection_pool_size = @server.config.cable[:connection_pool_size] || ENV["RAILS_MAX_THREADS"] || 5
        @reliable_broadcasting = !!@server.config.cable[:reliable_broadcasting]
        @message_retention = Integer(@server.config.cable[:message_retention] || DEFAULT_MESSAGE_RETENTION)
        @presence_ttl = Integer(@server.config.cable[:presence_ttl] || DEFAULT_PRESENCE_TTL)
        @presence_heartbeat_interval = Float(@server.config.cable[:presence_heartbeat_interval] || DEFAULT_PRESENCE_HEARTBEAT_INTERVAL)
        @broadcasts_table_ensured = false
        @presences_table_ensured = false
        @presence_touch_count = 0
      end

      def reliable_broadcasting?
        @reliable_broadcasting
      end

      attr_reader :message_retention, :presence_ttl, :presence_heartbeat_interval

      def broadcast(channel, payload)
        channel = channel_with_prefix(channel)

        with_broadcast_connection do |pg_conn|
          notify_channel = pg_conn.escape_identifier(channel_identifier(channel))
          escaped_payload = pg_conn.escape_string(payload)
          large = escaped_payload.bytesize > MAX_NOTIFY_SIZE

          if reliable_broadcasting? || large
            # Store the raw payload, encrypted (never the escaped form) - so a row read straight
            # out of the database can't be read or forged.
            message_id = insert_broadcast(pg_conn, channel, encrypt_stored_payload(payload))

            pg_conn.exec_params(DELETE_STALE_MESSAGES_QUERY, [message_retention]) if message_id % INSERTS_PER_DELETE == 0

            if large
              # Encrypt message_id to prevent simple integer ID spoofing
              encrypted_id = payload_encryptor.encrypt_and_sign(message_id)
              escaped_payload = pg_conn.escape_string("#{LARGE_PAYLOAD_PREFIX}#{encrypted_id}")
            end
          end

          pg_conn.exec("NOTIFY #{notify_channel}, '#{escaped_payload}'")
        end
      end

      # Every stored message for +channel+ with created_at >= +since+, ordered by id ASC. []
      # if nothing is stored, or the broadcasts table doesn't exist yet.
      def messages_since(channel, since)
        channel = channel_with_prefix(channel)
        since = clamp_since_to_retention_window(since)

        with_broadcast_connection do |pg_conn|
          result = pg_conn.exec_params(SELECT_MESSAGES_SINCE_QUERY, [channel, since.iso8601(6)])

          result.filter_map do |row|
            created_at = row["created_at"]
            # ActiveRecord connections type-cast timestamptz to Time; a raw PG::Connection
            # returns the text representation instead.
            created_at = Time.parse(created_at) unless created_at.is_a?(Time)

            begin
              payload = decrypt_stored_payload(row["payload"])
            rescue ActiveSupport::MessageEncryptor::InvalidMessage, ActiveSupport::MessageVerifier::InvalidSignature
              @server.logger.warn "#{self.class.name}#messages_since skipped an undecryptable stored payload " \
                "(id=#{row["id"]}, channel=#{row["channel"]})"
              next
            end

            Message.new(id: row["id"].to_i, channel: row["channel"], payload: payload, created_at: created_at)
          end
        end
      rescue PG::UndefinedTable
        []
      end

      def touch_presence(channel, presence, subscription_key)
        channel = channel_with_prefix(channel)

        with_broadcast_connection do |pg_conn|
          ensure_presences_table(pg_conn)
          exec_touch_presence(pg_conn, channel, presence, subscription_key)
        end
      end

      def remove_presence(channel, presence, subscription_key)
        channel = channel_with_prefix(channel)

        with_broadcast_connection do |pg_conn|
          pg_conn.exec_params(REMOVE_PRESENCE_QUERY, [channel, presence, subscription_key])
        end
      rescue PG::UndefinedTable
        nil
      end

      def presences(channel)
        channel = channel_with_prefix(channel)

        with_broadcast_connection do |pg_conn|
          result = pg_conn.exec_params(SELECT_PRESENCES_QUERY, [channel, presence_ttl])
          result.map { |row| row["presence"] }
        end
      rescue PG::UndefinedTable
        []
      end

      def payload_encryptor
        @payload_encryptor ||= begin
          secret = @server.config.cable[:payload_encryptor_secret]
          secret ||= Rails.application.secret_key_base if defined? Rails
          secret ||= ENV["SECRET_KEY_BASE"]

          raise ArgumentError, "Missing payload_encryptor_secret configuration for ActionCable EnhancedPostgresql adapter. You need to either explicitly configure it in cable.yml or set the SECRET_KEY_BASE environment variable." unless secret

          secret_32_byte = Digest::SHA256.digest(secret)
          ActiveSupport::MessageEncryptor.new(secret_32_byte)
        end
      end

      def with_broadcast_connection(&block)
        return super unless @url

        connection_pool.with do |pg_conn|
          yield pg_conn
        end
      end

      # Called from the Listener thread
      def with_subscriptions_connection(&block)
        return super unless @url

        pg_conn = PG::Connection.new(@url)
        pg_conn.exec("SET application_name = #{pg_conn.escape_identifier(identifier)}")
        yield pg_conn
      ensure
        pg_conn&.close
      end

      # Time -> "2026-09-02T18:55:12.123456Z" (UTC, microsecond precision).
      def self.format_timestamp(time)
        time.utc.iso8601(6)
      end

      # Inverse of .format_timestamp. Accepts a Time or a String (ISO 8601). Returns nil (never
      # raises) for nil/blank/unparseable input.
      def self.parse_timestamp(value)
        return value.utc if value.is_a?(Time)
        return nil unless value.respond_to?(:to_str)

        string = value.to_str
        return nil if string.empty?

        Time.iso8601(string).utc
      rescue ArgumentError, TypeError
        nil
      end

      # nil stays nil; anything else is converted with #to_s, then nil'd out if blank or over
      # PRESENCE_MAX_LENGTH characters. Never raises.
      def self.normalize_presence(value)
        return nil if value.nil?

        string = value.to_s
        return nil if string.strip.empty? || string.length > PRESENCE_MAX_LENGTH

        string
      end

      # value.to_s -> an encrypted-and-signed token safe to embed in HTML.
      def encrypt_presence(value)
        payload_encryptor.encrypt_and_sign(value.to_s, purpose: PRESENCE_PURPOSE)
      end

      # Inverse of #encrypt_presence. Returns a String, or nil if +token+ is missing, blank,
      # doesn't decrypt/verify (wrong secret, wrong purpose - e.g. a `since` value - tampered
      # with, or simply garbage), or normalizes away to nil. Never raises.
      def decrypt_presence(token)
        return nil unless token.respond_to?(:to_str)

        string = token.to_str
        return nil if string.empty?

        value = payload_encryptor.decrypt_and_verify(string, purpose: PRESENCE_PURPOSE)
        return nil unless value.is_a?(String)

        self.class.normalize_presence(value)
      rescue ActiveSupport::MessageEncryptor::InvalidMessage, ActiveSupport::MessageVerifier::InvalidSignature, ArgumentError, TypeError
        nil
      end

      def encrypt_stored_payload(plaintext)
        payload_encryptor.encrypt_and_sign(plaintext, purpose: BROADCAST_PAYLOAD_PURPOSE)
      end

      def decrypt_stored_payload(ciphertext)
        payload_encryptor.decrypt_and_verify(ciphertext, purpose: BROADCAST_PAYLOAD_PURPOSE)
      end

      private

      def clamp_since_to_retention_window(since)
        now = Time.now.utc
        floor = now - message_retention
        since = since.utc

        return floor if since < floor
        return now if since > now

        since
      end

      def connection_pool
        @connection_pool ||= ConnectionPool.new(size: @connection_pool_size, timeout: 5) do
          PG::Connection.new(@url)
        end
      end

      def ensure_broadcasts_table(pg_conn)
        return if @broadcasts_table_ensured

        create_broadcasts_table(pg_conn)
        @broadcasts_table_ensured = true
      end

      def create_broadcasts_table(pg_conn)
        pg_conn.exec(CREATE_BROADCASTS_TABLE_QUERY)
        pg_conn.exec(CREATE_CREATED_AT_INDEX_QUERY)
        pg_conn.exec(CREATE_CHANNEL_INDEX_QUERY)
      rescue PG::UniqueViolation, PG::DuplicateTable
        # Concurrent creation race, table (or an index) already exists.
      end

      def insert_broadcast(pg_conn, channel, payload)
        ensure_broadcasts_table(pg_conn)

        result = pg_conn.exec_params(INSERT_MESSAGE_QUERY, [channel, payload])
        result.first.fetch("id").to_i
      rescue PG::UndefinedTable
        create_broadcasts_table(pg_conn)
        retry
      end

      def ensure_presences_table(pg_conn)
        return if @presences_table_ensured

        create_presences_table(pg_conn)
        @presences_table_ensured = true
      end

      def create_presences_table(pg_conn)
        pg_conn.exec(CREATE_PRESENCES_TABLE_QUERY)
        pg_conn.exec(CREATE_PRESENCES_LAST_SEEN_INDEX_QUERY)
      rescue PG::UniqueViolation, PG::DuplicateTable
        # Concurrent creation race, table (or its index) already exists.
      end

      def exec_touch_presence(pg_conn, channel, presence, subscription_key)
        pg_conn.exec_params(TOUCH_PRESENCE_QUERY, [channel, presence, subscription_key])
        @presence_touch_count += 1
        pg_conn.exec_params(DELETE_STALE_PRESENCES_QUERY, [presence_ttl]) if (@presence_touch_count % INSERTS_PER_DELETE).zero?
      rescue PG::UndefinedTable
        create_presences_table(pg_conn)
        retry
      end

      # Override needed to ensure we reference our local Listener class
      def listener
        @listener || @server.mutex.synchronize { @listener ||= Listener.new(self, @server.event_loop) }
      end

      class Listener < PostgreSQL::Listener
        # The Listener thread has abort_on_exception = true, so a forged/garbage
        # `__large_payload:` NOTIFY must never raise here - only be dropped.
        def invoke_callback(callback, message)
          if message.start_with?(LARGE_PAYLOAD_PREFIX)
            message = fetch_large_payload(message)
            return if message.nil?
          end

          @event_loop.post { super }
        end

        private

        def fetch_large_payload(message)
          encrypted_payload_id = message.delete_prefix(LARGE_PAYLOAD_PREFIX)
          payload_id = @adapter.payload_encryptor.decrypt_and_verify(encrypted_payload_id)

          row = @adapter.with_broadcast_connection do |pg_conn|
            pg_conn.exec_params(SELECT_LARGE_PAYLOAD_QUERY, [payload_id]).first
          end
          return nil if row.nil?

          @adapter.decrypt_stored_payload(row.fetch("payload"))
        rescue ActiveSupport::MessageEncryptor::InvalidMessage, ActiveSupport::MessageVerifier::InvalidSignature
          @adapter.logger.warn "#{self.class.name} dropped a `#{LARGE_PAYLOAD_PREFIX}` notification with an " \
            "undecryptable id or payload"
          nil
        end
      end

      module Controller
        extend ActiveSupport::Concern

        included do
          prepend_before_action :capture_action_cable_since if respond_to?(:prepend_before_action)
          helper_method :action_cable_since if respond_to?(:helper_method)
        end

        def action_cable_since
          @action_cable_since ||= Time.now.utc
        end

        private

        def capture_action_cable_since
          @action_cable_since = Time.now.utc
        end
      end

      module Helper
        def action_cable_enhanced_since_param
          time = controller.action_cable_since if respond_to?(:controller) && controller.respond_to?(:action_cable_since)
          EnhancedPostgresql.format_timestamp(time || (@action_cable_since ||= Time.now.utc))
        end

        def action_cable_enhanced_presence_param(value)
          pubsub = ActionCable.server.pubsub

          unless pubsub.respond_to?(:encrypt_presence)
            raise "ActionCable.server.pubsub (#{pubsub.class}) does not support encrypted channel params - set `adapter: enhanced_postgresql` in cable.yml."
          end

          pubsub.encrypt_presence(value)
        end
      end
    end
  end
end

require_relative "enhanced_postgresql/channel"
