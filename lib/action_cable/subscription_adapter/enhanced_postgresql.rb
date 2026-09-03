# freeze_string_literal: true

require "action_cable/subscription_adapter/postgresql"
require "connection_pool"
require "time"

module ActionCable
  module SubscriptionAdapter
    class EnhancedPostgresql < PostgreSQL
      MAX_NOTIFY_SIZE = 7997 # documented as 8000 bytes, but there appears to be some overhead in transit
      LARGE_PAYLOAD_PREFIX = "__large_payload:"
      # MessageEncryptor `purpose:` stored broadcast payloads (both large payloads and, with
      # reliable_broadcasting on, every payload) are encrypted-and-signed under, so they sit
      # encrypted at rest in #{BROADCASTS_TABLE} - see #encrypt_stored_payload/#decrypt_stored_payload.
      BROADCAST_PAYLOAD_PURPOSE = "enhanced-broadcast"
      INSERTS_PER_DELETE = 100 # execute DELETE query every N inserts
      DEFAULT_MESSAGE_RETENTION = 120 # seconds
      DEFAULT_PRESENCE_TTL = 90 # seconds
      DEFAULT_PRESENCE_HEARTBEAT_INTERVAL = 30 # seconds

      BROADCASTS_TABLE = "action_cable_enhanced_broadcasts"
      LEGACY_LARGE_PAYLOADS_TABLE = "action_cable_large_payloads" # only used for schema-dumper ignore + docs
      LARGE_PAYLOADS_TABLE = BROADCASTS_TABLE # backwards-compatible alias

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

      # A single stored broadcast payload.
      #
      # id:         Integer primary key of the row in #{BROADCASTS_TABLE}
      # channel:    the stored (prefixed, un-hashed) channel name
      # payload:    the decrypted, raw (unescaped) broadcast payload, as passed to #broadcast -
      #             stored, and read back, encrypted-and-signed at rest (see #payload_encryptor
      #             and #decrypt_stored_payload)
      # created_at: Time (UTC) the row was inserted, as recorded by the database
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

      # Whether every broadcast payload (not only ones exceeding the NOTIFY size limit) is
      # stored, making it possible to replay messages via #messages_since.
      def reliable_broadcasting?
        @reliable_broadcasting
      end

      # Number of seconds stored messages are retained for before being eligible for deletion.
      attr_reader :message_retention

      # Number of seconds a presence stays listed (see #presences) without a heartbeat
      # (#touch_presence) before it's considered gone.
      attr_reader :presence_ttl

      # Number of seconds between heartbeats a Presence::Channel subscription sends to keep its
      # presence alive. May be a Float.
      attr_reader :presence_heartbeat_interval

      def broadcast(channel, payload)
        channel = channel_with_prefix(channel)

        with_broadcast_connection do |pg_conn|
          notify_channel = pg_conn.escape_identifier(channel_identifier(channel))
          escaped_payload = pg_conn.escape_string(payload)
          large = escaped_payload.bytesize > MAX_NOTIFY_SIZE

          if reliable_broadcasting? || large
            # Store the payload encrypted-and-signed at rest (never the escaped form - that would
            # double quotes/backslashes for any payload containing them), so a row read straight
            # out of the database can't be read or forged - see #encrypt_stored_payload.
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

      # Returns every stored message for +channel+ (the broadcasting name as passed to
      # #broadcast; the channel_prefix, if any, is applied internally) with created_at >= +since+
      # (anything responding to #utc, e.g. a Time), ordered by id ASC. Returns [] if nothing is
      # stored, or if the broadcasts table doesn't exist (reliable_broadcasting was never enabled
      # and no large payload was ever broadcast).
      #
      # +since+ is clamped to the retention window before the query runs: a value older than
      # message_retention seconds ago (by the application clock, i.e. Time.now.utc - the same
      # clock the ReliableBroadcasting::Controller-captured timestamp a client sends back is
      # drawn from) is raised to that floor, and a value in the future is capped at now. This
      # means a `since` older than the retention window returns only the retained window -
      # which is the guarantee #messages_since already documented, just now enforced here rather
      # than left to whatever hasn't been cleaned up yet - so direct callers of #messages_since
      # get the same bound the Channel concern's replay does. A caller that genuinely wants
      # everything currently retained can pass e.g. `Time.at(0)`.
      #
      # A row whose payload column fails to decrypt (e.g. it was written under a different
      # payload_encryptor_secret, or the column somehow holds garbage) is skipped - logged as a
      # warning rather than raised - since it can't be told apart from a forged row, and there is
      # nothing sensible to replay it as.
      def messages_since(channel, since)
        channel = channel_with_prefix(channel)
        since = clamp_since_to_retention_window(since)

        with_broadcast_connection do |pg_conn|
          result = pg_conn.exec_params(SELECT_MESSAGES_SINCE_QUERY, [channel, since.iso8601(6)])

          result.filter_map do |row|
            created_at = row["created_at"]
            # ActiveRecord-backed connections (the common case) apply a type map that already
            # casts timestamptz columns to Time; a plain PG::Connection (the `url:` option)
            # returns the raw text representation instead.
            created_at = Time.parse(created_at) unless created_at.is_a?(Time)

            begin
              payload = decrypt_stored_payload(row["payload"])
            rescue ActiveSupport::MessageEncryptor::InvalidMessage, ActiveSupport::MessageVerifier::InvalidSignature
              @server.logger.warn "#{self.class.name}#messages_since skipped an undecryptable stored payload " \
                "(id=#{row["id"]}, channel=#{row["channel"]})"
              next
            end

            Message.new(
              id: row["id"].to_i,
              channel: row["channel"],
              payload: payload,
              created_at: created_at
            )
          end
        end
      rescue PG::UndefinedTable
        []
      end

      # Records (or refreshes) that +presence+ is present on +channel+ (the broadcasting name as
      # passed to #broadcast; the channel_prefix, if any, is applied internally, and the value is
      # stored un-hashed) for the given +subscription_key+ - a value unique per subscription
      # (typically Presence::Channel's SecureRandom-generated key) so that the same +presence+
      # value from two different subscriptions is tracked, and expires, independently, while
      # still only being listed once by #presences. Periodically (every INSERTS_PER_DELETE calls)
      # also deletes presences that haven't been touched within #presence_ttl seconds.
      def touch_presence(channel, presence, subscription_key)
        channel = channel_with_prefix(channel)

        with_broadcast_connection do |pg_conn|
          ensure_presences_table(pg_conn)
          exec_touch_presence(pg_conn, channel, presence, subscription_key)
        end
      end

      # Removes the presence recorded by #touch_presence for this exact +channel+/+presence+/
      # +subscription_key+ triple. A no-op (not an error) if the presences table doesn't exist
      # yet or the row is already gone.
      def remove_presence(channel, presence, subscription_key)
        channel = channel_with_prefix(channel)

        with_broadcast_connection do |pg_conn|
          pg_conn.exec_params(REMOVE_PRESENCE_QUERY, [channel, presence, subscription_key])
        end
      rescue PG::UndefinedTable
        nil
      end

      # Returns the sorted, de-duplicated list (Array<String>) of presence values currently
      # recorded for +channel+ (the broadcasting name as passed to #broadcast; channel_prefix
      # applied internally) whose most recent #touch_presence happened within #presence_ttl
      # seconds. Returns [] if nothing is recorded, or if the presences table doesn't exist
      # (no presence was ever touched).
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

      private

      # Clamps +since+ (anything responding to #utc, e.g. a Time) to
      # [Time.now.utc - message_retention, Time.now.utc] - see #messages_since.
      def clamp_since_to_retention_window(since)
        now = Time.now.utc
        floor = now - message_retention
        since = since.utc

        return floor if since < floor
        return now if since > now

        since
      end

      # Encrypts-and-signs +plaintext+ (a broadcast payload) for storage in #{BROADCASTS_TABLE},
      # under BROADCAST_PAYLOAD_PURPOSE - see #payload_encryptor.
      def encrypt_stored_payload(plaintext)
        payload_encryptor.encrypt_and_sign(plaintext, purpose: BROADCAST_PAYLOAD_PURPOSE)
      end

      # Inverse of #encrypt_stored_payload. Raises ActiveSupport::MessageEncryptor::InvalidMessage
      # or ActiveSupport::MessageVerifier::InvalidSignature if +ciphertext+ doesn't decrypt/verify
      # (wrong secret, wrong purpose, or simply garbage) - callers decide how to handle that (see
      # #messages_since and Listener#invoke_callback).
      def decrypt_stored_payload(ciphertext)
        payload_encryptor.decrypt_and_verify(ciphertext, purpose: BROADCAST_PAYLOAD_PURPOSE)
      end

      def connection_pool
        @connection_pool ||= ConnectionPool.new(size: @connection_pool_size, timeout: 5) do
          PG::Connection.new(@url)
        end
      end

      # Ensures #{BROADCASTS_TABLE} (and its indexes) exist. Runs once per adapter instance,
      # before the first insert, so table creation doesn't rely on an error inside a possibly-open
      # application transaction. Rescues the race where a concurrent adapter instance created the
      # table first.
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
        # The table was dropped at runtime (or this is the very first insert and the ensure
        # above raced with something else) - create it and try again.
        create_broadcasts_table(pg_conn)
        retry
      end

      # Ensures #{PRESENCES_TABLE} (and its index) exist. Runs once per adapter instance, mirrors
      # #ensure_broadcasts_table above.
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
        # The table was dropped at runtime - create it and try again.
        create_presences_table(pg_conn)
        retry
      end

      # Override needed to ensure we reference our local Listener class
      def listener
        @listener || @server.mutex.synchronize { @listener ||= Listener.new(self, @server.event_loop) }
      end

      class Listener < PostgreSQL::Listener
        # Runs on the Listener thread, which has abort_on_exception = true (see
        # PostgreSQL::Listener#initialize) - an unrescued exception here would take the whole
        # process down. A forged/garbage `__large_payload:` NOTIFY (a spoofed id, an id whose row
        # is gone, or a payload that fails to decrypt) is therefore never allowed to raise: it's
        # logged as a warning and the message is dropped instead of delivered.
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

          @adapter.send(:decrypt_stored_payload, row.fetch("payload"))
        rescue ActiveSupport::MessageEncryptor::InvalidMessage, ActiveSupport::MessageVerifier::InvalidSignature
          @adapter.logger.warn "#{self.class.name} dropped a `#{LARGE_PAYLOAD_PREFIX}` notification with an " \
            "undecryptable id or payload"
          nil
        end
      end
    end
  end
end

require_relative "enhanced_postgresql/reliable_broadcasting"
require_relative "enhanced_postgresql/presence"
