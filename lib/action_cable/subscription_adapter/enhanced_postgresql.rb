# freeze_string_literal: true

require "action_cable/subscription_adapter/postgresql"
require "connection_pool"
require "time"

module ActionCable
  module SubscriptionAdapter
    class EnhancedPostgresql < PostgreSQL
      MAX_NOTIFY_SIZE = 7997 # documented as 8000 bytes, but there appears to be some overhead in transit
      LARGE_PAYLOAD_PREFIX = "__large_payload:"
      INSERTS_PER_DELETE = 100 # execute DELETE query every N inserts
      DEFAULT_MESSAGE_RETENTION = 120 # seconds

      MESSAGES_TABLE = "action_cable_messages"
      LEGACY_LARGE_PAYLOADS_TABLE = "action_cable_large_payloads" # only used for schema-dumper ignore + docs
      LARGE_PAYLOADS_TABLE = MESSAGES_TABLE # backwards-compatible alias

      CREATE_MESSAGES_TABLE_QUERY = <<~SQL
        CREATE UNLOGGED TABLE IF NOT EXISTS #{MESSAGES_TABLE} (
          id BIGSERIAL PRIMARY KEY,
          channel TEXT NOT NULL,
          payload TEXT NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
      SQL
      CREATE_CREATED_AT_INDEX_QUERY = <<~SQL
        CREATE INDEX IF NOT EXISTS index_action_cable_messages_on_created_at
        ON #{MESSAGES_TABLE} (created_at)
      SQL
      CREATE_CHANNEL_INDEX_QUERY = <<~SQL
        CREATE INDEX IF NOT EXISTS index_action_cable_messages_on_channel_and_created_at
        ON #{MESSAGES_TABLE} (channel, created_at)
      SQL
      INSERT_MESSAGE_QUERY = "INSERT INTO #{MESSAGES_TABLE} (channel, payload, created_at) VALUES ($1, $2, CURRENT_TIMESTAMP) RETURNING id"
      SELECT_LARGE_PAYLOAD_QUERY = "SELECT payload FROM #{MESSAGES_TABLE} WHERE id = $1"
      SELECT_MESSAGES_SINCE_QUERY = "SELECT id, channel, payload, created_at FROM #{MESSAGES_TABLE} WHERE channel = $1 AND created_at >= $2::timestamptz ORDER BY id ASC"
      DELETE_STALE_MESSAGES_QUERY = "DELETE FROM #{MESSAGES_TABLE} WHERE created_at < CURRENT_TIMESTAMP - ($1::int * INTERVAL '1 second')"

      # A single stored broadcast payload.
      #
      # id:         Integer primary key of the row in #{MESSAGES_TABLE}
      # channel:    the stored (prefixed, un-hashed) channel name
      # payload:    the raw (unescaped) broadcast payload, as passed to #broadcast
      # created_at: Time (UTC) the row was inserted, as recorded by the database
      Message = Struct.new(:id, :channel, :payload, :created_at, keyword_init: true)

      def initialize(*)
        super

        @url = @server.config.cable[:url]
        @connection_pool_size = @server.config.cable[:connection_pool_size] || ENV["RAILS_MAX_THREADS"] || 5
        @reliable_broadcasting = !!@server.config.cable[:reliable_broadcasting]
        @message_retention = Integer(@server.config.cable[:message_retention] || DEFAULT_MESSAGE_RETENTION)
        @messages_table_ensured = false
      end

      # Whether every broadcast payload (not only ones exceeding the NOTIFY size limit) is
      # stored, making it possible to replay messages via #messages_since.
      def reliable_broadcasting?
        @reliable_broadcasting
      end

      # Number of seconds stored messages are retained for before being eligible for deletion.
      attr_reader :message_retention

      def broadcast(channel, payload)
        channel = channel_with_prefix(channel)

        with_broadcast_connection do |pg_conn|
          notify_channel = pg_conn.escape_identifier(channel_identifier(channel))
          escaped_payload = pg_conn.escape_string(payload)
          large = escaped_payload.bytesize > MAX_NOTIFY_SIZE

          if reliable_broadcasting? || large
            # Store the RAW payload (fixes a latent bug: previously the escaped payload was
            # stored, doubling quotes/backslashes for any payload containing them).
            message_id = insert_message(pg_conn, channel, payload)

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
      # stored, or if the messages table doesn't exist (reliable_broadcasting was never enabled
      # and no large payload was ever broadcast).
      def messages_since(channel, since)
        channel = channel_with_prefix(channel)

        with_broadcast_connection do |pg_conn|
          result = pg_conn.exec_params(SELECT_MESSAGES_SINCE_QUERY, [channel, since.utc.iso8601(6)])

          result.map do |row|
            created_at = row["created_at"]
            # ActiveRecord-backed connections (the common case) apply a type map that already
            # casts timestamptz columns to Time; a plain PG::Connection (the `url:` option)
            # returns the raw text representation instead.
            created_at = Time.parse(created_at) unless created_at.is_a?(Time)

            Message.new(
              id: row["id"].to_i,
              channel: row["channel"],
              payload: row["payload"],
              created_at: created_at
            )
          end
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

      def connection_pool
        @connection_pool ||= ConnectionPool.new(size: @connection_pool_size, timeout: 5) do
          PG::Connection.new(@url)
        end
      end

      # Ensures #{MESSAGES_TABLE} (and its indexes) exist. Runs once per adapter instance, before
      # the first insert, so table creation doesn't rely on an error inside a possibly-open
      # application transaction. Rescues the race where a concurrent adapter instance created the
      # table first.
      def ensure_messages_table(pg_conn)
        return if @messages_table_ensured

        create_messages_table(pg_conn)
        @messages_table_ensured = true
      end

      def create_messages_table(pg_conn)
        pg_conn.exec(CREATE_MESSAGES_TABLE_QUERY)
        pg_conn.exec(CREATE_CREATED_AT_INDEX_QUERY)
        pg_conn.exec(CREATE_CHANNEL_INDEX_QUERY)
      rescue PG::UniqueViolation, PG::DuplicateTable
        # Concurrent creation race, table (or an index) already exists.
      end

      def insert_message(pg_conn, channel, payload)
        ensure_messages_table(pg_conn)

        result = pg_conn.exec_params(INSERT_MESSAGE_QUERY, [channel, payload])
        result.first.fetch("id").to_i
      rescue PG::UndefinedTable
        # The table was dropped at runtime (or this is the very first insert and the ensure
        # above raced with something else) - create it and try again.
        create_messages_table(pg_conn)
        retry
      end

      # Override needed to ensure we reference our local Listener class
      def listener
        @listener || @server.mutex.synchronize { @listener ||= Listener.new(self, @server.event_loop) }
      end

      class Listener < PostgreSQL::Listener
        def invoke_callback(callback, message)
          if message.start_with?(LARGE_PAYLOAD_PREFIX)
            encrypted_payload_id = message.delete_prefix(LARGE_PAYLOAD_PREFIX)
            payload_id = @adapter.payload_encryptor.decrypt_and_verify(encrypted_payload_id)

            @adapter.with_broadcast_connection do |pg_conn|
              result = pg_conn.exec_params(SELECT_LARGE_PAYLOAD_QUERY, [payload_id])
              message = result.first.fetch("payload")
            end
          end

          @event_loop.post { super }
        end
      end
    end
  end
end

require_relative "enhanced_postgresql/reliable_broadcasting"
