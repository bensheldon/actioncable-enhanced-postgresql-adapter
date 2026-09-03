# frozen_string_literal: true

require_relative "test_helper"
require_relative "common"
require_relative "channel_prefix"

require "active_record"

require "action_cable/subscription_adapter/enhanced_postgresql"

class PostgresqlAdapterTest < ActionCable::TestCase
  include CommonSubscriptionAdapterTest
  include ChannelPrefixTest

  def setup
    database_config = { "adapter" => "postgresql", "database" => "actioncable_enhanced_postgresql_test" }

    # Create the database unless it already exists
    begin
      ActiveRecord::Base.establish_connection database_config.merge("database" => "postgres")
      ActiveRecord::Base.connection.create_database database_config["database"], encoding: "utf8"
    rescue ActiveRecord::DatabaseAlreadyExists
    end

    # Connect to the database
    ActiveRecord::Base.establish_connection database_config
    ActiveRecord::Base.connection.connect!

    super
  end

  def teardown
    super

    ActiveRecord::Base.connection_handler.clear_all_connections!
  end

  def cable_config
    { adapter: "enhanced_postgresql", payload_encryptor_secret: SecureRandom.hex(16) }
  end

  def test_clear_active_record_connections_adapter_still_works
    server = ActionCable::Server::Base.new
    server.config.cable = cable_config.with_indifferent_access
    server.config.logger = Logger.new(StringIO.new).tap { |l| l.level = Logger::UNKNOWN }

    adapter_klass = Class.new(server.config.pubsub_adapter) do
      def active?
        !@listener.nil?
      end
    end

    adapter = adapter_klass.new(server)

    subscribe_as_queue("channel", adapter) do |queue|
      adapter.broadcast("channel", "hello world")
      assert_equal "hello world", queue.pop
    end

    ActiveRecord::Base.connection_handler.clear_reloadable_connections!

    assert adapter.active?
  end

  def test_default_subscription_connection_identifier
    subscribe_as_queue("channel") { }

    identifiers = ActiveRecord::Base.connection.exec_query("SELECT application_name FROM pg_stat_activity").rows
    assert_includes identifiers, ["ActionCable-PID-#{$$}"]
  end

  def test_custom_subscription_connection_identifier
    server = ActionCable::Server::Base.new
    server.config.cable = cable_config.merge(id: "hello-world-42").with_indifferent_access
    server.config.logger = Logger.new(StringIO.new).tap { |l| l.level = Logger::UNKNOWN }

    adapter = server.config.pubsub_adapter.new(server)

    subscribe_as_queue("channel", adapter) { }

    identifiers = ActiveRecord::Base.connection.exec_query("SELECT application_name FROM pg_stat_activity").rows
    assert_includes identifiers, ["hello-world-42"]
  end

  # Postgres has a NOTIFY payload limit of 8000 bytes which requires special handling to avoid
  # "PG::InvalidParameterValue: ERROR: payload string too long" errors.
  def test_large_payload_broadcast
    large_payloads_table = ActionCable::SubscriptionAdapter::EnhancedPostgresql::BROADCASTS_TABLE
    ActiveRecord::Base.connection_pool.with_connection do |connection|
      connection.execute("DROP TABLE IF EXISTS #{large_payloads_table}")
    end

    server = ActionCable::Server::Base.new
    server.config.cable = cable_config.with_indifferent_access
    server.config.logger = Logger.new(StringIO.new).tap { |l| l.level = Logger::UNKNOWN }
    adapter = server.config.pubsub_adapter.new(server)

    large_payload = "a" * (ActionCable::SubscriptionAdapter::EnhancedPostgresql::MAX_NOTIFY_SIZE + 1)

    subscribe_as_queue("channel", adapter) do |queue|
      adapter.broadcast("channel", large_payload)

      # The large payload is stored in the database at this point
      assert_equal 1, ActiveRecord::Base.connection.query("SELECT COUNT(*) FROM #{large_payloads_table}").first.first

      # ...but encrypted-and-signed at rest, not as the raw payload a client could read straight
      # out of the database.
      raw_payload = ActiveRecord::Base.connection.select_value("SELECT payload FROM #{large_payloads_table} LIMIT 1")
      refute_equal large_payload, raw_payload
      refute_includes raw_payload, large_payload

      assert_equal large_payload, queue.pop
    end
  end

  def test_automatic_payload_deletion
    inserts_per_delete = ActionCable::SubscriptionAdapter::EnhancedPostgresql::INSERTS_PER_DELETE
    large_payloads_table = ActionCable::SubscriptionAdapter::EnhancedPostgresql::BROADCASTS_TABLE
    large_payload = "a" * (ActionCable::SubscriptionAdapter::EnhancedPostgresql::MAX_NOTIFY_SIZE + 1)
    pg_conn = ActiveRecord::Base.connection.raw_connection

    # Prep the database so that we are one insert away from a delete. All but one entry should be old
    # enough to be reaped on the next broadcast.
    pg_conn.exec("DROP TABLE IF EXISTS #{large_payloads_table}")
    pg_conn.exec(ActionCable::SubscriptionAdapter::EnhancedPostgresql::CREATE_BROADCASTS_TABLE_QUERY)

    # Insert 98 stale payloads
    (inserts_per_delete - 2).times do
      pg_conn.exec("INSERT INTO #{large_payloads_table} (channel, payload, created_at) VALUES ('channel', 'a', NOW() - INTERVAL '3 minutes') RETURNING id")
    end
    # Insert 1 fresh payload
    new_payload_id = pg_conn.exec("INSERT INTO #{large_payloads_table} (channel, payload, created_at) VALUES ('channel', 'a', NOW() - INTERVAL '1 minutes') RETURNING id").first.fetch("id")

    # Sanity check that the auto incrementing ID is what we expect
    assert_equal inserts_per_delete - 1, new_payload_id

    server = ActionCable::Server::Base.new
    server.config.cable = cable_config.with_indifferent_access
    server.config.logger = Logger.new(StringIO.new).tap { |l| l.level = Logger::UNKNOWN }
    adapter = server.config.pubsub_adapter.new(server)

    adapter.broadcast("channel", large_payload)

    remaining_payload_ids = pg_conn.query("SELECT id FROM #{large_payloads_table} ORDER BY id").values.flatten
    assert_equal [inserts_per_delete - 1, inserts_per_delete], remaining_payload_ids
  ensure
    pg_conn&.close
  end

  # Specifying url should bypass ActiveRecord and connect directly to the provided database
  def test_explicit_url_configuration
    large_payloads_table = ActionCable::SubscriptionAdapter::EnhancedPostgresql::BROADCASTS_TABLE
    explicit_database = "actioncable_enhanced_postgresql_test_explicit"

    ActiveRecord::Base.connection_pool.with_connection do |connection|
      connection.execute("CREATE DATABASE #{explicit_database}")
    rescue ActiveRecord::DatabaseAlreadyExists
    end

    pg_conn = PG::Connection.open(dbname: explicit_database)
    pg_conn.exec("DROP TABLE IF EXISTS #{large_payloads_table}")

    server = ActionCable::Server::Base.new
    server.config.cable = cable_config.merge(url: "postgres://localhost/#{explicit_database}").with_indifferent_access
    server.config.logger = Logger.new(StringIO.new).tap { |l| l.level = Logger::UNKNOWN }
    adapter = server.config.pubsub_adapter.new(server)

    large_payload = "a" * (ActionCable::SubscriptionAdapter::EnhancedPostgresql::MAX_NOTIFY_SIZE + 1)

    subscribe_as_queue("channel", adapter) do |queue|
      adapter.broadcast("channel", large_payload)

      # The large payload is stored in the database at this point
      assert_equal "1", pg_conn.query("SELECT COUNT(*) FROM #{large_payloads_table}").first.fetch("count")

      assert_equal large_payload, queue.pop
    end
  ensure
    pg_conn&.close
  end

  def test_reliable_broadcasting_stores_and_delivers_small_payloads
    drop_messages_table

    adapter = build_adapter(reliable_broadcasting: true)

    subscribe_as_queue("channel", adapter) do |queue|
      adapter.broadcast("channel", "hello world")
      assert_equal "hello world", queue.pop
    end

    assert_equal 1, ActiveRecord::Base.connection.query("SELECT COUNT(*) FROM #{ActionCable::SubscriptionAdapter::EnhancedPostgresql::BROADCASTS_TABLE}").first.first
  end

  def test_non_reliable_broadcasting_only_stores_large_payloads
    drop_messages_table

    adapter = build_adapter(reliable_broadcasting: false)
    large_payload = "a" * (ActionCable::SubscriptionAdapter::EnhancedPostgresql::MAX_NOTIFY_SIZE + 1)

    subscribe_as_queue("channel", adapter) do |queue|
      adapter.broadcast("channel", "small")
      assert_equal "small", queue.pop

      # Nothing warranted storing anything yet, so the table hasn't even been created.
      assert_nil ActiveRecord::Base.connection.select_value("SELECT to_regclass('#{ActionCable::SubscriptionAdapter::EnhancedPostgresql::BROADCASTS_TABLE}')")

      adapter.broadcast("channel", large_payload)
      assert_equal large_payload, queue.pop
    end

    assert_equal 1, ActiveRecord::Base.connection.query("SELECT COUNT(*) FROM #{ActionCable::SubscriptionAdapter::EnhancedPostgresql::BROADCASTS_TABLE}").first.first
  end

  def test_reliable_broadcasting_with_large_payload_stores_once_and_delivers_intact
    drop_messages_table

    adapter = build_adapter(reliable_broadcasting: true)
    large_payload = "a" * (ActionCable::SubscriptionAdapter::EnhancedPostgresql::MAX_NOTIFY_SIZE + 1)

    subscribe_as_queue("channel", adapter) do |queue|
      adapter.broadcast("channel", large_payload)
      assert_equal large_payload, queue.pop
    end

    assert_equal 1, ActiveRecord::Base.connection.query("SELECT COUNT(*) FROM #{ActionCable::SubscriptionAdapter::EnhancedPostgresql::BROADCASTS_TABLE}").first.first
  end

  def test_reliable_broadcasting_stores_payload_encrypted_at_rest
    drop_messages_table

    adapter = build_adapter(reliable_broadcasting: true)
    t0 = Time.now.utc

    subscribe_as_queue("channel", adapter) do |queue|
      adapter.broadcast("channel", "hello world")
      assert_equal "hello world", queue.pop
    end

    raw_payload = ActiveRecord::Base.connection.select_value(
      "SELECT payload FROM #{ActionCable::SubscriptionAdapter::EnhancedPostgresql::BROADCASTS_TABLE} LIMIT 1"
    )
    refute_equal "hello world", raw_payload
    refute_includes raw_payload, "hello world"

    assert_equal ["hello world"], adapter.messages_since("channel", t0).map(&:payload)
  end

  def test_messages_since_skips_a_row_with_an_undecryptable_payload
    drop_messages_table

    adapter = build_adapter(reliable_broadcasting: true)
    t0 = Time.now.utc

    adapter.broadcast("channel", "good before")

    pg_conn = ActiveRecord::Base.connection.raw_connection
    pg_conn.exec_params(
      "INSERT INTO #{ActionCable::SubscriptionAdapter::EnhancedPostgresql::BROADCASTS_TABLE} (channel, payload, created_at) VALUES ($1, $2, CURRENT_TIMESTAMP)",
      [adapter.send(:channel_with_prefix, "channel"), "garbage-not-encrypted"]
    )

    adapter.broadcast("channel", "good after")

    assert_equal ["good before", "good after"], adapter.messages_since("channel", t0).map(&:payload)
  end

  def test_forged_large_payload_notify_is_dropped_without_killing_the_listener
    drop_messages_table

    adapter = build_adapter(reliable_broadcasting: true)
    pg_conn = ActiveRecord::Base.connection.raw_connection

    subscribe_as_queue("channel", adapter) do |queue|
      notify_channel = pg_conn.escape_identifier("channel")
      forged_message = pg_conn.escape_string("#{ActionCable::SubscriptionAdapter::EnhancedPostgresql::LARGE_PAYLOAD_PREFIX}garbage")
      pg_conn.exec("NOTIFY #{notify_channel}, '#{forged_message}'")

      sleep 0.2
      assert_empty queue

      # The listener thread (abort_on_exception = true) must still be alive and working.
      adapter.broadcast("channel", "still alive")
      assert_equal "still alive", queue.pop
    end
  end

  # Regression test: large payloads used to be stored SQL-escaped (quotes/backslashes doubled)
  # instead of raw, corrupting anything containing a quote or a backslash.
  def test_large_payload_with_quotes_and_backslashes_round_trips
    drop_messages_table

    adapter = build_adapter(reliable_broadcasting: false)
    tricky_payload = "a" * (ActionCable::SubscriptionAdapter::EnhancedPostgresql::MAX_NOTIFY_SIZE + 1) + %q{ '"\\ quotes and backslashes}

    subscribe_as_queue("channel", adapter) do |queue|
      adapter.broadcast("channel", tricky_payload)
      assert_equal tricky_payload, queue.pop
    end
  end

  def test_messages_since_returns_messages_in_order_since_given_time
    drop_messages_table

    adapter = build_adapter(reliable_broadcasting: true)

    t0 = Time.now.utc
    sleep 0.05
    adapter.broadcast("channel", "first")
    sleep 0.05
    t_between = Time.now.utc
    sleep 0.05
    adapter.broadcast("channel", "second")
    sleep 0.05
    t_after = Time.now.utc

    assert_equal ["first", "second"], adapter.messages_since("channel", t0).map(&:payload)
    assert_equal ["second"], adapter.messages_since("channel", t_between).map(&:payload)
    assert_equal [], adapter.messages_since("channel", t_after)
  end

  def test_messages_since_filters_by_channel
    drop_messages_table

    adapter = build_adapter(reliable_broadcasting: true)
    t0 = Time.now.utc

    adapter.broadcast("channel a", "a-message")
    adapter.broadcast("channel b", "b-message")

    assert_equal ["a-message"], adapter.messages_since("channel a", t0).map(&:payload)
    assert_equal ["b-message"], adapter.messages_since("channel b", t0).map(&:payload)
  end

  def test_messages_since_honours_channel_prefix
    drop_messages_table

    adapter1 = build_adapter(reliable_broadcasting: true, channel_prefix: "foo")
    adapter2 = build_adapter(reliable_broadcasting: true, channel_prefix: "bar")
    t0 = Time.now.utc

    adapter1.broadcast("channel", "from foo")
    adapter2.broadcast("channel", "from bar")

    assert_equal ["from foo"], adapter1.messages_since("channel", t0).map(&:payload)
    assert_equal ["from bar"], adapter2.messages_since("channel", t0).map(&:payload)
  end

  def test_messages_since_returns_empty_array_when_table_missing
    drop_messages_table

    adapter = build_adapter(reliable_broadcasting: true)

    assert_equal [], adapter.messages_since("channel", Time.now.utc)
  end

  def test_message_retention_configures_deletion_window
    inserts_per_delete = ActionCable::SubscriptionAdapter::EnhancedPostgresql::INSERTS_PER_DELETE
    pg_conn = ActiveRecord::Base.connection.raw_connection

    pg_conn.exec("DROP TABLE IF EXISTS #{ActionCable::SubscriptionAdapter::EnhancedPostgresql::BROADCASTS_TABLE}")
    pg_conn.exec(ActionCable::SubscriptionAdapter::EnhancedPostgresql::CREATE_BROADCASTS_TABLE_QUERY)

    # With message_retention: 10, rows older than 10s are stale.
    (inserts_per_delete - 2).times do
      pg_conn.exec("INSERT INTO #{ActionCable::SubscriptionAdapter::EnhancedPostgresql::BROADCASTS_TABLE} (channel, payload, created_at) VALUES ('channel', 'a', NOW() - INTERVAL '15 seconds') RETURNING id")
    end
    new_message_id = pg_conn.exec("INSERT INTO #{ActionCable::SubscriptionAdapter::EnhancedPostgresql::BROADCASTS_TABLE} (channel, payload, created_at) VALUES ('channel', 'a', NOW() - INTERVAL '5 seconds') RETURNING id").first.fetch("id")

    assert_equal inserts_per_delete - 1, new_message_id

    adapter = build_adapter(reliable_broadcasting: true, message_retention: 10)
    adapter.broadcast("channel", "trigger deletion")

    remaining_ids = pg_conn.query("SELECT id FROM #{ActionCable::SubscriptionAdapter::EnhancedPostgresql::BROADCASTS_TABLE} ORDER BY id").values.flatten
    assert_equal [inserts_per_delete - 1, inserts_per_delete], remaining_ids
  ensure
    pg_conn&.close
  end

  def test_messages_since_message_struct_fields
    drop_messages_table

    adapter = build_adapter(reliable_broadcasting: true, channel_prefix: "prefixed")
    t0 = Time.now.utc
    adapter.broadcast("channel", "payload")

    message = adapter.messages_since("channel", t0).first
    assert_kind_of Integer, message.id
    assert_equal "prefixed:channel", message.channel
    assert_equal "payload", message.payload
    assert_kind_of Time, message.created_at
  end

  private

  def build_adapter(config_overrides = {})
    # Explicitly build a fresh Configuration: ActionCable::Server::Base.new otherwise defaults to
    # a shared class-level Configuration instance, which would let multiple adapters built in the
    # same test stomp on each other's cable config (see ChannelPrefixTest for the same pattern).
    server = ActionCable::Server::Base.new(config: ActionCable::Server::Configuration.new)
    server.config.cable = cable_config.merge(config_overrides).with_indifferent_access
    server.config.logger = Logger.new(StringIO.new).tap { |l| l.level = Logger::UNKNOWN }
    server.config.pubsub_adapter.new(server)
  end

  def drop_messages_table
    ActiveRecord::Base.connection_pool.with_connection do |connection|
      connection.execute("DROP TABLE IF EXISTS #{ActionCable::SubscriptionAdapter::EnhancedPostgresql::BROADCASTS_TABLE}")
    end
  end
end

# Runs the whole adapter test suite again with reliable_broadcasting enabled, i.e. with every
# broadcast payload being stored in addition to being NOTIFY'd.
class ReliablePostgresqlAdapterTest < PostgresqlAdapterTest
  def cable_config
    super.merge(reliable_broadcasting: true)
  end
end

# Presence API (#touch_presence, #remove_presence, #presences) exposed directly on the adapter -
# see PresenceChannelTest in reliable_broadcasting_test.rb for the Channel concern built on top
# of this.
class PresenceAdapterTest < ActionCable::TestCase
  def setup
    database_config = { "adapter" => "postgresql", "database" => "actioncable_enhanced_postgresql_test" }

    # Create the database unless it already exists
    begin
      ActiveRecord::Base.establish_connection database_config.merge("database" => "postgres")
      ActiveRecord::Base.connection.create_database database_config["database"], encoding: "utf8"
    rescue ActiveRecord::DatabaseAlreadyExists
    end

    # Connect to the database
    ActiveRecord::Base.establish_connection database_config
    ActiveRecord::Base.connection.connect!

    super
  end

  def teardown
    super

    ActiveRecord::Base.connection_handler.clear_all_connections!
  end

  def cable_config
    { adapter: "enhanced_postgresql", payload_encryptor_secret: SecureRandom.hex(16) }
  end

  def test_touch_then_list_then_remove_presence
    drop_presences_table

    adapter = build_adapter
    room = unique_room

    adapter.touch_presence(room, "alice", "sub-1")
    assert_equal ["alice"], adapter.presences(room)

    adapter.remove_presence(room, "alice", "sub-1")
    assert_equal [], adapter.presences(room)
  end

  def test_same_presence_from_two_subscription_keys_is_listed_once
    drop_presences_table

    adapter = build_adapter
    room = unique_room

    adapter.touch_presence(room, "alice", "sub-1")
    adapter.touch_presence(room, "alice", "sub-2")

    assert_equal ["alice"], adapter.presences(room)

    # Removing one subscription's presence leaves the other's intact.
    adapter.remove_presence(room, "alice", "sub-1")
    assert_equal ["alice"], adapter.presences(room)

    adapter.remove_presence(room, "alice", "sub-2")
    assert_equal [], adapter.presences(room)
  end

  def test_presences_are_sorted_and_deduplicated_across_subscription_keys
    drop_presences_table

    adapter = build_adapter
    room = unique_room

    adapter.touch_presence(room, "bob", "sub-1")
    adapter.touch_presence(room, "alice", "sub-2")
    adapter.touch_presence(room, "alice", "sub-3")

    assert_equal ["alice", "bob"], adapter.presences(room)
  end

  def test_presences_are_isolated_by_channel
    drop_presences_table

    adapter = build_adapter

    adapter.touch_presence("room a", "alice", "sub-1")
    adapter.touch_presence("room b", "bob", "sub-2")

    assert_equal ["alice"], adapter.presences("room a")
    assert_equal ["bob"], adapter.presences("room b")
  end

  def test_presences_honours_channel_prefix
    drop_presences_table

    adapter1 = build_adapter(channel_prefix: "foo")
    adapter2 = build_adapter(channel_prefix: "bar")
    room = unique_room

    adapter1.touch_presence(room, "alice", "sub-1")
    adapter2.touch_presence(room, "bob", "sub-2")

    assert_equal ["alice"], adapter1.presences(room)
    assert_equal ["bob"], adapter2.presences(room)
  end

  def test_presence_expires_after_ttl_without_a_heartbeat
    drop_presences_table

    adapter = build_adapter(presence_ttl: 60)
    room = unique_room
    pg_conn = ActiveRecord::Base.connection.raw_connection

    adapter.touch_presence(room, "alice", "sub-1") # ensures the table exists

    presences_table = ActionCable::SubscriptionAdapter::EnhancedPostgresql::PRESENCES_TABLE
    pg_conn.exec_params(
      "UPDATE #{presences_table} SET last_seen_at = NOW() - INTERVAL '10 minutes' WHERE channel = $1 AND presence = $2",
      [adapter.send(:channel_with_prefix, room), "alice"]
    )

    assert_equal [], adapter.presences(room)
  ensure
    pg_conn&.close
  end

  def test_the_100th_touch_deletes_stale_presences
    drop_presences_table

    inserts_per_delete = ActionCable::SubscriptionAdapter::EnhancedPostgresql::INSERTS_PER_DELETE
    presences_table = ActionCable::SubscriptionAdapter::EnhancedPostgresql::PRESENCES_TABLE
    adapter = build_adapter(presence_ttl: 60)
    room = unique_room
    pg_conn = ActiveRecord::Base.connection.raw_connection

    adapter.touch_presence(room, "stale", "sub-stale") # ensures the table exists, count = 1
    pg_conn.exec_params(
      "UPDATE #{presences_table} SET last_seen_at = NOW() - INTERVAL '10 minutes' WHERE channel = $1 AND presence = $2",
      [adapter.send(:channel_with_prefix, room), "stale"]
    )

    # Bring the touch counter up to one shy of the next multiple of INSERTS_PER_DELETE.
    (inserts_per_delete - 2).times { |i| adapter.touch_presence(room, "filler-#{i}", "sub-filler-#{i}") }

    assert_equal inserts_per_delete - 1, adapter.instance_variable_get(:@presence_touch_count)
    assert_equal 1, pg_conn.exec_params("SELECT COUNT(*) FROM #{presences_table} WHERE presence = 'stale'").first.fetch("count").to_i

    # This 100th touch triggers the periodic DELETE, reaping the stale row above (it doesn't
    # touch itself, so it isn't reaped by its own call).
    adapter.touch_presence(room, "trigger", "sub-trigger")

    assert_equal 0, pg_conn.exec_params("SELECT COUNT(*) FROM #{presences_table} WHERE presence = 'stale'").first.fetch("count").to_i
  ensure
    pg_conn&.close
  end

  def test_presences_returns_empty_array_when_table_missing
    drop_presences_table

    adapter = build_adapter

    assert_equal [], adapter.presences(unique_room)
  end

  def test_remove_presence_is_a_no_op_when_table_missing
    drop_presences_table

    adapter = build_adapter

    assert_nil adapter.remove_presence(unique_room, "alice", "sub-1")
  end

  def test_presence_ttl_and_presence_heartbeat_interval_readers
    default_adapter = build_adapter
    assert_equal 90, default_adapter.presence_ttl
    assert_equal 30, default_adapter.presence_heartbeat_interval

    configured_adapter = build_adapter(presence_ttl: 2, presence_heartbeat_interval: 0.5)
    assert_equal 2, configured_adapter.presence_ttl
    assert_in_delta 0.5, configured_adapter.presence_heartbeat_interval, 0.0001
  end

  private

  def unique_room
    "presence-test-#{SecureRandom.hex(8)}"
  end

  def build_adapter(config_overrides = {})
    server = ActionCable::Server::Base.new(config: ActionCable::Server::Configuration.new)
    server.config.cable = cable_config.merge(config_overrides).with_indifferent_access
    server.config.logger = Logger.new(StringIO.new).tap { |l| l.level = Logger::UNKNOWN }
    server.config.pubsub_adapter.new(server)
  end

  def drop_presences_table
    ActiveRecord::Base.connection_pool.with_connection do |connection|
      connection.execute("DROP TABLE IF EXISTS #{ActionCable::SubscriptionAdapter::EnhancedPostgresql::PRESENCES_TABLE}")
    end
  end
end
