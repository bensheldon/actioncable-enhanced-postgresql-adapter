# frozen_string_literal: true

require_relative "test_helper"

require "active_record"
require "active_support/core_ext/hash/indifferent_access"
require "active_support/tagged_logging"
require "action_controller"
require "action_view"
require "securerandom"
require "json"
require "stringio"
require "timeout"

require "action_cable/subscription_adapter/enhanced_postgresql"

ReliableBroadcasting = ActionCable::SubscriptionAdapter::EnhancedPostgresql::ReliableBroadcasting
Presence = ActionCable::SubscriptionAdapter::EnhancedPostgresql::Presence

# A minimal stand-in for ActionCable::Connection::Base, exposing just what
# ActionCable::Channel::Base / Streams / our concern touch: #server (for its #pubsub, #worker_pool
# and #event_loop), #logger, #identifiers, #config, #subscriptions and #transmit.
class FakeSubscriptions
  def remove_subscription(_channel)
  end
end

class FakeConnection
  attr_reader :server, :identifiers, :config, :subscriptions, :logger, :log_io
  # A stand-in for whatever a real app's connection would expose via `identified_by` (e.g.
  # `current_user`) - used by PresenceOverrideTestChannel below to demonstrate a channel
  # computing its own presence value in Ruby instead of trusting the frontend-supplied param.
  attr_accessor :current_user_name

  def initialize(server)
    @server = server
    @identifiers = []
    @config = server.config
    @subscriptions = FakeSubscriptions.new
    @log_io = StringIO.new
    tagged_logger = ActiveSupport::TaggedLogging.new(Logger.new(@log_io))
    @logger = ActionCable::Connection::TaggedLoggerProxy.new(tagged_logger, tags: [])
    @transmissions = Queue.new
  end

  def pubsub
    server.pubsub
  end

  def worker_pool
    server.worker_pool
  end

  def transmit(data)
    @transmissions << data
  end

  # Pops the next transmission, failing (instead of hanging forever) if none arrives in time.
  def pop_transmission(timeout: 3)
    Timeout.timeout(timeout) { @transmissions.pop }
  rescue Timeout::Error
    raise "Timed out after #{timeout}s waiting for a transmission"
  end

  def assert_no_more_transmissions(wait: 0.2)
    sleep wait
    raise "Expected no more transmissions, but got #{@transmissions.pop.inspect}" unless @transmissions.empty?
  end
end

class ReplayTestChannel < ActionCable::Channel::Base
  include ActionCable::SubscriptionAdapter::EnhancedPostgresql::ReliableBroadcasting::Channel

  def subscribed
    stream_from params[:room]
  end
end

class PresenceTestChannel < ActionCable::Channel::Base
  include ActionCable::SubscriptionAdapter::EnhancedPostgresql::ReliableBroadcasting::Channel
  include ActionCable::SubscriptionAdapter::EnhancedPostgresql::Presence::Channel

  def subscribed
    stream_from params[:room]
  end
end

# Demonstrates (and lets tests verify) a channel computing its own presence value in Ruby - e.g.
# from `current_user` in a real app - instead of trusting the `enhanced-presence` frontend param.
# #enhanced_presence_call_count lets tests assert the override runs at most once per subscription
# no matter how many streams it's touching or how many heartbeats go by (see
# Presence::Channel#resolved_enhanced_presence).
class PresenceOverrideTestChannel < PresenceTestChannel
  attr_accessor :enhanced_presence_call_count

  def enhanced_presence
    self.enhanced_presence_call_count = (enhanced_presence_call_count || 0) + 1
    connection.current_user_name
  end
end

class ReplayTestController < ActionController::Base
  include ActionCable::SubscriptionAdapter::EnhancedPostgresql::ReliableBroadcasting::Controller

  def index
    sleep 0.01
    inner_time = Time.now.utc
    render plain: "#{action_cable_since.iso8601(6)}|#{inner_time.iso8601(6)}"
  end
end

class ReliableBroadcastingChannelTest < ActionCable::TestCase
  CONFIRMATION_TYPE = ActionCable::INTERNAL[:message_types][:confirmation]

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

  def test_replays_messages_broadcast_before_subscribing_in_order_and_still_delivers_live_messages
    server = build_server
    room = unique_room
    since = Time.now.utc

    server.pubsub.broadcast(room, {"text" => "first"}.to_json)
    sleep 0.05
    server.pubsub.broadcast(room, {"text" => "second"}.to_json)
    sleep 0.05

    connection, channel = build_channel(server, room: room, since: since)
    channel.subscribe_to_channel

    assert_confirmation(connection)
    assert_equal({"text" => "first"}, connection.pop_transmission[:message])
    assert_equal({"text" => "second"}, connection.pop_transmission[:message])

    server.pubsub.broadcast(room, {"text" => "live"}.to_json)
    assert_equal({"text" => "live"}, connection.pop_transmission[:message])
  end

  def test_since_later_than_all_broadcasts_only_delivers_confirmation
    server = build_server
    room = unique_room

    server.pubsub.broadcast(room, {"text" => "before"}.to_json)
    sleep 0.05
    since = Time.now.utc

    connection, channel = build_channel(server, room: room, since: since)
    channel.subscribe_to_channel

    assert_confirmation(connection)
    connection.assert_no_more_transmissions
  end

  def test_missing_since_param_only_delivers_confirmation
    server = build_server
    room = unique_room
    server.pubsub.broadcast(room, {"text" => "before"}.to_json)
    sleep 0.05

    connection, channel = build_channel(server, room: room)
    channel.subscribe_to_channel

    assert_confirmation(connection)
    connection.assert_no_more_transmissions
  end

  def test_garbage_since_param_only_delivers_confirmation_without_raising
    server = build_server
    room = unique_room
    server.pubsub.broadcast(room, {"text" => "before"}.to_json)
    sleep 0.05

    connection, channel = build_channel(server, room: room, since: "yesterday")
    channel.subscribe_to_channel

    assert_confirmation(connection)
    connection.assert_no_more_transmissions
    assert_match(/invalid/, connection.log_io.string)
  end

  # `enhanced-since` is a plain ISO 8601 UTC timestamp - see ReliableBroadcasting.format_timestamp
  # - not an encrypted token: it's not secret, it only ever selects a window of messages the
  # subscriber is already authorized to receive, and the server clamps it to message_retention
  # regardless of what value it's given (see EnhancedPostgresql#messages_since). Passing the raw
  # formatted string directly (rather than going through build_channel's Time-based helper below)
  # confirms the channel accepts exactly what ReliableBroadcasting.format_timestamp /
  # ReliableBroadcasting::Helper#action_cable_enhanced_since_param produce, with no encryption
  # step in between.
  def test_plain_iso8601_timestamp_since_param_triggers_replay
    server = build_server
    room = unique_room
    since = Time.now.utc

    server.pubsub.broadcast(room, {"text" => "first"}.to_json)
    sleep 0.05

    connection, channel = build_channel(server, room: room, since: ReliableBroadcasting.format_timestamp(since))
    channel.subscribe_to_channel

    assert_confirmation(connection)
    assert_equal({"text" => "first"}, connection.pop_transmission[:message])
  end

  # turbo-rails forwards a `data-enhanced-since` attribute as the channel param `enhanced_since`
  # (see ReliableBroadcasting::SINCE_PARAM_ALTERNATIVES) - accept that spelling too.
  def test_enhanced_since_underscore_alternative_param_name_is_accepted
    server = build_server
    room = unique_room
    since = Time.now.utc

    server.pubsub.broadcast(room, {"text" => "first"}.to_json)
    sleep 0.05

    connection, channel = build_channel(server, room: room, since: since, param_name: "enhanced_since")
    channel.subscribe_to_channel

    assert_confirmation(connection)
    assert_equal({"text" => "first"}, connection.pop_transmission[:message])
  end

  # Channel params are normally a HashWithIndifferentAccess already (see #build_channel), but the
  # concern is defensive about looking the param up under a symbol key too.
  def test_since_param_as_a_symbol_key_is_accepted
    server = build_server
    room = unique_room
    since = Time.now.utc

    server.pubsub.broadcast(room, {"text" => "first"}.to_json)
    sleep 0.05

    connection = FakeConnection.new(server)
    params = {room: room, ReliableBroadcasting::SINCE_PARAM.to_sym => ReliableBroadcasting.format_timestamp(since)}
    channel = ReplayTestChannel.new(connection, '{"channel":"ReplayTestChannel"}', params)
    channel.subscribe_to_channel

    assert_confirmation(connection)
    assert_equal({"text" => "first"}, connection.pop_transmission[:message])
  end

  def test_reliable_broadcasting_disabled_on_adapter_only_delivers_confirmation_and_warns
    server = build_server(reliable_broadcasting: false)
    room = unique_room
    since = Time.now.utc

    connection, channel = build_channel(server, room: room, since: since)
    channel.subscribe_to_channel

    assert_confirmation(connection)
    connection.assert_no_more_transmissions
    assert_match(/reliable_broadcasting/, connection.log_io.string)
  end

  def test_stop_stream_from_removes_handler_bookkeeping
    server = build_server
    room = unique_room

    connection, channel = build_channel(server, room: room)
    channel.subscribe_to_channel
    assert_confirmation(connection)

    assert channel.send(:reliable_broadcasting_handlers).key?(room)
    channel.stop_stream_from(room)
    assert_not channel.send(:reliable_broadcasting_handlers).key?(room)
  end

  # ActionCable's stop_stream_from / stop_all_streams are public; the concern must not make them private.
  def test_stream_stopping_methods_remain_public
    assert ReplayTestChannel.public_method_defined?(:stop_stream_from)
    assert ReplayTestChannel.public_method_defined?(:stop_all_streams)
  end

  private

  def unique_room
    "reliable-broadcasting-test-#{SecureRandom.hex(8)}"
  end

  def build_server(overrides = {})
    server = ActionCable::Server::Base.new(config: ActionCable::Server::Configuration.new)
    server.config.cable = cable_config.merge(reliable_broadcasting: true).merge(overrides).with_indifferent_access
    server.config.logger = Logger.new(StringIO.new).tap { |l| l.level = Logger::UNKNOWN }
    server
  end

  def build_channel(server, room:, since: :none, param_name: ReliableBroadcasting::SINCE_PARAM)
    connection = FakeConnection.new(server)
    params = {"room" => room}
    if since != :none
      # A Time is formatted into a plain ISO 8601 string, exactly like
      # ReliableBroadcasting::Helper#action_cable_enhanced_since_param would produce; a String is
      # used as-is, to simulate a garbage value arriving as the param.
      params[param_name] = since.is_a?(Time) ? ReliableBroadcasting.format_timestamp(since) : since
    end
    # ActionCable normally parses subscription params from a JSON identifier via
    # ActiveSupport::JSON.decode(...).with_indifferent_access (see
    # ActionCable::Connection::Subscriptions#execute_command) - do the same here so
    # `params[:room]` in ReplayTestChannel#subscribed works like it would in production.
    channel = ReplayTestChannel.new(connection, '{"channel":"ReplayTestChannel"}', params.with_indifferent_access)
    [connection, channel]
  end

  def assert_confirmation(connection)
    transmission = connection.pop_transmission
    assert_equal CONFIRMATION_TYPE, transmission[:type]
  end
end

class ReliableBroadcastingControllerTest < ActionCable::TestCase
  def test_captures_time_before_the_action_runs
    request = ActionDispatch::TestRequest.create
    response = ActionDispatch::TestResponse.new
    controller = ReplayTestController.new

    controller.dispatch(:index, request, response)

    captured_str, inner_str = response.body.split("|")
    captured = Time.iso8601(captured_str)
    inner = Time.iso8601(inner_str)

    assert_operator captured, :<, inner
    assert_predicate captured, :utc?
  end
end

class ReliableBroadcastingHelperTest < ActionCable::TestCase
  StubController = Struct.new(:action_cable_since)

  def test_since_param_renders_a_plain_iso8601_timestamp_of_the_controllers_captured_time
    controller_time = Time.now.utc
    encryptor = build_encryptor
    view = build_view(StubController.new(controller_time), encryptor)

    param = view.action_cable_enhanced_since_param

    # A plain, readable ISO 8601 timestamp - not secret, and not encrypted - see the "Security"
    # section of the README: it only ever selects a window of already-authorized messages, and
    # the server clamps it to message_retention regardless of what value it's given.
    assert_equal ReliableBroadcasting.format_timestamp(controller_time), param

    parsed = ReliableBroadcasting.parse_timestamp(param)
    refute_nil parsed
    assert_in_delta controller_time.to_f, parsed.to_f, 0.000002
  end

  def test_since_param_falls_back_to_now_without_a_controller_method
    encryptor = build_encryptor
    view = ActionView::Base.empty
    view.singleton_class.include(ActionCable::SubscriptionAdapter::EnhancedPostgresql::ReliableBroadcasting::Helper)
    view.define_singleton_method(:action_cable_param_encryptor) { encryptor }

    before = Time.now.utc
    param = view.action_cable_enhanced_since_param
    after = Time.now.utc

    parsed = ReliableBroadcasting.parse_timestamp(param)

    refute_nil parsed
    assert_operator parsed, :>=, before
    assert_operator parsed, :<=, after
  end

  # action_cable_enhanced_since_param never touches #action_cable_param_encryptor (it's a plain
  # timestamp, not encrypted) - only action_cable_enhanced_presence_param still needs it, so
  # that's what should raise without the enhanced_postgresql adapter.
  def test_action_cable_param_encryptor_raises_a_clear_error_without_the_enhanced_adapter
    view = ActionView::Base.empty
    view.singleton_class.include(ActionCable::SubscriptionAdapter::EnhancedPostgresql::ReliableBroadcasting::Helper)

    # ActionCable.server.pubsub is the "test" adapter here (see test_helper.rb), which has no
    # #payload_encryptor - i.e. the app isn't using the enhanced_postgresql adapter.
    error = assert_raises(RuntimeError) { view.action_cable_enhanced_presence_param("alice") }
    assert_match(/payload_encryptor|enhanced_postgresql/, error.message)
  end

  def test_enhanced_presence_param_renders_an_encrypted_token_of_the_given_value
    encryptor = build_encryptor
    view = build_view(StubController.new(Time.now.utc), encryptor)

    token = view.action_cable_enhanced_presence_param("alice")

    # Not the plain value a client could read or forge - see the "Security" section of the
    # README.
    refute_equal "alice", token

    assert_equal "alice", Presence.decrypt(token, encryptor)
  end

  private

  def build_encryptor
    ActiveSupport::MessageEncryptor.new(SecureRandom.random_bytes(32))
  end

  def build_view(stub_controller, encryptor)
    view = ActionView::Base.empty
    view.singleton_class.include(ActionCable::SubscriptionAdapter::EnhancedPostgresql::ReliableBroadcasting::Helper)
    view.define_singleton_method(:controller) { stub_controller }
    view.define_singleton_method(:action_cable_param_encryptor) { encryptor }
    view
  end
end

class ReliableBroadcastingTimestampTest < ActionCable::TestCase
  def test_format_and_parse_round_trip
    time = Time.now.utc
    formatted = ReliableBroadcasting.format_timestamp(time)

    assert_match(/\A\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z\z/, formatted)
    assert_in_delta time.to_f, ReliableBroadcasting.parse_timestamp(formatted).to_f, 0.000002
  end

  def test_parse_timestamp_accepts_a_time_as_is
    time = Time.now
    assert_equal time.utc, ReliableBroadcasting.parse_timestamp(time)
  end

  def test_parse_timestamp_returns_nil_for_nil_blank_or_garbage
    assert_nil ReliableBroadcasting.parse_timestamp(nil)
    assert_nil ReliableBroadcasting.parse_timestamp("")
    assert_nil ReliableBroadcasting.parse_timestamp("yesterday")
    assert_nil ReliableBroadcasting.parse_timestamp(42)
  end

  def test_since_param_value_accepts_the_primary_name_the_alternative_name_and_symbol_keys
    assert_equal "x", ReliableBroadcasting.since_param_value({"enhanced-since" => "x"})
    assert_equal "x", ReliableBroadcasting.since_param_value({enhanced_since: "x"})
    assert_equal "x", ReliableBroadcasting.since_param_value({"enhanced_since" => "x"})
    assert_nil ReliableBroadcasting.since_param_value({"since" => "x"})
    assert_nil ReliableBroadcasting.since_param_value({})
  end
end

class PresenceModuleTest < ActionCable::TestCase
  def test_encrypt_and_decrypt_round_trip
    encryptor = build_encryptor

    token = Presence.encrypt("alice", encryptor)

    refute_equal "alice", token
    assert_equal "alice", Presence.decrypt(token, encryptor)
  end

  def test_encrypt_stringifies_the_value
    encryptor = build_encryptor

    token = Presence.encrypt(:alice, encryptor)

    assert_equal "alice", Presence.decrypt(token, encryptor)
  end

  def test_decrypt_returns_nil_for_nil_blank_or_garbage_tokens
    encryptor = build_encryptor

    assert_nil Presence.decrypt(nil, encryptor)
    assert_nil Presence.decrypt("", encryptor)
    assert_nil Presence.decrypt("garbage", encryptor)
  end

  def test_decrypt_returns_nil_for_a_blank_decrypted_value
    encryptor = build_encryptor

    assert_nil Presence.decrypt(Presence.encrypt("", encryptor), encryptor)
    assert_nil Presence.decrypt(Presence.encrypt("   ", encryptor), encryptor)
  end

  def test_decrypt_returns_nil_for_a_value_over_max_length
    encryptor = build_encryptor
    too_long = "a" * (Presence::MAX_LENGTH + 1)

    assert_nil Presence.decrypt(Presence.encrypt(too_long, encryptor), encryptor)
    assert_equal "a" * Presence::MAX_LENGTH, Presence.decrypt(Presence.encrypt("a" * Presence::MAX_LENGTH, encryptor), encryptor)
  end

  def test_decrypt_returns_nil_for_a_token_encrypted_with_a_different_secret
    token = Presence.encrypt("alice", build_encryptor)

    assert_nil Presence.decrypt(token, build_encryptor)
  end

  # `enhanced-since` is a plain, unencrypted ISO 8601 timestamp - it must never be accepted as a
  # presence token (which requires an encrypted-and-signed value under Presence::PURPOSE).
  def test_decrypt_returns_nil_for_a_since_value
    encryptor = build_encryptor
    since_value = ReliableBroadcasting.format_timestamp(Time.now.utc)

    assert_nil Presence.decrypt(since_value, encryptor)
  end

  def test_param_token_accepts_the_primary_name_the_alternative_name_and_symbol_keys
    assert_equal "x", Presence.param_token({"enhanced-presence" => "x"})
    assert_equal "x", Presence.param_token({enhanced_presence: "x"})
    assert_equal "x", Presence.param_token({"enhanced_presence" => "x"})
    assert_nil Presence.param_token({"presence" => "x"})
    assert_nil Presence.param_token({})
  end

  def test_normalize_returns_nil_for_nil
    assert_nil Presence.normalize(nil)
  end

  def test_normalize_stringifies_non_string_values
    assert_equal "42", Presence.normalize(42)
    assert_equal "alice", Presence.normalize(:alice)
  end

  def test_normalize_returns_nil_for_blank_after_stripping
    assert_nil Presence.normalize("")
    assert_nil Presence.normalize("   ")
  end

  def test_normalize_returns_nil_for_a_value_over_max_length
    assert_nil Presence.normalize("a" * (Presence::MAX_LENGTH + 1))
    assert_equal "a" * Presence::MAX_LENGTH, Presence.normalize("a" * Presence::MAX_LENGTH)
  end

  def test_normalize_passes_through_an_ordinary_string
    assert_equal "alice", Presence.normalize("alice")
  end

  private

  def build_encryptor
    ActiveSupport::MessageEncryptor.new(SecureRandom.random_bytes(32))
  end
end

# A Railtie smoke test: verify it loads (without actionpack/actionview or a full app boot required
# for *this* part) and registers its initializer, without actually booting a Rails::Application
# (which run_load_hooks(:action_controller_base) etc. would require).
class ReliableBroadcastingRailtieTest < ActionCable::TestCase
  def test_railtie_loads_and_registers_its_initializer
    require "active_support/all"
    require "rails/railtie"
    require_relative "../lib/railtie"

    railtie = ActionCable::SubscriptionAdapter::EnhancedPostgresql::Railtie
    assert_operator railtie, :<, Rails::Railtie
    assert_includes railtie.initializers.map(&:name), "action_cable.enhanced_postgresql_adapter"
  end
end

# Presence::Channel: touches (and heartbeats) presence for every stream a subscription is
# streaming from, and removes it again on unsubscribe. Composes with ReliableBroadcasting::Channel
# (see PresenceTestChannel above) - both override transmit_subscription_confirmation and
# stop_stream_from/stop_all_streams, each calling super.
class PresenceChannelTest < ActionCable::TestCase
  CONFIRMATION_TYPE = ActionCable::INTERNAL[:message_types][:confirmation]

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

    # Every #build_server below spins up its own worker pool, event loop and (once a channel
    # subscribes) a Postgres LISTEN thread - none of which ActionCable ever tears down on its
    # own outside of a real Rack server shutdown. Left alone, those background threads pile up
    # for the rest of the process (every subsequent test in this file builds more on top), and
    # the growing thread count adds enough GVL scheduling noise to occasionally delay a
    # worker-pool job (a registration, a heartbeat) past a test's poll deadline - most likely to
    # bite a test like #test_two_connections_with_different_presences_are_both_listed, which
    # needs *two* independent async registrations to land inside the same window. Tracking every
    # server built here and shutting each one down in #teardown keeps the number of live
    # background threads bounded to what the *current* test needs, closing that gap.
    @built_servers = []

    super
  end

  def teardown
    super

    @built_servers.each do |server|
      # #restart halts the worker pool (fast, synchronous) and shuts down pubsub - but the
      # latter joins the adapter's Listener thread, which only wakes up (and notices the
      # shutdown request) at its next ~1s `wait_for_notify` poll. Do that part on a throwaway
      # thread instead of blocking here, so a slow-to-notice Listener doesn't turn every single
      # test's teardown into an extra ~1s of dead time - we only need these torn down before
      # *too many* accumulate, not before the next test starts.
      server.worker_pool.halt if server.instance_variable_get(:@worker_pool)
      Thread.new { server.pubsub.shutdown } if server.instance_variable_get(:@pubsub)

      event_loop = server.instance_variable_get(:@event_loop)
      if event_loop
        event_loop.stop
        # #stop only flags the reactor thread to exit at its next wakeup - it doesn't touch the
        # separate thread pool executor StreamEventLoop spawns (and never tears down itself) the
        # first time anything is #post-ed to it, e.g. to deliver our own subscription
        # confirmation. Reach in and shut that down too, or it's one more thread leaked per test.
        event_loop.instance_variable_get(:@executor)&.shutdown
      end
    end

    ActiveRecord::Base.connection_handler.clear_all_connections!
  end

  def cable_config
    { adapter: "enhanced_postgresql", payload_encryptor_secret: SecureRandom.hex(16) }
  end

  def test_presence_is_listed_after_subscription_confirms
    server = build_server
    room = unique_room

    connection, channel = build_channel(server, room: room, presence: "alice")
    channel.subscribe_to_channel

    assert_confirmation(connection)
    assert wait_for_presences(server, room, ["alice"])
  end

  def test_two_connections_with_different_presences_are_both_listed
    server = build_server
    room = unique_room

    connection1, channel1 = build_channel(server, room: room, presence: "alice")
    channel1.subscribe_to_channel
    assert_confirmation(connection1)

    connection2, channel2 = build_channel(server, room: room, presence: "bob")
    channel2.subscribe_to_channel
    assert_confirmation(connection2)

    assert wait_for_presences(server, room, ["alice", "bob"])
  ensure
    channel1&.unsubscribe_from_channel
    channel2&.unsubscribe_from_channel
  end

  def test_same_presence_from_two_connections_is_listed_once_and_survives_one_unsubscribing
    server = build_server
    room = unique_room

    connection1, channel1 = build_channel(server, room: room, presence: "alice")
    channel1.subscribe_to_channel
    assert_confirmation(connection1)

    connection2, channel2 = build_channel(server, room: room, presence: "alice")
    channel2.subscribe_to_channel
    assert_confirmation(connection2)

    assert wait_for_presences(server, room, ["alice"])

    channel1.unsubscribe_from_channel
    # The second connection's "alice" is a separate, independently tracked row (distinct
    # subscription_key) - it's still there.
    assert wait_for_presences(server, room, ["alice"])

    channel2.unsubscribe_from_channel
    assert wait_for_presences(server, room, [])
  end

  def test_presence_is_removed_on_unsubscribe_from_channel
    server = build_server
    room = unique_room

    connection, channel = build_channel(server, room: room, presence: "alice")
    channel.subscribe_to_channel
    assert_confirmation(connection)
    assert wait_for_presences(server, room, ["alice"])

    channel.unsubscribe_from_channel
    assert wait_for_presences(server, room, [])
  end

  def test_heartbeat_keeps_presence_alive_past_the_ttl
    server = build_server(presence_ttl: 2, presence_heartbeat_interval: 0.5)
    room = unique_room

    connection, channel = build_channel(server, room: room, presence: "alice")
    channel.subscribe_to_channel
    assert_confirmation(connection)
    assert wait_for_presences(server, room, ["alice"])

    sleep 3

    assert_equal ["alice"], server.pubsub.presences(room)
  ensure
    channel&.unsubscribe_from_channel
  end

  # Simulates a crashed process: the heartbeat timer stops firing (as it would if the connection
  # simply vanished without a clean unsubscribe), but nothing removes the row. It should still
  # expire on its own once presence_ttl elapses without a heartbeat.
  def test_presence_expires_after_ttl_once_the_timer_stops_without_a_clean_unsubscribe
    server = build_server(presence_ttl: 2, presence_heartbeat_interval: 0.5)
    room = unique_room

    connection, channel = build_channel(server, room: room, presence: "alice")
    channel.subscribe_to_channel
    assert_confirmation(connection)
    assert wait_for_presences(server, room, ["alice"])

    timer = channel.instance_variable_get(:@enhanced_presence_timer)
    refute_nil timer
    timer.shutdown

    sleep 3

    assert_equal [], server.pubsub.presences(room)
  end

  def test_garbage_presence_token_stores_nothing_and_warns
    server = build_server
    room = unique_room

    connection, channel = build_channel(server, room: room, presence_token: "garbage-not-a-token")
    channel.subscribe_to_channel
    assert_confirmation(connection)

    sleep 0.3
    assert_equal [], server.pubsub.presences(room)
    assert_match(/invalid or forged/, connection.log_io.string)
  end

  # `enhanced-since` is a plain, unencrypted ISO 8601 timestamp - it must not be accepted as a
  # presence token, which requires an encrypted-and-signed value (see Presence::PURPOSE).
  def test_since_token_is_rejected_as_a_presence_token
    server = build_server
    room = unique_room
    since_value = ReliableBroadcasting.format_timestamp(Time.now.utc)

    connection, channel = build_channel(server, room: room, presence_token: since_value)
    channel.subscribe_to_channel
    assert_confirmation(connection)

    sleep 0.3
    assert_equal [], server.pubsub.presences(room)
    assert_match(/invalid or forged/, connection.log_io.string)
  end

  def test_no_presence_param_stores_nothing_and_starts_no_timer
    server = build_server
    room = unique_room

    connection, channel = build_channel(server, room: room)
    channel.subscribe_to_channel
    assert_confirmation(connection)

    sleep 0.3
    assert_equal [], server.pubsub.presences(room)
    assert_nil channel.instance_variable_get(:@enhanced_presence_timer)
  end

  # Registration is dispatched to the worker pool after the confirmation is sent. If the client
  # disconnects in between, the unsubscribe cleanup runs first and the late registration job must
  # not leave an orphaned heartbeat timer (or rows) behind.
  def test_registration_after_unsubscribe_starts_no_timer_and_stores_nothing
    server = build_server
    room = unique_room

    _connection, channel = build_channel(server, room: room, presence: "alice")
    channel.send(:stream_from, room)

    channel.send(:stop_enhanced_presence)   # what after_unsubscribe runs
    channel.send(:start_enhanced_presence)  # the late worker pool job

    sleep 0.3
    assert_nil channel.instance_variable_get(:@enhanced_presence_timer)
    assert_equal [], server.pubsub.presences(room)
  end

  # A channel can override #enhanced_presence to compute its presence value in Ruby - e.g. from
  # `current_user` - instead of trusting the frontend-supplied param. Here there's no presence
  # param at all; the override alone is enough to register a presence.
  def test_channel_can_override_enhanced_presence_to_compute_it_in_ruby
    server = build_server
    room = unique_room

    connection, channel = build_channel(server, room: room, channel_class: PresenceOverrideTestChannel)
    connection.current_user_name = "carol"
    channel.subscribe_to_channel

    assert_confirmation(connection)
    assert wait_for_presences(server, room, ["carol"])
  ensure
    channel&.unsubscribe_from_channel
  end

  # Whatever an #enhanced_presence override returns wins outright - the frontend's param is only
  # consulted if the override calls `super`, which PresenceOverrideTestChannel does not.
  def test_channel_override_wins_over_a_valid_frontend_presence_token
    server = build_server
    room = unique_room

    connection, channel = build_channel(server, room: room, presence: "alice", channel_class: PresenceOverrideTestChannel)
    connection.current_user_name = "carol"
    channel.subscribe_to_channel

    assert_confirmation(connection)
    assert wait_for_presences(server, room, ["carol"])
  ensure
    channel&.unsubscribe_from_channel
  end

  def test_channel_override_returning_nil_stores_nothing_and_starts_no_timer
    server = build_server
    room = unique_room

    connection, channel = build_channel(server, room: room, channel_class: PresenceOverrideTestChannel)
    connection.current_user_name = nil
    channel.subscribe_to_channel

    assert_confirmation(connection)

    sleep 0.3
    assert_equal [], server.pubsub.presences(room)
    assert_nil channel.instance_variable_get(:@enhanced_presence_timer)
  end

  # #enhanced_presence is memoized (via #resolved_enhanced_presence) so an override - which might
  # be an expensive or side-effecting call - runs exactly once per subscription, no matter how
  # many heartbeats go by.
  def test_channel_override_is_invoked_only_once_per_subscription_across_heartbeats
    server = build_server(presence_heartbeat_interval: 0.3)
    room = unique_room

    connection, channel = build_channel(server, room: room, channel_class: PresenceOverrideTestChannel)
    connection.current_user_name = "carol"
    channel.subscribe_to_channel

    assert_confirmation(connection)
    assert wait_for_presences(server, room, ["carol"])

    sleep 1 # long enough for at least two more heartbeats at a 0.3s interval

    assert_equal 1, channel.enhanced_presence_call_count
  ensure
    channel&.unsubscribe_from_channel
  end

  private

  def unique_room
    "presence-channel-test-#{SecureRandom.hex(8)}"
  end

  def build_server(overrides = {})
    server = ActionCable::Server::Base.new(config: ActionCable::Server::Configuration.new)
    server.config.cable = cable_config.merge(overrides).with_indifferent_access
    server.config.logger = Logger.new(StringIO.new).tap { |l| l.level = Logger::UNKNOWN }
    @built_servers << server
    server
  end

  def build_channel(server, room:, presence: :none, presence_token: nil, channel_class: PresenceTestChannel)
    connection = FakeConnection.new(server)
    params = {"room" => room}
    if presence_token
      params[Presence::PARAM] = presence_token
    elsif presence != :none
      params[Presence::PARAM] = Presence.encrypt(presence, server.pubsub.payload_encryptor)
    end
    channel = channel_class.new(connection, '{"channel":"PresenceTestChannel"}', params.with_indifferent_access)
    [connection, channel]
  end

  def assert_confirmation(connection)
    transmission = connection.pop_transmission
    assert_equal CONFIRMATION_TYPE, transmission[:type]
  end

  # Polls server.pubsub.presences(room) until it matches +expected+ (order-independent) or
  # +timeout+ elapses, returning whether it matched - presence registration and heartbeats are
  # dispatched to the worker pool, so they don't happen synchronously with subscribe_to_channel /
  # unsubscribe_from_channel.
  def wait_for_presences(server, room, expected, timeout: 3)
    deadline = Time.now + timeout
    loop do
      actual = server.pubsub.presences(room)
      return true if actual.sort == expected.sort
      return false if Time.now > deadline

      sleep 0.05
    end
  end
end
