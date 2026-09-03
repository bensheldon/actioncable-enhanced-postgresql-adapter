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

# A minimal stand-in for ActionCable::Connection::Base, exposing just what
# ActionCable::Channel::Base / Streams / our concern touch: #server (for its #pubsub, #worker_pool
# and #event_loop), #logger, #identifiers, #config, #subscriptions and #transmit.
class FakeSubscriptions
  def remove_subscription(_channel)
  end
end

class FakeConnection
  attr_reader :server, :identifiers, :config, :subscriptions, :logger, :log_io

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
    assert_match(/invalid or forged/, connection.log_io.string)
  end

  # A plain ISO 8601 timestamp used to be exactly what this param expected - now that it's
  # encrypted, one is just another kind of garbage/forged input and must not be accepted as-is.
  def test_plain_unencrypted_timestamp_since_param_is_rejected
    server = build_server
    room = unique_room
    server.pubsub.broadcast(room, {"text" => "before"}.to_json)
    sleep 0.05

    connection, channel = build_channel(server, room: room, since: ReliableBroadcasting.format_timestamp(Time.now.utc))
    channel.subscribe_to_channel

    assert_confirmation(connection)
    connection.assert_no_more_transmissions
    assert_match(/invalid or forged/, connection.log_io.string)
  end

  # A token encrypted with a different secret (e.g. a forged/spoofed value, or one left over from
  # a previous secret_key_base) must not verify.
  def test_since_token_encrypted_with_a_different_secret_is_rejected
    server = build_server
    room = unique_room
    server.pubsub.broadcast(room, {"text" => "before"}.to_json)
    sleep 0.05

    forged_encryptor = ActiveSupport::MessageEncryptor.new(SecureRandom.random_bytes(32))
    forged_token = ReliableBroadcasting.encrypt_since(Time.now.utc, forged_encryptor)

    connection, channel = build_channel(server, room: room, since: forged_token)
    channel.subscribe_to_channel

    assert_confirmation(connection)
    connection.assert_no_more_transmissions
    assert_match(/invalid or forged/, connection.log_io.string)
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
    token = ReliableBroadcasting.encrypt_since(since, server.pubsub.payload_encryptor)

    server.pubsub.broadcast(room, {"text" => "first"}.to_json)
    sleep 0.05

    connection = FakeConnection.new(server)
    params = {room: room, ReliableBroadcasting::SINCE_PARAM.to_sym => token}
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
      # A Time is encrypted into a real token with the adapter's own payload_encryptor, exactly
      # like ReliableBroadcasting::Helper#action_cable_enhanced_since_param would produce; a
      # String is used as-is, to simulate a garbage/forged/unencrypted value arriving as the
      # param.
      params[param_name] = since.is_a?(Time) ? ReliableBroadcasting.encrypt_since(since, server.pubsub.payload_encryptor) : since
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

  def test_meta_tag_renders_an_encrypted_token_of_the_controllers_captured_time
    controller_time = Time.now.utc
    encryptor = build_encryptor
    view = build_view(StubController.new(controller_time), encryptor)

    tag = view.action_cable_enhanced_since_meta_tag

    assert_match(/\A<meta name="action-cable-enhanced-since" content="[^"]+"\s*\/?>\z/, tag)
    content = tag[/content="([^"]+)"/, 1]

    # Not the plain ISO 8601 string a client could read or forge - see the "Security" section
    # of the README.
    refute_match(/\A\d{4}-\d{2}-\d{2}T/, content)

    parsed = ReliableBroadcasting.decrypt_since(content, encryptor)

    refute_nil parsed
    assert_in_delta controller_time.to_f, parsed.to_f, 0.000002
  end

  def test_meta_tag_falls_back_to_now_without_a_controller_method
    encryptor = build_encryptor
    view = ActionView::Base.empty
    view.singleton_class.include(ActionCable::SubscriptionAdapter::EnhancedPostgresql::ReliableBroadcasting::Helper)
    view.define_singleton_method(:action_cable_param_encryptor) { encryptor }

    before = Time.now.utc
    tag = view.action_cable_enhanced_since_meta_tag
    after = Time.now.utc

    content = tag[/content="([^"]+)"/, 1]
    parsed = ReliableBroadcasting.decrypt_since(content, encryptor)

    refute_nil parsed
    assert_operator parsed, :>=, before
    assert_operator parsed, :<=, after
  end

  def test_action_cable_param_encryptor_raises_a_clear_error_without_the_enhanced_adapter
    view = ActionView::Base.empty
    view.singleton_class.include(ActionCable::SubscriptionAdapter::EnhancedPostgresql::ReliableBroadcasting::Helper)

    # ActionCable.server.pubsub is the "test" adapter here (see test_helper.rb), which has no
    # #payload_encryptor - i.e. the app isn't using the enhanced_postgresql adapter.
    error = assert_raises(RuntimeError) { view.action_cable_enhanced_since_param }
    assert_match(/payload_encryptor|enhanced_postgresql/, error.message)
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

  def test_encrypt_and_decrypt_since_round_trip
    time = Time.now.utc
    encryptor = ActiveSupport::MessageEncryptor.new(SecureRandom.random_bytes(32))

    token = ReliableBroadcasting.encrypt_since(time, encryptor)

    refute_match(/\A\d{4}-\d{2}-\d{2}T/, token)
    assert_in_delta time.to_f, ReliableBroadcasting.decrypt_since(token, encryptor).to_f, 0.000002
  end

  def test_decrypt_since_returns_nil_for_nil_blank_or_garbage_tokens
    encryptor = ActiveSupport::MessageEncryptor.new(SecureRandom.random_bytes(32))

    assert_nil ReliableBroadcasting.decrypt_since(nil, encryptor)
    assert_nil ReliableBroadcasting.decrypt_since("", encryptor)
    assert_nil ReliableBroadcasting.decrypt_since("garbage", encryptor)
    assert_nil ReliableBroadcasting.decrypt_since(ReliableBroadcasting.format_timestamp(Time.now.utc), encryptor)
  end

  def test_decrypt_since_returns_nil_for_a_token_encrypted_with_a_different_secret
    time = Time.now.utc
    token = ReliableBroadcasting.encrypt_since(time, ActiveSupport::MessageEncryptor.new(SecureRandom.random_bytes(32)))

    other_encryptor = ActiveSupport::MessageEncryptor.new(SecureRandom.random_bytes(32))
    assert_nil ReliableBroadcasting.decrypt_since(token, other_encryptor)
  end

  def test_decrypt_since_returns_nil_for_a_token_encrypted_with_a_different_purpose
    time = Time.now.utc
    encryptor = ActiveSupport::MessageEncryptor.new(SecureRandom.random_bytes(32))
    token = encryptor.encrypt_and_sign(ReliableBroadcasting.format_timestamp(time), purpose: "some-other-purpose")

    assert_nil ReliableBroadcasting.decrypt_since(token, encryptor)
  end

  def test_since_param_token_accepts_the_primary_name_the_alternative_name_and_symbol_keys
    assert_equal "x", ReliableBroadcasting.since_param_token({"enhanced-since" => "x"})
    assert_equal "x", ReliableBroadcasting.since_param_token({enhanced_since: "x"})
    assert_equal "x", ReliableBroadcasting.since_param_token({"enhanced_since" => "x"})
    assert_nil ReliableBroadcasting.since_param_token({"since" => "x"})
    assert_nil ReliableBroadcasting.since_param_token({})
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
