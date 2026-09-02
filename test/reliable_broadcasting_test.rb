# frozen_string_literal: true

require_relative "test_helper"
require_relative "postgresql_setup"

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
  include PostgresqlAdapterSetup

  CONFIRMATION_TYPE = ActionCable::INTERNAL[:message_types][:confirmation]

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

  def build_channel(server, room:, since: :none)
    connection = FakeConnection.new(server)
    params = {"room" => room}
    if since != :none
      params["since"] = since.is_a?(Time) ? ReliableBroadcasting.format_timestamp(since) : since
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

  def test_meta_tag_renders_the_controllers_captured_time
    controller_time = Time.now.utc
    view = build_view(StubController.new(controller_time))

    tag = view.action_cable_since_meta_tag

    assert_match(/\A<meta name="action-cable-since" content="[^"]+"\s*\/?>\z/, tag)
    content = tag[/content="([^"]+)"/, 1]
    parsed = ReliableBroadcasting.parse_timestamp(content)

    refute_nil parsed
    assert_in_delta controller_time.to_f, parsed.to_f, 0.000002
  end

  def test_meta_tag_falls_back_to_now_without_a_controller_method
    view = ActionView::Base.empty
    view.singleton_class.include(ActionCable::SubscriptionAdapter::EnhancedPostgresql::ReliableBroadcasting::Helper)

    before = Time.now.utc
    tag = view.action_cable_since_meta_tag
    after = Time.now.utc

    content = tag[/content="([^"]+)"/, 1]
    parsed = ReliableBroadcasting.parse_timestamp(content)

    refute_nil parsed
    assert_operator parsed, :>=, before
    assert_operator parsed, :<=, after
  end

  private

  def build_view(stub_controller)
    view = ActionView::Base.empty
    view.singleton_class.include(ActionCable::SubscriptionAdapter::EnhancedPostgresql::ReliableBroadcasting::Helper)
    view.define_singleton_method(:controller) { stub_controller }
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
