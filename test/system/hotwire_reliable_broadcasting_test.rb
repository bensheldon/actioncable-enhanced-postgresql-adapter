# frozen_string_literal: true

# An end-to-end test of the "Option B: Hotwire Turbo Streams" walkthrough from the README's
# "Reliable broadcasting" section: a real (dummy) Rails app, a real browser (via Capybara +
# Cuprite/Chromium), and a real Postgres-backed enhanced_postgresql adapter with
# reliable_broadcasting enabled.
#
# This deliberately does NOT load test/test_helper.rb, which forces
# `ActionCable.server.config.cable = { "adapter" => "test" }` - this test needs the real
# enhanced_postgresql adapter, so it boots the dummy app (test/dummy) instead.

ENV["RAILS_ENV"] ||= "test"

require_relative "../dummy/config/environment"
require "rails/test_help"
require "capybara/cuprite"
require "securerandom"

browser_path = ENV.fetch("BROWSER_PATH", "/opt/pw-browsers/chromium")

# Registered under a name ActionDispatch::SystemTesting::Driver doesn't recognize (its
# `driven_by` helper insists on re-registering :cuprite itself, dropping our options), so
# `driven_by` below just does `Capybara.current_driver = :configured_cuprite` and leaves this
# registration alone.
Capybara.register_driver :configured_cuprite do |app|
  options = {
    browser_options: { "no-sandbox" => nil, "disable-gpu" => nil },
    headless: true,
    process_timeout: 30,
    timeout: 15
  }
  options[:browser_path] = browser_path if File.exist?(browser_path)

  Capybara::Cuprite::Driver.new(app, **options)
end

Capybara.server = :puma, { Silent: true }

class HotwireReliableBroadcastingTest < ActionDispatch::SystemTestCase
  # Postgres NOTIFY only reaches LISTENers once the issuing transaction commits, so the default
  # transactional-fixtures wrapping (which never commits) would silently swallow every broadcast
  # this test makes. There are no fixtures to roll back here anyway.
  self.use_transactional_tests = false

  driven_by :configured_cuprite

  # A live broadcast (no replay involved) proves the subscription is actually up and working -
  # used in both tests below to make the "was the missing message really missing" assertion
  # meaningful rather than a false negative from a dead connection.
  def assert_live_broadcast_received(room)
    marker = "live-#{SecureRandom.hex(4)}"
    Turbo::StreamsChannel.broadcast_append_to(room, target: "messages", html: %(<div id="message_live" data-marker="#{marker}">live</div>))

    assert_selector "#message_live[data-marker='#{marker}']", wait: 10
  end

  def setup
    database_config = { "adapter" => "postgresql", "database" => "actioncable_enhanced_postgresql_test" }

    begin
      ActiveRecord::Base.establish_connection database_config.merge("database" => "postgres")
      ActiveRecord::Base.connection.create_database database_config["database"], encoding: "utf8"
    rescue ActiveRecord::DatabaseAlreadyExists
    end

    ActiveRecord::Base.establish_connection database_config

    begin
      ActiveRecord::Base.connection.connect!
    rescue
      skip "Couldn't connect to PostgreSQL: #{database_config.inspect}"
    end
  end

  def test_broadcast_during_render_is_replayed_to_the_page
    room_id = SecureRandom.hex(8)

    visit "/rooms/#{room_id}?broadcast_during_render=true"

    assert_selector "turbo-cable-stream-source[data-since]", visible: :all

    # Replayed: the controller broadcast this before the browser could possibly have
    # subscribed, yet it shows up once the subscription confirms and replay runs.
    assert_selector "#message_during_render", text: "during render", wait: 10

    # Live delivery still works after replay has happened.
    assert_live_broadcast_received("room-#{room_id}")
  end

  def test_without_since_param_the_broadcast_during_render_is_lost
    room_id = SecureRandom.hex(8)

    visit "/rooms/#{room_id}?broadcast_during_render=true&reliable=false"

    assert_no_selector "turbo-cable-stream-source[data-since]", visible: :all

    # Wait for the live message first, so a lack of #message_during_render below is meaningful
    # (i.e. the subscription really is up, it just didn't replay anything).
    assert_live_broadcast_received("room-#{room_id}")

    assert_no_selector "#message_during_render"
  end
end
