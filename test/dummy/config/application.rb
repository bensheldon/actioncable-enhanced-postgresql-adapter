# frozen_string_literal: true

require "rails"
require "active_record/railtie"
require "action_controller/railtie"
require "action_view/railtie"
require "action_cable/engine"

require "turbo-rails"
require "propshaft"
require "importmap-rails"

# Loads the gem under test (and its Railtie), exactly as an application's Gemfile would.
require_relative "../../../lib/actioncable-enhanced-postgresql-adapter"

module Dummy
  class Application < Rails::Application
    config.load_defaults Rails::VERSION::STRING.to_f

    config.root = File.expand_path("..", __dir__)
    config.eager_load = false
    config.cache_classes = true

    config.secret_key_base = "dummy-secret-key-base"

    config.active_record.maintain_test_schema = false if config.active_record.respond_to?(:maintain_test_schema=)

    # Let the system test's browser connect without a matching Origin header.
    config.action_cable.disable_request_forgery_protection = true

    config.logger = Logger.new(File.expand_path("../log/test.log", __dir__))
    config.log_level = :info

    config.active_support.deprecation = :stderr

    config.hosts.clear
  end
end
