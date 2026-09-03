# frozen_string_literal: true

require "pg"

# Makes sure a local PostgreSQL server is actually reachable before the test suite tries to use
# it. Every test file that talks to Postgres calls PostgresSupport.ensure_available! at load
# time, so a stopped server is started (via bin/ensure-postgres) rather than causing every test
# depending on it to be silently skipped.
module PostgresSupport
  REPO_ROOT = File.expand_path("../..", __dir__)
  ENSURE_POSTGRES_SCRIPT = File.join(REPO_ROOT, "bin", "ensure-postgres")

  RETRY_ATTEMPTS = 5
  RETRY_DELAY = 1 # seconds

  class << self
    def ensure_available!
      return if connectable?

      attempted_repair = system(ENSURE_POSTGRES_SCRIPT)
      warn "[PostgresSupport] #{ENSURE_POSTGRES_SCRIPT} exited unsuccessfully" if attempted_repair == false

      last_error = nil

      RETRY_ATTEMPTS.times do
        return if connectable? { |error| last_error = error }

        sleep RETRY_DELAY
      end

      raise "PostgreSQL is not available. Start it (bin/ensure-postgres) and re-run. " \
        "Last error: #{last_error ? "#{last_error.class}: #{last_error.message}" : "unknown"}"
    end

    private

    def connectable?
      dbname = ENV["PGDATABASE"] || "postgres"
      PG.connect(dbname: dbname).close
      true
    rescue PG::Error, RuntimeError => e
      yield e if block_given?
      false
    end
  end
end
