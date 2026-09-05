Unreleased

- Add reliable broadcasting: `reliable_broadcasting` config option, `messages_since` adapter API, and a single `EnhancedPostgresql::Channel` concern that replays missed messages via the `enhanced-since` channel param (`action_cable_enhanced_since_param` view helper)
- Add presence: `EnhancedPostgresql::Channel` also registers (and heartbeats) a presence value for every stream a subscription is on, so `ActionCable.server.pubsub.presences("room-1")` can list who's there. New `presence_ttl`/`presence_heartbeat_interval` config options, `touch_presence`/`remove_presence`/`presences` adapter API, `action_cable_enhanced_presence_param` view helper, and a `action_cable_enhanced_presences` table
- Add `presence_identity serialize: ...` class macro to compute a channel's presence value in Ruby (a Proc, Symbol, or callable) instead of trusting the frontend; it wins over the frontend param, falling back to it on nil
- Rename the storage table to `action_cable_enhanced_broadcasts` (the old `action_cable_large_payloads` table is no longer used and can be dropped)
- Encrypt-and-sign every stored payload at rest with the adapter's `payload_encryptor`, so a row read directly out of the database can't be read or forged; an undecryptable row is skipped with a warning rather than raised
- Fix a bug where large payloads containing quotes were stored SQL-escaped instead of raw

1.0.1

- Fix gemspec metadata

1.0.0

- Support > 8000 byte payloads
- Remove hard dependency on ActiveRecord
