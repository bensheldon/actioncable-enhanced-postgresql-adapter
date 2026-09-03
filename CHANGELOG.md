Unreleased

- Add reliable broadcasting, so clients that subscribe after a broadcast happened can still receive it (`reliable_broadcasting` config option, `messages_since` adapter API, and `ReliableBroadcasting::Channel` / `Controller` / `Helper` concerns)
- Add `message_retention` configuration option to control how long stored messages are kept
- Rename the storage table to `action_cable_enhanced_broadcasts` (the old `action_cable_large_payloads` table is no longer used and can be dropped)
- Rename the reliable broadcasting channel param from `since` to `enhanced-since` (also accepting `enhanced_since`, what turbo-rails produces from a `data-enhanced-since` attribute) and encrypt its value with the adapter's `payload_encryptor`, so a client can no longer read or forge the timestamp it sends back. The view helper is renamed to match: `action_cable_since_param` is now `action_cable_enhanced_since_param`
- Fix a bug where large payloads containing quotes were stored SQL-escaped instead of raw
- Add presence: `Presence::Channel` registers (and heartbeats) an encrypted `enhanced-presence` value for every stream a subscription is streaming from, so `ActionCable.server.pubsub.presences("room-1")` can list who's currently there. New `presence_ttl` (default 90) and `presence_heartbeat_interval` (default 30) configuration options, `touch_presence`/`remove_presence`/`presences` adapter API, `action_cable_enhanced_presence_param` view helper, and a new `action_cable_enhanced_presences` table
- Encrypt-and-sign every payload stored in `action_cable_enhanced_broadcasts` (large payloads, and, with `reliable_broadcasting` on, every payload) at rest with the adapter's `payload_encryptor`, so a row read directly out of the database can't be read or forged. A row that fails to decrypt (e.g. after a `payload_encryptor_secret` rotation) is skipped by `messages_since` and the large-payload fetch path, logged as a warning, rather than raised or replayed
- `Presence::Channel#enhanced_presence` is now overridable: a channel can compute its presence value in Ruby (e.g. `current_user.name`) instead of trusting the frontend-supplied `enhanced-presence` param, which is only consulted if the override calls `super`. The resolved value is fetched at most once per subscription. New `Presence.normalize` module function factors out the shared blank/length validation used both by the default (frontend-token) implementation and by an override's return value

1.0.1

- Fix gemspec metadata

1.0.0

- Support > 8000 byte payloads
- Remove hard dependency on ActiveRecord
