Unreleased

- Add reliable broadcasting, so clients that subscribe after a broadcast happened can still receive it (`reliable_broadcasting` config option, `messages_since` adapter API, and `ReliableBroadcasting::Channel` / `Controller` / `Helper` concerns)
- Add `message_retention` configuration option to control how long stored messages are kept
- Rename the storage table to `action_cable_enhanced_broadcasts` (the old `action_cable_large_payloads` table is no longer used and can be dropped)
- Rename the reliable broadcasting channel param from `since` to `enhanced-since` (also accepting `enhanced_since`, what turbo-rails produces from a `data-enhanced-since` attribute) and encrypt its value with the adapter's `payload_encryptor`, so a client can no longer read or forge the timestamp it sends back. The view helpers are renamed to match: `action_cable_since_param`/`action_cable_since_meta_tag` are now `action_cable_enhanced_since_param`/`action_cable_enhanced_since_meta_tag`
- Fix a bug where large payloads containing quotes were stored SQL-escaped instead of raw
- Add presence: `Presence::Channel` registers (and heartbeats) an encrypted `enhanced-presence` value for every stream a subscription is streaming from, so `ActionCable.server.pubsub.presences("room-1")` can list who's currently there. New `presence_ttl` (default 90) and `presence_heartbeat_interval` (default 30) configuration options, `touch_presence`/`remove_presence`/`presences` adapter API, `action_cable_enhanced_presence_param` view helper, and a new `action_cable_enhanced_presences` table

1.0.1

- Fix gemspec metadata

1.0.0

- Support > 8000 byte payloads
- Remove hard dependency on ActiveRecord
