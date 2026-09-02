Unreleased

- Add reliable broadcasting, so clients that subscribe after a broadcast happened can still receive it (`reliable_broadcasting` config option, `messages_since` adapter API, and `ReliableBroadcasting::Channel` / `Controller` / `Helper` concerns)
- Add `message_retention` configuration option to control how long stored messages are kept
- Rename the storage table to `action_cable_messages` (the old `action_cable_large_payloads` table is no longer used and can be dropped)
- Fix a bug where large payloads containing quotes were stored SQL-escaped instead of raw

1.0.1

- Fix gemspec metadata

1.0.0

- Support > 8000 byte payloads
- Remove hard dependency on ActiveRecord
