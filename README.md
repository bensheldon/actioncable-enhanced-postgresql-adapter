# actioncable-enhanced-postgresql-adapter

An enhanced PostgreSQL adapter for ActionCable, based on the built-in one, adding:

- Broadcast payloads larger than 8000 bytes
- Reliable broadcasting: a client never misses a message broadcast between page render and subscription
- Presence: know who's currently on a stream

Not dependent on ActiveRecord (but integrates with it if available).

## Installation

```ruby
gem "actioncable-enhanced-postgresql-adapter"
```

## Configuration

```yaml
# config/cable.yml
production:
  adapter: enhanced_postgresql
  payload_encryptor_secret: <%= ENV["ACTION_CABLE_SECRET"] %>
  url: postgres://localhost/myapp_production
  connection_pool_size: 5
  reliable_broadcasting: true
  message_retention: 120
  presence_ttl: 90
  presence_heartbeat_interval: 30
```

| Option | Default | Meaning |
| --- | --- | --- |
| `payload_encryptor_secret` | `Rails.application.secret_key_base` or `SECRET_KEY_BASE` | Secret used to encrypt stored payloads and presence tokens |
| `url` | ActiveRecord's connection | Use a different Postgres database than ActiveRecord's |
| `connection_pool_size` | `RAILS_MAX_THREADS` or 5 | Size of the connection pool used with `url` |
| `reliable_broadcasting` | `false` | Store every broadcast so it can be replayed |
| `message_retention` | `120` | Seconds stored messages are kept |
| `presence_ttl` | `90` | Seconds a presence stays listed without a heartbeat |
| `presence_heartbeat_interval` | `30` | Seconds between presence heartbeats |

## Large payloads

Payloads over 8000 bytes are stored in an unlogged table, `action_cable_enhanced_broadcasts` (created on first broadcast), and the NOTIFY carries `__large_payload:<encrypted-id>` instead. The listener decrypts the id, fetches the payload, and delivers it as usual. The id is encrypted so it can't be spoofed, and the stored payload itself is encrypted-and-signed at rest. Stale rows are cleaned up every 100 inserts. If you're upgrading from an old version of this gem, the `action_cable_large_payloads` table is no longer used and can be dropped.

## Reliable broadcasting

Normally a broadcast is only delivered to clients already subscribed at that moment - anything broadcast while a page is rendering, or before the WebSocket subscription confirms, is lost. Setting `reliable_broadcasting: true` closes that gap by storing every payload and replaying what a client missed once its subscription confirms.

```ruby
# app/channels/application_cable/channel.rb
module ApplicationCable
  class Channel < ActionCable::Channel::Base
    include ActionCable::SubscriptionAdapter::EnhancedPostgresql::Channel
  end
end
```

```ruby
# config/initializers/reliable_turbo_streams.rb
Rails.application.config.to_prepare do
  Turbo::StreamsChannel.include(ActionCable::SubscriptionAdapter::EnhancedPostgresql::Channel)
end
```

```erb
<%= turbo_stream_from @room, data: { enhanced_since: action_cable_enhanced_since_param } %>
```

A hand-written channel passes the same value as a plain channel param:

```js
consumer.subscriptions.create(
  { channel: "ChatChannel", room_id: 1, "enhanced-since": messages.dataset.enhancedSince },
  {
    received(data) {
      if (document.getElementById(`message_${data.id}`)) return // keep handlers idempotent
      messages.insertAdjacentHTML("beforeend", data.html)
    }
  }
)
```

You can also query history directly:

```ruby
ActionCable.server.pubsub.messages_since("chat_1", 5.minutes.ago).each { |m| puts m.payload }
```

- Delivery is at-least-once: a reconnect replays the whole retained window again, so handlers must be idempotent.
- Replay is clamped to `message_retention` seconds, however old a `since` value is passed.
- The app server's clock and the database's clock must agree.

## Presence

Register (and heartbeat) a value for every stream a subscription is on, so you can ask who's there.

```erb
<%= turbo_stream_from @room, data: { enhanced_presence: action_cable_enhanced_presence_param(current_user.name) } %>
```

Or compute it server-side instead of trusting the frontend:

```ruby
class ChatChannel < ApplicationCable::Channel
  presence_identity serialize: -> { current_user&.name }
  # or: presence_identity serialize: :current_user_name
end
```

The serializer wins outright; if it returns `nil`, the frontend-supplied `enhanced-presence` param is used instead.

```ruby
ActionCable.server.pubsub.presences("room-1") # => ["alice", "bob"]
```

A heartbeat every `presence_heartbeat_interval` seconds keeps a presence alive; it drops off `presence_ttl` seconds after the last heartbeat (a clean unsubscribe removes it immediately).

## Security

- Stored payloads are encrypted-and-signed at rest with `payload_encryptor`.
- Presence tokens are encrypted, so a client can't impersonate another presence value.
- `enhanced-since` is plain text: it only bounds a window the subscriber could already read, and is clamped server-side regardless of what's sent.

`payload_encryptor_secret` must match across every process sharing the database.

## Development

- `bundle install`
- `bin/test` runs every test file (pass one or more files to run a subset, e.g. `bin/test test/postgresql_test.rb`); Postgres is started automatically via `bin/ensure-postgres`
