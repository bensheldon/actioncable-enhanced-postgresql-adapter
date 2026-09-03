# actioncable-enhanced-postgresql-adapter

This gem provides an enhanced PostgreSQL adapter for ActionCable. It is based on the original PostgreSQL adapter, but includes the following enhancements:
- Ability to broadcast payloads larger than 8000 bytes
- Optional reliable broadcasting, so a client never misses a message broadcast between page render and subscription
- Not dependent on ActiveRecord (but can still integrate with it if available)

### Approach

To overcome the 8000 bytes limit, we temporarily store large payloads in an [unlogged](https://www.crunchydata.com/blog/postgresl-unlogged-tables) database table named `action_cable_enhanced_broadcasts`. The table is lazily created on first broadcast.

We then broadcast a payload in the style of `__large_payload:<encrypted-payload-id>`. The listener client then decrypts incoming ID's, fetches the original payload from the database, and replaces the temporary payload before invoking the subscriber callback.

ID encryption is done to prevent spoofing large payloads by manually broadcasting messages prefixed with `__large_payload:` with just an auto incrementing integer.

Note that payloads smaller than 8000 bytes are sent directly via NOTIFY, as per the original adapter, unless [reliable broadcasting](#reliable-broadcasting) is enabled, in which case every payload is also stored so it can be replayed later.

If you're upgrading from an earlier version of this gem, the old `action_cable_large_payloads` table is no longer used and can be dropped.

## Installation

Add this line to your application's Gemfile:

```ruby
gem "actioncable-enhanced-postgresql-adapter"
```

## Usage

In your `config/cable.yml` file, change the adapter for relevant environments to `enhanced_postgresql`:

```yaml
development:
  adapter: enhanced_postgresql

production:
  adapter: enhanced_postgresql
```

## Configuration

The following configuration options are available:

- `payload_encryptor_secret` - The secret used to encrypt large payload ID's. Defaults to `Rails.application.secret_key_base` or the `SECRET_KEY_BASE` environment variable unless explicitly specified.
- `url` - Set this if you want to use a different database than the one provided by ActiveRecord. Must be a valid PostgreSQL connection string.
- `connection_pool_size` - Set this in conjunction with `url` to set the size of the postgres connection pool used for broadcasts. Defaults to `RAILS_MAX_THREADS` environment variable or falls back to 5.
- `reliable_broadcasting` - Set this to `true` to store every broadcast payload so it can be replayed to a client that subscribes after the broadcast happened. See [Reliable broadcasting](#reliable-broadcasting). Defaults to `false`.
- `message_retention` - How long, in seconds, stored messages (large payloads and, when `reliable_broadcasting` is on, everything else) are kept before cleanup deletes them. Defaults to `120`.
- `presence_ttl` - How long, in seconds, a [presence](#presence) stays listed without a heartbeat before it's considered gone. Defaults to `90`.
- `presence_heartbeat_interval` - How often, in seconds, a subscription using `Presence::Channel` heartbeats its presence to keep it alive. Defaults to `30`.

## Performance

For payloads smaller than 8000 bytes, which should cover the majority of cases, performance is identical to the original adapter, unless `reliable_broadcasting` is enabled.

When broadcasting large payloads, or with `reliable_broadcasting` enabled, one has to consider the overhead of storing the payload in the database (and, for large payloads, fetching it back out). For low frequency broadcasting, this overhead is likely negligible. But take care if you're doing very high frequency broadcasting: with `reliable_broadcasting` enabled every broadcast costs one extra INSERT, in addition to the NOTIFY it already sends.

Note that whichever ActionCable adapter you're using, sending large payloads with high frequency is an anti-pattern. Even Redis pub/sub has [limitations](https://redis.io/docs/reference/clients/#output-buffer-limits) to be aware of.

### Cleanup of stored messages

Deletion of stale messages (`message_retention` seconds old or older, 120 by default) is triggered every 100 inserts into `action_cable_enhanced_broadcasts`. We do this by looking at the incremental ID generated on insert and checking if it is evenly divisible by 100. This approach avoids having to manually schedule cleanup jobs while striking a balance between performance and cleanup frequency.

## Reliable broadcasting

Normally, a broadcast is only delivered to clients that are already subscribed at the moment it happens. That leaves a gap: a page is rendered by a controller, sent to the browser, and only then does the client's WebSocket connect and subscribe to a channel. Anything broadcast while the page is rendering, or in the time between the response arriving and the subscription being confirmed, is lost. The same gap reopens on every reconnect.

Setting `reliable_broadcasting: true` in `cable.yml` closes that gap. When it's enabled, every broadcast payload is stored in the `action_cable_enhanced_broadcasts` table (not only ones over 8000 bytes), and the adapter exposes a `messages_since` API that a channel can use to replay anything it missed, right after its subscription is confirmed.

The flow looks like this:

1. A controller records the current time (`Time.now.utc`) before the action runs.
2. A view helper encrypts that timestamp and embeds the resulting token in the rendered HTML, either as a meta tag or a data attribute.
3. The client passes it back as the `enhanced-since` channel parameter when it subscribes.
4. Once the channel's subscription to Postgres (`LISTEN`) is confirmed, the server decrypts the token back into a timestamp and looks up every message stored for that stream with a `created_at` at or after it, delivering them through the normal stream handler, oldest first. Live messages that arrive while the replay runs are delivered as usual.

### Integrating into a Rails app

There are three moving parts: the adapter has to store messages (`cable.yml`), your channel has to replay them (the `Channel` concern), and the page has to hand the render timestamp back to the channel (a helper in the view plus the `enhanced-since` channel parameter). Controllers and views need no manual wiring in a Rails app: the gem's Railtie includes the `Controller` concern into `ActionController::Base` and registers the `Helper` module for all views.

Two complete walkthroughs follow, one for a hand-written Action Cable channel and one for Hotwire Turbo Streams. Step 1 is shared.

#### Step 1: enable message storage

```yaml
# config/cable.yml
development:
  adapter: enhanced_postgresql
  reliable_broadcasting: true

production:
  adapter: enhanced_postgresql
  reliable_broadcasting: true
  message_retention: 120 # seconds, optional, this is the default
```

Restart the server. The `action_cable_enhanced_broadcasts` table is created on the first broadcast, no migration needed.

#### Option A: a plain Action Cable channel

Include the concern once in your base channel so every channel replays missed messages when an `enhanced-since` parameter is present:

```ruby
# app/channels/application_cable/channel.rb
module ApplicationCable
  class Channel < ActionCable::Channel::Base
    include ActionCable::SubscriptionAdapter::EnhancedPostgresql::ReliableBroadcasting::Channel
  end
end
```

Your channels don't change. Replay happens automatically for every stream set up in `subscribed`, after the subscription is confirmed:

```ruby
# app/channels/chat_channel.rb
class ChatChannel < ApplicationCable::Channel
  def subscribed
    @room = Room.find(params[:room_id])
    stream_for @room
  end
end
```

Render the timestamp into the page. Put the meta tag in the `<head>` next to `action_cable_meta_tag`:

```erb
<%# app/views/layouts/application.html.erb %>
<head>
  <%= action_cable_meta_tag %>
  <%= action_cable_enhanced_since_meta_tag %>
  <%= javascript_importmap_tags %>
</head>
```

This renders `<meta name="action-cable-enhanced-since" content="...">`, where the content is an encrypted-and-signed token, not the timestamp itself - see [Security](#security) below. It's captured by a `prepend_before_action` at the very start of the request, so any broadcast that happens while your action queries the database or renders its templates is covered.

Pass it along when the JavaScript subscribes. `ActionCable.getConfig("enhanced-since")` reads the meta tag for you:

```js
// app/javascript/channels/chat_channel.js
import * as ActionCable from "@rails/actioncable"

const consumer = ActionCable.createConsumer()

consumer.subscriptions.create(
  { channel: "ChatChannel", room_id: 1, "enhanced-since": ActionCable.getConfig("enhanced-since") },
  {
    received(data) {
      // May be called more than once for the same message (see Caveats), so keep this idempotent,
      // e.g. skip the message if an element with data.id is already on the page.
      if (document.getElementById(`message_${data.id}`)) return
      document.getElementById("messages").insertAdjacentHTML("beforeend", data.html)
    }
  }
)
```

Broadcast exactly as you do today, from a model callback, a job, or a controller:

```ruby
# app/models/message.rb
class Message < ApplicationRecord
  belongs_to :room

  after_create_commit do
    ChatChannel.broadcast_to(room, id: id, html: ApplicationController.render(self))
  end
end
```

Nothing about broadcasting changes. The adapter stores the payload and sends the NOTIFY as usual, so clients that are already subscribed get the message live, and a client whose page was rendered before the message was created gets it replayed the moment its subscription is confirmed.

#### Option B: Hotwire Turbo Streams (`turbo_stream_from`)

`Turbo::StreamsChannel` is provided by turbo-rails and doesn't inherit from `ApplicationCable::Channel`, so include the concern into it directly from an initializer:

```ruby
# config/initializers/reliable_turbo_streams.rb
Rails.application.config.to_prepare do
  Turbo::StreamsChannel.include(
    ActionCable::SubscriptionAdapter::EnhancedPostgresql::ReliableBroadcasting::Channel
  )
end
```

Then pass the token wherever you call `turbo_stream_from`. turbo-rails forwards the `data-*` attributes of the rendered `<turbo-cable-stream-source>` element as channel parameters, dasherizing the attribute name on the way out and snake\_casing it back on the way in, so a `data: { enhanced_since: ... }` attribute renders as `data-enhanced-since` and arrives at the channel as `params[:enhanced_since]` - which is exactly why the `Channel` concern accepts both that and the primary `enhanced-since` spelling:

```erb
<%# app/views/rooms/show.html.erb %>
<%= turbo_stream_from @room, data: { enhanced_since: action_cable_enhanced_since_param } %>

<h1><%= @room.name %></h1>
<div id="messages">
  <%= render @room.messages %>
</div>
```

No JavaScript changes are needed; Turbo's built-in consumer handles the subscription. Broadcast with the usual Turbo helpers:

```ruby
# app/models/message.rb
class Message < ApplicationRecord
  belongs_to :room
  broadcasts_to :room
end
```

`broadcasts_to` uses `append` for creates and `replace` for updates, both keyed on the element's DOM id, so a replayed message that already made it into the rendered page is harmless: Turbo replaces the existing element rather than adding a second one.

If you render several `turbo_stream_from` tags on one page, pass `data: { enhanced_since: action_cable_enhanced_since_param }` to each of them. Every one is its own subscription and each is replayed independently.

#### Checking that it works

1. Open a page that subscribes to a channel and confirm the rendered HTML contains the token (`action-cable-enhanced-since` meta tag or a `data-enhanced-since` attribute) - and that it is *not* a readable timestamp.
2. In the Rails log for the WebSocket connection you should see `ChatChannel is streaming from ...` followed by `ChatChannel replayed N message(s) from ... since ...` whenever there was something to catch up on.
3. To reproduce the race on purpose, add `sleep 5` to the controller action, load the page, and broadcast to the stream from a Rails console during those five seconds. The broadcast shows up in the browser as soon as the page connects.

#### Outside a Rails `ActionController::Base` controller

The Railtie only wires up `ActionController::Base`. If you render HTML from an `ActionController::API` subclass, or from something that isn't a Rails controller at all, include the pieces yourself:

```ruby
class ApplicationController < ActionController::API
  include ActionCable::SubscriptionAdapter::EnhancedPostgresql::ReliableBroadcasting::Controller
  helper ActionCable::SubscriptionAdapter::EnhancedPostgresql::ReliableBroadcasting::Helper
end
```

The controller concern adds `action_cable_since` (a UTC `Time`, captured before the action) and the helper adds `action_cable_enhanced_since_param` (that time as an encrypted token) and `action_cable_enhanced_since_meta_tag`.

### Security

The `enhanced-since` value sent to the browser is never the plain timestamp - it's an [`ActiveSupport::MessageEncryptor`](https://api.rubyonrails.org/classes/ActiveSupport/MessageEncryptor.html) token, encrypted and signed with the adapter's own `payload_encryptor` (see [Configuration](#configuration)) under the purpose `"enhanced-since"`. A client can read the token back on later requests but can't decrypt it, tamper with it, or forge one of their own to fish for messages from an arbitrary point in time. On the way back in, the channel decrypts and verifies it the same way; anything that fails to decrypt or verify - a missing/invalid signature, a mismatched purpose, or simply garbage - is treated exactly like a missing param (no replay, just the subscription confirmation) and logged as a warning, never raised.

### `messages_since`

The adapter exposes `messages_since(channel, since)`, returning an array of `Message` structs (`id`, `channel`, `payload`, `created_at`), ordered oldest first. It's what the `Channel` concern calls internally, but it's also available directly if you need to inspect or replay history yourself:

```ruby
adapter = ActionCable.server.pubsub
adapter.messages_since("chat_1", 5.minutes.ago).each do |message|
  puts "#{message.created_at}: #{message.payload}"
end
```

It returns `[]` if nothing has been stored for that channel, or if `reliable_broadcasting` has never been enabled and the table doesn't exist yet.

### Caveats

- **Delivery is at-least-once, not exactly-once.** A message broadcast in the small window between the subscription's `LISTEN` becoming active and the replay query running can be delivered twice: once live, once replayed. A reconnect also replays the entire retained window again using the original `since` value, which is exactly what makes reconnects reliable, but it means handlers need to be idempotent. Turbo Streams' `replace` and `update` actions are idempotent by nature, and so are `append` / `prepend` of elements that carry an `id`, since Turbo will not insert a duplicate.
- **Replay is bounded by `message_retention`.** Only messages younger than `message_retention` seconds (120 by default) are guaranteed to still be in the table. A `since` timestamp older than that will only get back whatever hasn't been cleaned up yet.
- **Clocks must be in sync.** The `since` timestamp is captured on the Rails server, but compared against `created_at` timestamps generated by the database clock. Keep both clocks synchronized (NTP) or the comparison can miss messages or replay too much.
- **There's a performance cost.** With `reliable_broadcasting` enabled, every broadcast does one extra INSERT into `action_cable_enhanced_broadcasts` in addition to the NOTIFY. See [Performance](#performance).

## Presence

Presence answers a different question than reliable broadcasting: not "what did I miss?" but "who's here right now?". Include `Presence::Channel` in a channel (alongside `ReliableBroadcasting::Channel` if you like, or on its own) and, for as long as a subscription is open, the adapter keeps a row recording that a given value - a user's name, id, or anything else you want to identify them by - is present on every stream that subscription is streaming from. Ask the adapter for the current list at any time with `presences`.

### How it works

1. The client passes an encrypted `enhanced-presence` value when it subscribes (a `turbo_stream_from ..., data: { enhanced_presence: ... }` attribute, or a plain channel param).
2. As soon as the subscription is confirmed, the channel touches a row for every stream it's on: `(channel, presence, subscription_key)`, where `subscription_key` is a random identifier generated per channel instance, so two connections presenting the same value (two tabs, two devices) are tracked - and expire - independently, while still only being listed once.
3. A repeating heartbeat (every `presence_heartbeat_interval` seconds, default 30) touches that row again, refreshing its timestamp.
4. `ActionCable.server.pubsub.presences("room-1")` returns the sorted, de-duplicated list of presence values whose most recent touch was within `presence_ttl` seconds (default 90) - i.e. everyone who's still actively subscribed, or who dropped off no more than `presence_ttl` seconds ago.
5. Unsubscribing (a clean disconnect, `stop_stream_from`, or `stop_all_streams`) removes the row immediately. A connection that vanishes without unsubscribing cleanly (a crashed tab, a lost network) just stops heartbeating - its presence naturally falls out of the list once `presence_ttl` elapses without a fresh touch.

### Setup

Include `Presence::Channel` the same way you'd include `ReliableBroadcasting::Channel` - see [Integrating into a Rails app](#integrating-into-a-rails-app) above for the two walkthroughs, Option A (a plain channel) or Option B (Turbo Streams). Both concerns can be included together:

```ruby
# app/channels/application_cable/channel.rb
module ApplicationCable
  class Channel < ActionCable::Channel::Base
    include ActionCable::SubscriptionAdapter::EnhancedPostgresql::ReliableBroadcasting::Channel
    include ActionCable::SubscriptionAdapter::EnhancedPostgresql::Presence::Channel
  end
end
```

```ruby
# config/initializers/reliable_turbo_streams.rb
Rails.application.config.to_prepare do
  Turbo::StreamsChannel.include(ActionCable::SubscriptionAdapter::EnhancedPostgresql::ReliableBroadcasting::Channel)
  Turbo::StreamsChannel.include(ActionCable::SubscriptionAdapter::EnhancedPostgresql::Presence::Channel)
end
```

Pass the value with the `Helper`'s `action_cable_enhanced_presence_param`, which encrypts it exactly like `action_cable_enhanced_since_param` does for the replay timestamp:

```erb
<%# app/views/rooms/show.html.erb %>
<%= turbo_stream_from @room, data: {
  enhanced_since: action_cable_enhanced_since_param,
  enhanced_presence: (action_cable_enhanced_presence_param(current_user.name) if current_user)
}.compact %>
```

Or, for a plain Action Cable channel, pass it as a channel param from JavaScript:

```js
consumer.subscriptions.create(
  { channel: "RoomChannel", room_id: 1, "enhanced-presence": "<%= action_cable_enhanced_presence_param(current_user.name) %>" },
  { /* ... */ }
)
```

`enhanced_presence` (the underscored spelling turbo-rails produces from a `data-enhanced-presence` attribute) is accepted too, exactly like `enhanced_since`/`enhanced-since`.

Then ask for the list wherever you need it - a controller, a background job, another channel:

```ruby
ActionCable.server.pubsub.presences("room-1") # => ["alice", "bob"]
```

If you're using Turbo's `turbo_stream_from @room`, the broadcasting name is `@room.to_gid_param` for a model or the string itself otherwise - the same name passed to `broadcasts_to`/`stream_for`.

### Caveats

- **Staleness is bounded by `presence_heartbeat_interval`, not instant.** A connection that drops stays listed for up to `presence_ttl` seconds (default 90) after its last heartbeat, not the moment it actually disappears - `presences` reflects "seen within the last `presence_ttl` seconds", not "connected right now". Keep `presence_heartbeat_interval` comfortably smaller than `presence_ttl` (the default 30s/90s gives three heartbeats of slack) so a single missed beat doesn't drop someone early.
- **A clean unsubscribe is still faster.** Closing a tab, navigating away, or calling `stop_stream_from`/`stop_all_streams` removes the row immediately rather than waiting for the TTL - the TTL only matters for connections that vanish without unsubscribing.

### Security

Like `enhanced-since`, the `enhanced-presence` value is never sent or accepted as plain text - it's encrypted and signed with the adapter's `payload_encryptor` under the purpose `"enhanced-presence"` (a different purpose than `"enhanced-since"`, so one kind of token can never be replayed as the other). A client can't read what a presence token decrypts to, tamper with it, or forge an arbitrary presence value to impersonate someone else; a token that fails to decrypt or verify is treated as no presence param at all (nothing is stored, no timer starts) and logged as a warning.

## Development

- Clone repo
- `bundle install` to install dependencies
- The test suite needs a real, reachable PostgreSQL server - it does not skip when one isn't available, it fails. `bin/ensure-postgres` starts a local server if one isn't already running (and creates a superuser role for the current OS user if needed); every test file calls it automatically before connecting, so in the common case you don't need to run it yourself. If it can't find a way to start Postgres on your system, run it directly to see why, or start Postgres yourself.
- `bundle exec ruby test/postgresql_test.rb` to run the adapter tests
- `bundle exec ruby test/reliable_broadcasting_test.rb` to run the reliable broadcasting integration tests
- `bundle exec ruby test/system/hotwire_reliable_broadcasting_test.rb` to run the Hotwire Turbo Streams system test - it boots a minimal Rails app under `test/dummy` and drives it with a real, headless Chromium via Capybara/Cuprite; set `BROWSER_PATH` if Chromium isn't auto-detected
