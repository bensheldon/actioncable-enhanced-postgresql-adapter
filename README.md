# actioncable-enhanced-postgresql-adapter

This gem provides an enhanced PostgreSQL adapter for ActionCable. It is based on the original PostgreSQL adapter, but includes the following enhancements:
- Ability to broadcast payloads larger than 8000 bytes
- Optional reliable broadcasting, so a client never misses a message broadcast between page render and subscription
- Not dependent on ActiveRecord (but can still integrate with it if available)

### Approach

To overcome the 8000 bytes limit, we temporarily store large payloads in an [unlogged](https://www.crunchydata.com/blog/postgresl-unlogged-tables) database table named `action_cable_messages`. The table is lazily created on first broadcast.

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

## Performance

For payloads smaller than 8000 bytes, which should cover the majority of cases, performance is identical to the original adapter, unless `reliable_broadcasting` is enabled.

When broadcasting large payloads, or with `reliable_broadcasting` enabled, one has to consider the overhead of storing the payload in the database (and, for large payloads, fetching it back out). For low frequency broadcasting, this overhead is likely negligible. But take care if you're doing very high frequency broadcasting: with `reliable_broadcasting` enabled every broadcast costs one extra INSERT, in addition to the NOTIFY it already sends.

Note that whichever ActionCable adapter you're using, sending large payloads with high frequency is an anti-pattern. Even Redis pub/sub has [limitations](https://redis.io/docs/reference/clients/#output-buffer-limits) to be aware of.

### Cleanup of stored messages

Deletion of stale messages (`message_retention` seconds old or older, 120 by default) is triggered every 100 inserts into `action_cable_messages`. We do this by looking at the incremental ID generated on insert and checking if it is evenly divisible by 100. This approach avoids having to manually schedule cleanup jobs while striking a balance between performance and cleanup frequency.

## Reliable broadcasting

Normally, a broadcast is only delivered to clients that are already subscribed at the moment it happens. That leaves a gap: a page is rendered by a controller, sent to the browser, and only then does the client's WebSocket connect and subscribe to a channel. Anything broadcast while the page is rendering, or in the time between the response arriving and the subscription being confirmed, is lost. The same gap reopens on every reconnect.

Setting `reliable_broadcasting: true` in `cable.yml` closes that gap. When it's enabled, every broadcast payload is stored in the `action_cable_messages` table (not only ones over 8000 bytes), and the adapter exposes a `messages_since` API that a channel can use to replay anything it missed, right after its subscription is confirmed.

The flow looks like this:

1. A controller records the current time (`Time.now.utc`) before the action runs.
2. A view helper embeds that timestamp in the rendered HTML, either as a meta tag or a data attribute.
3. The client passes it back as the `since` channel parameter when it subscribes.
4. Once the channel's subscription to Postgres (`LISTEN`) is confirmed, the server looks up every message stored for that stream with a `created_at` at or after `since`, and delivers them through the normal stream handler, oldest first. Live messages that arrive while the replay runs are delivered as usual.

### Integrating into a Rails app

There are three moving parts: the adapter has to store messages (`cable.yml`), your channel has to replay them (the `Channel` concern), and the page has to hand the render timestamp back to the channel (a helper in the view plus the `since` channel parameter). Controllers and views need no manual wiring in a Rails app: the gem's Railtie includes the `Controller` concern into `ActionController::Base` and registers the `Helper` module for all views.

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

Restart the server. The `action_cable_messages` table is created on the first broadcast, no migration needed.

#### Option A: a plain Action Cable channel

Include the concern once in your base channel so every channel replays missed messages when a `since` parameter is present:

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
  <%= action_cable_since_meta_tag %>
  <%= javascript_importmap_tags %>
</head>
```

This renders `<meta name="action-cable-since" content="2026-09-02T18:55:12.123456Z">`. The value is captured by a `prepend_before_action` at the very start of the request, so any broadcast that happens while your action queries the database or renders its templates is covered.

Pass it along when the JavaScript subscribes. `ActionCable.getConfig("since")` reads the meta tag for you:

```js
// app/javascript/channels/chat_channel.js
import * as ActionCable from "@rails/actioncable"

const consumer = ActionCable.createConsumer()

consumer.subscriptions.create(
  { channel: "ChatChannel", room_id: 1, since: ActionCable.getConfig("since") },
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

Then pass the timestamp wherever you call `turbo_stream_from`. turbo-rails forwards the `data-*` attributes of the rendered `<turbo-cable-stream-source>` element as channel parameters, so a `data-since` attribute arrives at the channel as `params[:since]`, the same as it would for a hand-written subscription:

```erb
<%# app/views/rooms/show.html.erb %>
<%= turbo_stream_from @room, data: { since: action_cable_since_param } %>

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

If you render several `turbo_stream_from` tags on one page, pass `data: { since: action_cable_since_param }` to each of them. Every one is its own subscription and each is replayed independently.

#### Checking that it works

1. Open a page that subscribes to a channel and confirm the rendered HTML contains the timestamp (`action-cable-since` meta tag or a `data-since` attribute).
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

The controller concern adds `action_cable_since` (a UTC `Time`, captured before the action) and the helper adds `action_cable_since_param` (that time as an ISO 8601 string) and `action_cable_since_meta_tag`.

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
- **There's a performance cost.** With `reliable_broadcasting` enabled, every broadcast does one extra INSERT into `action_cable_messages` in addition to the NOTIFY. See [Performance](#performance).

## Development

- Clone repo
- `bundle install` to install dependencies
- `bundle exec ruby test/postgresql_test.rb` to run the adapter tests
- `bundle exec ruby test/reliable_broadcasting_test.rb` to run the reliable broadcasting integration tests
- `bundle exec ruby test/system/hotwire_reliable_broadcasting_test.rb` to run the Hotwire Turbo Streams system test - it boots a minimal Rails app under `test/dummy` and drives it with a real, headless Chromium via Capybara/Cuprite; set `BROWSER_PATH` if Chromium isn't auto-detected
