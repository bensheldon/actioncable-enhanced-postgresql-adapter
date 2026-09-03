# frozen_string_literal: true

class RoomsController < ApplicationController
  def show
    @room = "room-#{params[:id]}"
    @reliable = params[:reliable] != "false"
    # Simulates a channel computing presence server-side (from something it already knows) rather
    # than trusting the frontend param - see the `server_presence` override in
    # config/initializers/reliable_turbo_streams.rb. A real app would read `current_user` instead
    # of a query param; there's no authentication in this dummy app to demonstrate that with.
    @server_presence = params[:server_presence]

    if params[:broadcast_during_render] == "true"
      # This runs *after* the Railtie's prepend_before_action has already captured
      # action_cable_since, and well before the response reaches the browser - exactly the
      # race that reliable broadcasting closes. Without it, this message would only ever be
      # delivered to a client that happened to already be subscribed.
      Turbo::StreamsChannel.broadcast_append_to(
        @room, target: "messages", html: %(<div id="message_during_render">during render</div>)
      )
    end
  end
end
