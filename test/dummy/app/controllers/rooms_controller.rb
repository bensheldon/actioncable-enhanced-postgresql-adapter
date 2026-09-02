# frozen_string_literal: true

class RoomsController < ApplicationController
  def show
    @room = "room-#{params[:id]}"
    @reliable = params[:reliable] != "false"

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
