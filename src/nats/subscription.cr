# Copyright 2019-2020 The NATS Authors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

module NATS
  # A subscription to a given subject and possible queue group. Used to unsubscribe/close.
  #
  # ```
  # nc = NATS::Connection.new("demo.nats.io")
  # sub = nc.subscribe("foo") { |msg| }
  # sub.close
  # sub.unsubscribe
  # ```
  class Subscription
    protected def initialize(@sid : Int32, @conn : Connection, @cb : Msg ->)
      @msgs = Channel(Msg?).new(128)
      spawn process_msgs
    end

    # Will unsubscribe from a subscription.
    def unsubscribe
      unless closed?
        @conn.unsubscribe(@sid)
        @msgs.close
      end
    end

    # ditto
    def close
      unsubscribe
    end

    # Determines if the subscription is already closed.
    def closed?
      return @msgs.closed?
    end

    protected def send(msg : Msg)
      @msgs.send(msg) unless @msgs.closed?
    end

    private def process_msgs
      until @conn.closed?
        msg = @msgs.receive
        break if msg.nil? # FIXME(dlc) - I think nil ok.
        @cb.call(msg)
      end
    rescue # for when msgs channel gets closed.
    end
  end

  private class InternalSubscription
    def initialize(@sid : Int32, @conn : Connection)
    end

    def unsubscribe
      @conn.unsubscribe(@sid)
    end

    def send(msg : Msg)
      @conn.handle_resp(msg)
    end
  end
end
