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
  # A delivered message from a subscription.
  #
  # ```
  # nc = NATS::Connection.new("demo.nats.io")
  # nc.subscribe("foo") do |msg|
  #   puts "Received '#{msg}'"
  #   puts "Subject is #{msg.subject}"
  #   puts "Reply Subject is #{msg.reply}"
  #   puts "Raw Data is #{msg.data}"
  # end
  # ```
  struct Msg
    getter subject : String
    getter reply : String?
    getter data : Bytes

    @conn : Connection | Nil

    # :nodoc:
    def to_s(io : IO)
      io.write data
    end

    # :nodoc:
    def initialize(@subject, @data, @reply = nil)
    end

    protected def initialize(@subject, @data, @reply = nil, @conn = nil)
    end

    # Allows a response to a request message to be easily sent.
    #
    # ```
    # nc = NATS::Connection.new("demo.nats.io")
    # nc.subscribe("req") do |msg|
    #   msg.respond("ANSWER is 42")
    # end
    # ```
    def respond(msg)
      raise "No reply subject" if @reply.nil?
      raise "Not a received message" if @conn.nil?
      @conn.try(&.publish(@reply.to_s, msg))
    end
  end
end
