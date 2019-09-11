# Copyright 2019 The NATS Authors
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

require "option_parser"
require "../src/nats/connection"

USAGE = "Usage: nats-sub [-s server] [-r] [-t] <subject> [queue_group]"

def usage
  STDERR.puts USAGE; exit(1)
end

server = NATS::Connection::DEFAULT_URI
show_time, show_raw = false, false

args = ARGV.dup
OptionParser.parse(args) do |parser|
  parser.banner = USAGE
  parser.on("-s SERVER", "--server=SERVER", "NATS Server to connect") { |url| server = url }
  parser.on("-t", "--time") { show_time = true }
  parser.on("-r", "--raw") { show_raw = true }
  parser.on("-h", "--help", "Show this help") { usage }
  parser.invalid_option do |flag|
    STDERR.puts "ERROR: #{flag} is not a valid option."
    usage
  end
end

usage unless args.size >= 1
subject, queue_group = args[0], args[1]?
index = 0

nc = NATS::Connection.new(server)
nc.on_close { STDERR.puts "Connection closed, exiting."; exit(1) }

nc.subscribe(subject, queue_group) do |msg|
  if show_raw
    puts msg
  else
    time_prefix = "[#{Time.now}] " if show_time
    puts "#{time_prefix}[\##{index += 1}] Received on [#{msg.subject}] : '#{msg}'"
  end
end

puts "Listening on [#{subject}]" unless show_raw
sleep
