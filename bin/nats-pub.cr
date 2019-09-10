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

USAGE = "Usage: nats-pub [-s server] <subject> <msg>"

def usage
  STDERR.puts USAGE; exit(1)
end

server = NATS::Connection::DEFAULT_URI
args = ARGV.dup

OptionParser.parse(args) do |parser|
  parser.banner = USAGE
  parser.on("-s SERVER", "--server=SERVER", "NATS Server to connect") { |url| server = url }
  parser.on("-h", "--help", "Show this help") { usage }
  parser.invalid_option do |flag|
    STDERR.puts "ERROR: #{flag} is not a valid option."
    usage
  end
end

usage unless args.size == 2
subject, msg = args

nc = NATS::Connection.new(server)
at_exit { nc.close }

nc.publish(subject, msg)
puts "Published [#{subject}] : '#{msg}'"
