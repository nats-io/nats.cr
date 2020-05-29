require "spec"
require "socket"
require "../src/nats"
require "../src/nats/connection"

class NATSServer
  getter? was_running

  def self.start(uri = NATS::CONNECTION::DEFAULT_URI)
    server = NATSServer.new(uri)
    server.start(true)
    return server
  end

  getter uri

  def initialize(uri = NATS::CONNECTION::DEFAULT_URI, flags : String? = nil, config_file : String? = nil)
    @uri = uri.is_a?(URI) ? uri : URI.parse(uri)
    @flags = flags
    @config_file = config_file
  end

  def start(wait_for_server = true)
    args = ["-p", "#{@uri.port}", "-a", "#{@uri.host}"]

    if @uri.user && !@uri.password
      args += " --auth #{@uri.user}".split
    else
      args += " --user #{@uri.user}".split if @uri.user
      args += " --pass #{@uri.password}".split if @uri.password
    end
    args += " #{@flags}".split if @flags

    args << "-DV" if ENV["DEBUG_NATS_TEST"]? == "true"

    @p = Process.new("nats-server", args)
    raise "Server could not start, already running?" if @p.nil? || @p.try(&.terminated?)
    wait_for_server(@uri, 10) if wait_for_server
  end

  def shutdown
    @p.try(&.kill)
    @p = nil
  rescue
  end

  def wait_for_server(uri, max_wait = 5)
    start = Time.monotonic
    wait = Time::Span.new(seconds: max_wait)
    while (Time.monotonic - start < wait) # Wait max_wait seconds max
      return if server_running?(uri)
      sleep(20.milliseconds)
    end
    raise "Server not started, can not connect"
  end

  def server_running?(uri)
    s = TCPSocket.new(uri.host.to_s, uri.port)
    s.close
    return true
  rescue
    return false
  end
end

def wait(ch : Channel, timeout = 2.second)
  spawn { sleep timeout; ch.close }
  return ch.receive
rescue
  fail("Operation timed-out")
end
