require "../src/nats/connection"

nc = NATS::Connection.new(NATS::Connection::DEFAULT_URI)

# nc = NATS::Connection.new(name: "Sample", user: "derek", pass: "foo")

# nc = NATS::Connection.new(4222)

# nc = NATS::Connection.new()

# nc = NATS::Connection.new("demo.nats.io")

# n5 = NATS::Connection.new("nats://demo.nats.io")

puts "Connected"

nc.publish("foo", "Hello!")
# nc.publish("foo")

nc.subscribe("foo") { |msg| puts "Received '#{msg}'" }
nc.subscribe("foo", "bar") do |msg|
  puts "Got a queue msg #{msg}"
end

nc.publish("foo", "Hello World!")

nc.subscribe("req") do |msg|
  msg.respond("ANSWER is 42")
end

puts "Sending a request!"

puts "INBOX is #{nc.new_inbox}"

answer = nc.request("req", "Help!")
puts "Received a response '#{answer}'!"

# puts "Send a request with no response"
# answer = nc.request("no_req", "Help!") rescue "Timeout"
# puts "Received this response #{answer}"

# answer = nc.request("no_req", "Help!", 10.millisecond) rescue "Quick Timeout"
# puts "Received this response #{answer}"

# nc.flush
# puts "Returned from flush"

puts "Simple bench"
data = "HELLO".to_slice

to_send = 1_000_000
elapsed = Time.measure do
  (0...to_send).each { nc.publish("a") }
  nc.flush
end
puts "elapsed for #{to_send} msgs is #{elapsed}"
puts "msgs/sec is #{(to_send/elapsed.to_f).ceil}"

received = 0
ch = Channel(Nil).new

nc2 = NATS::Connection.new(4222)

# to_send *= 5

nc2.subscribe("a") do
  received += 1
  ch.send(nil) if received >= to_send
end
nc2.flush

elapsed = Time.measure do
  (0...to_send).each { nc.publish("a") }
  ch.receive
end

puts "elapsed for pub/sub of #{to_send} msgs is #{elapsed}"
puts "msgs/sec is #{(to_send/elapsed.to_f).ceil}"

sleep 20
nc2.close
puts "Closed connection"

sleep

nc.close
