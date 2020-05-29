require "../spec_helper"

describe NATS::Connection do
  uri = URI.parse("nats://127.0.0.1:4322")
  server : NATSServer | Nil = nil

  Spec.before_each do
    server = NATSServer.start(uri)
  end

  Spec.after_each do
    server.try(&.shutdown)
    sleep(20.millisecond)
    server = nil
  end

  describe "#new" do
    it "Should raise when failing to connect" do
      expect_raises(Exception) do
        NATS::Connection.new("nats://127.0.0.1:4333")
      end
    end

    it "Should do basic connect with proper uri" do
      nc = NATS::Connection.new(uri)
      nc.should_not be_nil
      nc.close
    end
  end

  describe "#close" do
    it "Should report closed when closed" do
      nc = NATS::Connection.new(uri)
      nc.closed?.should be_false
      nc.close
      nc.closed?.should be_true
    end
  end

  describe "#on_close" do
    it "Should call on_close callback when closing" do
      nc = NATS::Connection.new(uri)
      ch = Channel(Bool).new
      nc.on_close { ch.send(true) }
      nc.close
      wait(ch)
    end

    it "Should call on_close when connection closed underneath" do
      nc = NATS::Connection.new(uri)
      ch = Channel(Bool).new
      nc.on_close { ch.send(true) }
      server.try(&.shutdown)
      wait(ch)
    end
  end

  describe "#new_inbox" do
    it "Should properly create _INBOX subjects" do
      nc = NATS::Connection.new(uri)
      inbox = nc.new_inbox
      inbox.should start_with("_INBOX")
      inbox.split(".").size.should eq(3)
    end
  end

  describe "#publish" do
    it "Should not raise when doing a publish" do
      nc = NATS::Connection.new(uri)
      nc.publish("foo", "Hello!")
      nc.close
    end

    it "Should raise on empty subject" do
      nc = NATS::Connection.new(uri)
      expect_raises(Exception) { nc.publish("", "Hello!") }
    end

    it "Should raise on closed connection" do
      nc = NATS::Connection.new(uri)
      nc.close
      expect_raises(Exception) { nc.publish("foo", "Hello!") }
    end

    it "Should raise on too big of a payload" do
      nc = NATS::Connection.new(uri)
      expect_raises(Exception) { nc.publish("foo", "abc"*10_000_000) }
    end

    it "Should deliver an error on bad subject" do
      nc = NATS::Connection.new(uri, pedantic: true)
      ch = Channel(Bool).new
      nc.on_error do |e|
        e.should contain("Invalid Publish Subject")
        ch.send(true)
      end
      nc.publish("foo..", "abc")
      wait(ch)
      nc.close
    end
  end

  describe "#publish_with_reply" do
    it "Should attach a reply subject" do
      nc = NATS::Connection.new(uri)
      ch = Channel(Bool).new
      nc.subscribe("foo") do |msg|
        msg.should_not be_nil
        msg.reply.should_not be_nil
        msg.reply.to_s.should eq("respond")
        ch.send(true)
      end
      nc.publish_with_reply("foo", "respond", "Hello!")
      wait(ch)
      nc.close
    end
  end

  describe "#subscribe" do
    it "Should receive messages it subscribed to" do
      nc = NATS::Connection.new(uri)
      ch = Channel(Bool).new
      nc.subscribe("foo") { ch.send(true) }
      nc.publish("foo", "Hello!")
      wait(ch)
      nc.close
    end

    it "Should receive messages it subscribed to as a group" do
      nc = NATS::Connection.new(uri)
      ch = Channel(Bool).new
      nc.subscribe("foo", "bar") { ch.send(true) }
      nc.subscribe("foo", "bar") { ch.send(true) }
      nc.publish("foo", "Hello!")
      nc.publish("foo", "Hello!")
      wait(ch)
      wait(ch)
    end

    it "Should not receive messages after unssubscribe" do
      nc = NATS::Connection.new(uri)
      ch = Channel(Bool).new
      sub = nc.subscribe("foo") { ch.send(true) }
      nc.publish("foo", "Hello!")
      wait(ch)
      sub.unsubscribe
      nc.publish("foo", "Hello!")
      spawn { sleep 20.millisecond; ch.close }
      expect_raises(Exception) { ch.receive }
      sub.closed?.should be_true
      # Unsubscribing/closing twice should be ok.
      sub.close
    end
  end

  describe "#request" do
    it "Should receive requests with reply subjects" do
      nc = NATS::Connection.new(uri)
      nc.subscribe("request") do |msg|
        msg.should_not be_nil
        msg.reply.should_not be_nil
        msg.reply.to_s.should start_with("_INBOX")
        nc.publish(msg.reply.to_s, "42")
      end
      answer = nc.request("request", "Help!")
      answer.to_s.should eq("42")
      nc.close
    end

    it "Should timeout correctly" do
      nc = NATS::Connection.new(uri)
      elapsed = Time.measure do
        answer = nc.request("request", "Help!", 10.millisecond)
      rescue
      end
      elapsed.should be_close(10.millisecond, 5.millisecond)
      nc.close
    end

    it "Should be able to respond to request message" do
      nc = NATS::Connection.new(uri)
      nc.subscribe("request") do |req|
        req.respond("42")
      end
      answer = nc.request("request", "Help!")
      answer.to_s.should eq("42")
      nc.close
    end
  end
end
