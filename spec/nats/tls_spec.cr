require "../spec_helper"

describe "NATS::Connection#TLS" do
  tls_uri = "tls://demo.nats.io"

  describe "#new" do
    it "should connect to a TLS based server" do
      nc = NATS::Connection.new(tls_uri)
      nc.should_not be_nil
      nc.close
    end
  end

  describe "#subscribe" do
    it "Should receive messages it subscribed to" do
      nc = NATS::Connection.new(tls_uri)
      inbox = nc.new_inbox
      ch = Channel(Bool).new
      ch = Channel(Bool).new
      nc.subscribe(inbox) { ch.send(true) }
      nc.publish(inbox, "Hello!")
      wait(ch)
      nc.close
    end
  end
end
