require "../spec_helper"

describe "NATS::Connection#TLS" do
  folders = __DIR__.split("/")
  cert_dir = "#{folders[0...-2].join("/")}/certs"

  tls_uri = "tls://demo.nats.io"
  tls_with_cert_uri = "tls://127.0.0.1:4322"
  tls_ca_cert = "#{cert_dir}/ca.pem"
  tls_cert = "#{cert_dir}/server.pem"
  tls_key = "#{cert_dir}/server-key.pem"

  describe "#new" do
    it "should connect to a TLS based server" do
      nc = NATS::Connection.new(tls_uri)
      nc.should_not be_nil
      nc.close
    end

    it "should connect to a TLS based server with tls config" do
      uri = URI.parse(tls_with_cert_uri)
      server = NATSServer.new(uri, "--tls --tlscert=#{tls_cert} --tlskey=#{tls_key}")
      server.start(false)
      server.wait_for_server(uri)

      nc = NATS::Connection.new(tls_with_cert_uri, tlskey: tls_key, tlscert: tls_cert, tlscacert: tls_ca_cert)
      nc.should_not be_nil
      nc.close

      server.try(&.shutdown)
      sleep(20.millisecond)
      server = nil
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
