require "../spec_helper"

describe "NATS::Connection#auth" do
  user = "user"
  pass = "s3cr3t"

  auth_uri = URI.parse("nats://#{user}:#{pass}@127.0.0.1:4333")
  noauth_uri = URI.parse("nats://127.0.0.1:4333")

  auth_server : NATSServer | Nil = nil

  Spec.after_each do
    auth_server.try(&.shutdown)
    auth_server = nil
  end

  describe "#new" do
    it "should connect with correct user/pass" do
      auth_server = NATSServer.start(auth_uri)
      nc = NATS::Connection.new(auth_uri)
      nc.should_not be_nil
      nc.close
    end

    it "should fail with no user/pass" do
      auth_server = NATSServer.start(auth_uri)
      expect_raises(Exception) do
        NATS::Connection.new(noauth_uri)
      end
    end
  end
end
