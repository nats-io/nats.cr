# NATS - Crystal Client

Simple NATS client for the [Crystal](https://crystal-lang.org) programming language.

[![License Apache 2](https://img.shields.io/badge/License-Apache2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Build Status](https://travis-ci.org/nats-io/nats.cr.svg?branch=master)](http://travis-ci.org/nats-io/nats.cr)

## Installation

1. Add the dependency to your `shard.yml`:

```yaml
   dependencies:
     nats:
       github: nats-io/nats.cr
```

2. Run `shards install`

## Usage

```crystal
require "nats"

nc = NATS::Connection.new("demo.nats.io")
nc.subscribe("foo") { |msg| puts "Received '#{msg}'"}
nc.publish("foo", "Hello!")

sub = nc.subscribe("req") do |msg|
  msg.respond("ANSWER is 42")
end

answer = nc.request("req", "Help!")
puts "Received a response '#{answer}'!"

sub.close
nc.close
```

## License

Unless otherwise noted, the NATS source files are distributed under
the Apache Version 2.0 license found in the LICENSE file.
