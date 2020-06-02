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

require "random/secure"

# Copied and modified from nats-pure repo.
# https://github.com/nats-io/nats-pure.rb/blob/master/lib/nats/nuid.rb

module NATS
  class NUID
    DIGITS        = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
    BASE          = 62
    PREFIX_LENGTH = 12
    SEQ_LENGTH    = 10
    TOTAL_LENGTH  = PREFIX_LENGTH + SEQ_LENGTH
    MAX_SEQ       = 839299365868340224_i64
    MIN_INC       =                 33_i64
    MAX_INC       =                333_i64
    INC           = MAX_INC - MIN_INC

    def initialize
      @pre_bytes = Bytes.new(PREFIX_LENGTH)
      @seq = @inc = 0_i64
      reset!
    end

    def next
      @seq += @inc
      if @seq >= MAX_SEQ
        reset!
      end
      l = @seq

      # Do this inline 10 times to avoid even more extra allocs,
      # then use string interpolation of everything which works
      # faster for doing concat.
      s_10 = DIGITS[l % BASE]
      # Ugly, but parallel assignment is slightly faster here...
      s_09, s_08, s_07, s_06, s_05, s_04, s_03, s_02, s_01 = \
         (l //= BASE; DIGITS[l % BASE]), (l //= BASE; DIGITS[l % BASE]), (l //= BASE; DIGITS[l % BASE]), (l //= BASE; DIGITS[l % BASE]), (l //= BASE; DIGITS[l % BASE]), (l //= BASE; DIGITS[l % BASE]), (l //= BASE; DIGITS[l % BASE]), (l //= BASE; DIGITS[l % BASE]), (l //= BASE; DIGITS[l % BASE])
      "#{@prefix}#{s_01}#{s_02}#{s_03}#{s_04}#{s_05}#{s_06}#{s_07}#{s_08}#{s_09}#{s_10}"
    end

    private def reset!
      randomize_prefix!
      reset_sequential!
    end

    def randomize_prefix!
      @prefix = String::Builder.build(PREFIX_LENGTH) do |io|
        Random::Secure.random_bytes(@pre_bytes)
        @pre_bytes.each { |n| io << DIGITS[n % BASE] }
      end
    end

    private def reset_sequential!
      @seq = Random::Secure.rand(MAX_SEQ)
      @inc = MIN_INC + Random::Secure.rand(INC)
    end
  end
end
