# encoding: ascii-8bit

require 'spec_helper'

module Stern
  module Protocol
    describe ProduceRequest do
      describe '#to_b' do
        it 'encodes a request with no messages' do
          bytes = described_class.new(nil, 3, 1000, {}).to_b
          expect(bytes).to eql(
            "\x00\x03" +
            "\x00\x00\x03\xe8" +
            "\x00\x00\x00\x00"
          )
        end

        it 'encodes a request with a single message' do
          message_set = MessageSet.new([Message.new('foo', 'bar', -1)])
          bytes = described_class.new(nil, 3, 1000, {'topotopic' => {9 => message_set}}).to_b
          expect(bytes).to eql(
            "\x00\x03" +
            "\x00\x00\x03\xe8" +
            "\x00\x00\x00\x01" +
            "\x00\x09topotopic" +
            "\x00\x00\x00\x01" +
            "\x00\x00\x00\x09" +
            [message_set.to_b.bytesize].pack('N') +
            message_set.to_b
          )
        end
      end
    end
  end
end
