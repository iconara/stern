# encoding: ascii-8bit

require 'spec_helper'

module Stern
  module Protocol
    describe MetadataRequest do
      describe '#to_b' do
        it 'encodes a request with no topics' do
          bytes = described_class.new(nil, {}).to_b
          expect(bytes).to eq(
            "\x00\x00\x00\x00"
          )
        end

        it 'encodes a request with a single topic' do
          bytes = described_class.new(nil, ['topotopic']).to_b
          expect(bytes).to eq(
            "\x00\x00\x00\x01" +
            "\x00\x09topotopic"
          )
        end

        it 'encodes a request with multiple topics' do
          bytes = described_class.new(nil, ['topotopic', 'topicoco', 'topopoco']).to_b
          expect(bytes).to eq(
            "\x00\x00\x00\x03" +
            "\x00\x09topotopic" +
            "\x00\x08topicoco" +
            "\x00\x08topopoco"
          )
        end
      end
    end
  end
end
