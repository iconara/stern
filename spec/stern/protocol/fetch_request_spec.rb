# encoding: ascii-8bit

require 'spec_helper'

module Stern
  module Protocol
    describe FetchRequest do
      describe '#to_b' do
        it 'encodes a request with no topic fetches' do
          bytes = described_class.new(nil, 0x11223344, 10_000, 128, []).to_b
          expect(bytes).to eq(
            "\x11\x22\x33\x44" +
            "\x00\x00\x27\x10" +
            "\x00\x00\x00\x80" +
            "\x00\x00\x00\x00"
          )
        end

        it 'encodes a request with a single topic and a single partition fetch' do
          fetches = [described_class::Fetch.new('topotopic', 9, 9999, 4096)]
          bytes = described_class.new(nil, 0x11223344, 10_000, 128, fetches).to_b
          expect(bytes).to eq(
            "\x11\x22\x33\x44" +
            "\x00\x00\x27\x10" +
            "\x00\x00\x00\x80" +
            "\x00\x00\x00\x01" +
            "\x00\x09topotopic" +
            "\x00\x00\x00\x01" +
            "\x00\x00\x00\x09" +
            "\x00\x00\x00\x00\x00\x00\x27\x0f" +
            "\x00\x00\x10\x00"
          )
        end

        it 'encodes a request with a single topic and multiple partition fetches' do
          fetches = [
            described_class::Fetch.new('topotopic', 9, 9999, 4096),
            described_class::Fetch.new('topotopic', 8, 8888, 8192),
          ]
          bytes = described_class.new(nil, 0x11223344, 10_000, 128, fetches).to_b
          expect(bytes).to eq(
            "\x11\x22\x33\x44" +
            "\x00\x00\x27\x10" +
            "\x00\x00\x00\x80" +
            "\x00\x00\x00\x01" +
            "\x00\x09topotopic" +
            "\x00\x00\x00\x02" +
            "\x00\x00\x00\x09" +
            "\x00\x00\x00\x00\x00\x00\x27\x0f" +
            "\x00\x00\x10\x00" +
            "\x00\x00\x00\x08" +
            "\x00\x00\x00\x00\x00\x00\x22\xb8" +
            "\x00\x00\x20\x00"
          )
        end

        it 'encodes a request with multiple topics and multiple partition fetches' do
          fetches = [
            described_class::Fetch.new('topotopic', 9, 9999, 4096),
            described_class::Fetch.new('topotopic', 8, 8888, 8192),
            described_class::Fetch.new('topicoco', 3, 3333, 4096),
          ]
          bytes = described_class.new(nil, 0x11223344, 10_000, 128, fetches).to_b
          expect(bytes).to eq(
            "\x11\x22\x33\x44" +
            "\x00\x00\x27\x10" +
            "\x00\x00\x00\x80" +
            "\x00\x00\x00\x02" +
            "\x00\x09topotopic" +
            "\x00\x00\x00\x02" +
            "\x00\x00\x00\x09" +
            "\x00\x00\x00\x00\x00\x00\x27\x0f" +
            "\x00\x00\x10\x00" +
            "\x00\x00\x00\x08" +
            "\x00\x00\x00\x00\x00\x00\x22\xb8" +
            "\x00\x00\x20\x00" +
            "\x00\x08topicoco" +
            "\x00\x00\x00\x01" +
            "\x00\x00\x00\x03" +
            "\x00\x00\x00\x00\x00\x00\x0d\x05" +
            "\x00\x00\x10\x00"
          )
        end
      end
    end
  end
end
