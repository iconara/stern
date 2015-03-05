# encoding: ascii-8bit

require 'spec_helper'

module Stern
  module Protocol
    describe OffsetRequest do
      describe '#to_b' do
        it 'encodes a request with no topics' do
          bytes = described_class.new(nil, 0x11223344, {}).to_b
          expect(bytes).to eq(
            "\x11\x22\x33\x44" +
            "\x00\x00\x00\x00"
          )
        end

        it 'encodes a request with a single topic with no partitions' do
          bytes = described_class.new(nil, 0x11223344, {'topotopic' => []}).to_b
          expect(bytes).to eq(
            "\x11\x22\x33\x44" +
            "\x00\x00\x00\x01" +
            "\x00\x09topotopic" +
            "\x00\x00\x00\x00"
          )
        end

        it 'encodes a request with a single topic with a single partition for a millisecond timestamp' do
          bytes = described_class.new(nil, 0x11223344, {'topotopic' => [[0x00000007, 0x14be41f2d1e, 1]]}).to_b
          expect(bytes).to eq(
            "\x11\x22\x33\x44" +
            "\x00\x00\x00\x01" +
            "\x00\x09topotopic" +
            "\x00\x00\x00\x01" +
            "\x00\x00\x00\x07" +
            "\x00\x00\x01\x4b\xe4\x1f\x2d\x1e" +
            "\x00\x00\x00\x01"
          )
        end

        it 'encodes a request with a single topic with a single partition for the latest offset' do
          bytes = described_class.new(nil, 0x11223344, {'topotopic' => [[0x00000007, -1, 1]]}).to_b
          expect(bytes).to eq(
            "\x11\x22\x33\x44" +
            "\x00\x00\x00\x01" +
            "\x00\x09topotopic" +
            "\x00\x00\x00\x01" +
            "\x00\x00\x00\x07" +
            "\xff\xff\xff\xff\xff\xff\xff\xff" +
            "\x00\x00\x00\x01"
          )
        end

        it 'encodes a request with a single topic with a single partition for the earliest offset' do
          bytes = described_class.new(nil, 0x11223344, {'topotopic' => [[0x00000007, -2, 1]]}).to_b
          expect(bytes).to eq(
            "\x11\x22\x33\x44" +
            "\x00\x00\x00\x01" +
            "\x00\x09topotopic" +
            "\x00\x00\x00\x01" +
            "\x00\x00\x00\x07" +
            "\xff\xff\xff\xff\xff\xff\xff\xfe" +
            "\x00\x00\x00\x01"
          )
        end

        it 'encodes a request with multiple topics and partitions' do
          bytes = described_class.new(nil, 0x11223344, {
            'topotopic' => [[0x00000007, -1, 1], [0x00000006, -2, 10]],
            'topicoco' => [[0x00000001, 0x14be41f2d1e, 1], [0x00000002, 0x14be41f2d1e, 1], [0x00000003, 0x14be41f2d1e, 1]],
          }).to_b
          expect(bytes).to eq(
            "\x11\x22\x33\x44" +
            "\x00\x00\x00\x02" +
            "\x00\x09topotopic" +
            "\x00\x00\x00\x02" +
            "\x00\x00\x00\x07" +
            "\xff\xff\xff\xff\xff\xff\xff\xff" +
            "\x00\x00\x00\x01" +
            "\x00\x00\x00\x06" +
            "\xff\xff\xff\xff\xff\xff\xff\xfe" +
            "\x00\x00\x00\x0a" +
            "\x00\x08topicoco" +
            "\x00\x00\x00\x03" +
            "\x00\x00\x00\x01" +
            "\x00\x00\x01\x4b\xe4\x1f\x2d\x1e" +
            "\x00\x00\x00\x01" +
            "\x00\x00\x00\x02" +
            "\x00\x00\x01\x4b\xe4\x1f\x2d\x1e" +
            "\x00\x00\x00\x01" +
            "\x00\x00\x00\x03" +
            "\x00\x00\x01\x4b\xe4\x1f\x2d\x1e" +
            "\x00\x00\x00\x01"
          )
        end
      end
    end
  end
end
