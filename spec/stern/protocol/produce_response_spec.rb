# encoding: ascii-8bit

require 'spec_helper'

module Stern
  module Protocol
    describe ProduceResponse do
      describe '.decode' do
        it 'decodes an empty frame' do
          frame = "\x00\x00\x00\x00"
          response = described_class.decode(frame)
          expect(response.topic_offsets).to be_empty
        end

        it 'decodes a frame with a single topic with no partitions' do
          frame =  "\x00\x00\x00\x01"
          frame << "\x00\x09topotopic"
          frame << "\x00\x00\x00\x00"
          response = described_class.decode(frame)
          expect(response.topic_offsets.size).to eq(1)
          expect(response.topic_offsets['topotopic'].partition_offsets.size).to eq(0)
        end

        it 'decodes a frame with a single topic with and a single partition' do
          frame =  "\x00\x00\x00\x01"
          frame << "\x00\x09topotopic"
          frame << "\x00\x00\x00\x01"
          frame << "\x00\x00\x00\x09"
          frame << "\x00\x00"
          frame << "\x01\x02\x03\x04\x05\x06\x07\x08"
          response = described_class.decode(frame)
          partition_offset = response.topic_offsets['topotopic'].partition_offsets[9]
          expect(partition_offset.offset).to eq(0x0102030405060708)
        end

        it 'decodes a frame with multiple topics and offsets' do
          frame =  "\x00\x00\x00\x03"
          frame << "\x00\x09topotopic"
          frame << "\x00\x00\x00\x03"
          frame << "\x00\x00\x00\x09"
          frame << "\x00\x00"
          frame << "\x01\x02\x03\x04\x05\x06\x07\x08"
          frame << "\x00\x00\x00\x08"
          frame << "\x00\x00"
          frame << "\x02\x03\x04\x05\x06\x07\x08\x09"
          frame << "\x00\x00\x00\x07"
          frame << "\x00\x00"
          frame << "\x03\x04\x05\x06\x07\x08\x09\x0a"
          frame << "\x00\x08topicoco"
          frame << "\x00\x00\x00\x02"
          frame << "\x00\x00\x00\x02"
          frame << "\x00\x00"
          frame << "\x04\x05\x06\x07\x08\x09\x0a\x0b"
          frame << "\x00\x00\x00\x03"
          frame << "\x00\x00"
          frame << "\x05\x06\x07\x08\x09\x0a\x0b\x0c"
          frame << "\x00\x08topopoco"
          frame << "\x00\x00\x00\x01"
          frame << "\x00\x00\x00\x01"
          frame << "\x00\x00"
          frame << "\x06\x07\x08\x09\x0a\x0b\x0c\x0d"
          response = described_class.decode(frame)
          expect(response.topic_offsets.keys).to eq(%w[topotopic topicoco topopoco])
          expect(response.topic_offsets['topotopic'].partition_offsets.keys).to eq([9, 8, 7])
          expect(response.topic_offsets['topicoco'].partition_offsets.keys).to eq([2, 3])
          expect(response.topic_offsets['topopoco'].partition_offsets.keys).to eq([1])
          expect(response.topic_offsets['topotopic'].partition_offsets[7].offset).to eq(0x030405060708090a)
          expect(response.topic_offsets['topicoco'].partition_offsets[3].offset).to eq(0x05060708090a0b0c)
          expect(response.topic_offsets['topopoco'].partition_offsets[1].offset).to eq(0x060708090a0b0c0d)
        end

        it 'decodes a frame with a partition error' do
          frame =  "\x00\x00\x00\x01"
          frame << "\x00\x09topotopic"
          frame << "\x00\x00\x00\x02"
          frame << "\x00\x00\x00\x09"
          frame << "\x00\x03"
          frame << "\x01\x02\x03\x04\x05\x06\x07\x08"
          frame << "\x00\x00\x00\x08"
          frame << "\x00\x00"
          frame << "\x02\x03\x04\x05\x06\x07\x08\x09"
          response = described_class.decode(frame)
          expect(response.topic_offsets['topotopic'].partition_offsets[9]).to be_error
        end
      end

      describe '.merge' do
        let :response1 do
          described_class.new({
            'topotopic' => TopicOffsets.new('topotopic', {
              8 => PartitionOffset.new('topotopic', 8, 88),
              9 => PartitionOffset.new('topotopic', 9, 99),
            }),
            'topicoco' => TopicOffsets.new('topicoco', {
              3 => PartitionOffset.new('topicoco', 3, 33),
              4 => PartitionOffset.new('topicoco', 4, 44),
              5 => PartitionOffset.new('topicoco', 5, 55),
              6 => PartitionOffset.new('topicoco', 6, 66),
            }),
          })
        end

        let :response2 do
          described_class.new({
            'topotopic' => TopicOffsets.new('topotopic', {
              7 => PartitionOffset.new('topotopic', 7, 77),
            }),
            'topicoco' => TopicOffsets.new('topicoco', {
              2 => PartitionOffset.new('topicoco', 2, 22),
            }),
            'topopoco' => TopicOffsets.new('topopoco', {
              1 => PartitionOffset.new('topopoco', 1, 11),
            })
          })
        end

        let :response3 do
          described_class.new({
            'topicoco' => TopicOffsets.new('topicoco', {
              7 => PartitionOffset.new('topicoco', 7, 77),
            }),
            'topopoco' => TopicOffsets.new('topopoco', {
              2 => PartitionOffset.new('topopoco', 2, 22),
            })
          })
        end

        it 'returns an offset response with the offsets from all the given offset responses' do
          merged_response = described_class.merge(response1, response2, response3)
          expect(merged_response.topic_offsets.keys).to match_array(%w[topotopic topopoco topicoco])
          expect(merged_response.topic_offsets['topotopic'].partition_offsets.keys).to match_array([7, 8, 9])
          expect(merged_response.topic_offsets['topicoco'].partition_offsets.keys).to match_array([2, 3, 4, 5, 6, 7])
          expect(merged_response.topic_offsets['topopoco'].partition_offsets.keys).to match_array([1, 2])
        end

        context 'when the same topic/partition combination exists in more than one response' do
          it 'picks the last' do
            responseX = described_class.new('topotopic' => TopicOffsets.new('topotopic', {8 => PartitionOffset.new('topotopic', 8, 8888)}))
            merged_response = described_class.merge(response1, responseX)
            expect(merged_response.topic_offsets['topotopic'].partition_offsets[8].offset).to eq(8888)
          end
        end
      end
    end
  end
end
