# encoding: ascii-8bit

require 'spec_helper'

module Stern
  module Protocol
    describe OffsetResponse do
      describe '.decode' do
        let :topic_name1 do
          "\x00\x09topotopic"
        end

        let :topic_name2 do
          "\x00\x08topicoco"
        end

        let :partition3 do
          "\x00\x00\x00\x03" +
          "\x00\x00" +
          "\x00\x00\x00\x01" +
          "\x07\x07\x07\x07\x07\x07\x07\x07"
        end

        let :partition7 do
          "\x00\x00\x00\x07" +
          "\x00\x00" +
          "\x00\x00\x00\x03" +
          "\x04\x04\x04\x04\x04\x04\x04\x04" +
          "\x05\x05\x05\x05\x05\x05\x05\x05" +
          "\x06\x06\x06\x06\x06\x06\x06\x06"
        end

        let :partition8 do
          "\x00\x00\x00\x08" +
          "\x00\x00" +
          "\x00\x00\x00\x02" +
          "\x02\x02\x02\x02\x02\x02\x02\x02" +
          "\x03\x03\x03\x03\x03\x03\x03\x03"
        end

        let :partition9 do
          "\x00\x00\x00\x09" +
          "\x00\x00" +
          "\x00\x00\x00\x01" +
          "\x01\x01\x01\x01\x01\x01\x01\x01"
        end

        let :error_partition do
          "\x00\x00\x00\xff" +
          "\x00\x06" +
          "\x00\x00\x00\x00"
        end

        it 'decodes an empty frame' do
          frame = "\x00\x00\x00\x00"
          response = described_class.decode(frame)
          expect(response.topic_offsets).to be_empty
        end

        it 'decodes a frame with a topic with no partitions' do
          frame =  "\x00\x00\x00\x01"
          frame << topic_name1
          frame << "\x00\x00\x00\x00"
          response = described_class.decode(frame)
          expect(response.topic_offsets['topotopic'].partition_offsets).to be_empty
        end

        it 'decodes a frame with a single topic with a single partition' do
          frame =  "\x00\x00\x00\x01"
          frame << topic_name1
          frame << "\x00\x00\x00\x01"
          frame << partition9
          response = described_class.decode(frame)
          expect(response.topic_offsets.size).to eq(1)
          expect(response.topic_offsets['topotopic'].partition_offsets.size).to eq(1)
          expect(response.topic_offsets['topotopic'].partition_offsets[9].offsets).to eq([0x0101010101010101])
          expect(response.topic_offsets['topotopic'].partition_offsets[9].topic_name).to eq('topotopic')
          expect(response.topic_offsets['topotopic'].partition_offsets[9].partition_id).to eq(9)
        end

        it 'decodes a frame with multiple topics, partitions and offsets' do
          frame =  "\x00\x00\x00\x02"
          frame << topic_name1
          frame << "\x00\x00\x00\x03"
          frame << partition9
          frame << partition8
          frame << partition7
          frame << topic_name2
          frame << "\x00\x00\x00\x01"
          frame << partition3
          response = described_class.decode(frame)
          topic1 = response.topic_offsets['topotopic']
          topic2 = response.topic_offsets['topicoco']
          expect(topic1.partition_offsets[7].offsets).to eq([0x0404040404040404, 0x0505050505050505, 0x0606060606060606])
          expect(topic1.partition_offsets[8].offsets).to eq([0x0202020202020202, 0x0303030303030303])
          expect(topic1.partition_offsets[9].offsets).to eq([0x0101010101010101])
          expect(topic2.partition_offsets[3].offsets).to eq([0x0707070707070707])
        end

        it 'decodes a frame with an error partition' do
          frame =  "\x00\x00\x00\x01"
          frame << topic_name1
          frame << "\x00\x00\x00\x01"
          frame << error_partition
          response = described_class.decode(frame)
          partition = response.topic_offsets['topotopic'].partition_offsets[0xff]
          expect(partition).to be_error
          expect(partition.topic_name).to eq('topotopic')
          expect(partition.partition_id).to eq(0xff)
          expect(partition.error).to be_a(Errors::NotLeaderForPartitionError)
        end

        it 'decodes a frame with error and non-error partitions' do
          frame =  "\x00\x00\x00\x02"
          frame << topic_name1
          frame << "\x00\x00\x00\x03"
          frame << partition8
          frame << error_partition
          frame << partition9
          frame << topic_name2
          frame << "\x00\x00\x00\x01"
          frame << partition3
          response = described_class.decode(frame)
          partition9 = response.topic_offsets['topotopic'].partition_offsets[9]
          partitionX = response.topic_offsets['topotopic'].partition_offsets[0xff]
          partition3 = response.topic_offsets['topicoco'].partition_offsets[3]
          expect(partition9).to_not be_error
          expect(partitionX).to be_error
          expect(partition3).to_not be_error
        end
      end

      describe '.merge' do
        let :response1 do
          described_class.new({
            'topotopic' => TopicOffsets.new('topotopic', {
              8 => PartitionOffsets.new('topotopic', 8, [88]),
              9 => PartitionOffsets.new('topotopic', 9, [99]),
            }),
            'topicoco' => TopicOffsets.new('topicoco', {
              3 => PartitionOffsets.new('topicoco', 3, [33]),
              4 => PartitionOffsets.new('topicoco', 4, [44]),
              5 => PartitionOffsets.new('topicoco', 5, [55]),
              6 => PartitionOffsets.new('topicoco', 6, [66]),
            }),
          })
        end

        let :response2 do
          described_class.new({
            'topotopic' => TopicOffsets.new('topotopic', {
              7 => PartitionOffsets.new('topotopic', 7, [77]),
            }),
            'topicoco' => TopicOffsets.new('topicoco', {
              2 => PartitionOffsets.new('topicoco', 2, [22]),
            }),
            'topopoco' => TopicOffsets.new('topopoco', {
              1 => PartitionOffsets.new('topopoco', 1, [11]),
            })
          })
        end

        let :response3 do
          described_class.new({
            'topicoco' => TopicOffsets.new('topicoco', {
              7 => PartitionOffsets.new('topicoco', 7, [77]),
            }),
            'topopoco' => TopicOffsets.new('topopoco', {
              2 => PartitionOffsets.new('topopoco', 2, [22]),
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
            responseX = described_class.new('topotopic' => TopicOffsets.new('topotopic', {8 => PartitionOffsets.new('topotopic', 8, [8888])}))
            merged_response = described_class.merge(response1, responseX)
            expect(merged_response.topic_offsets['topotopic'].partition_offsets[8].offsets).to eq([8888])
          end
        end
      end
    end
  end
end
