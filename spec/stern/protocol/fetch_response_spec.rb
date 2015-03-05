# encoding: ascii-8bit

require 'spec_helper'

module Stern
  module Protocol
    describe FetchResponse do
      describe '.decode' do
        let :topic_name1 do
          "\x00\x09topotopic"
        end

        let :topic_name2 do
          "\x00\x08topicoco"
        end

        it 'decodes an empty frame' do
          frame = "\x00\x00\x00\x00"
          response = described_class.decode(frame)
          expect(response.topic_fetches).to be_empty
        end

        it 'decodes a frame with a single topic with no partitions' do
          frame =  "\x00\x00\x00\x01"
          frame << topic_name1
          frame << "\x00\x00\x00\x00"
          response = described_class.decode(frame)
          expect(response.topic_fetches['topotopic'].partition_fetches).to be_empty
        end

        it 'decodes a frame with a topic and a partition with an empty message set' do
          frame =  "\x00\x00\x00\x01"
          frame << topic_name1
          frame << "\x00\x00\x00\x01"
          frame << "\x00\x00\x00\x09"
          frame << "\x00\x00"
          frame << "\x10\x10\x10\x10\x10\x10\x10\x10"
          frame << "\x00\x00\x00\x00"
          response = described_class.decode(frame)
          partition_fetch = response.topic_fetches['topotopic'].partition_fetches[9]
          expect(partition_fetch.partition_id).to eq(9)
          expect(partition_fetch.hwm_offset).to eq(0x1010101010101010)
          expect(partition_fetch.message_set).to be_empty
        end

        it 'decodes a frame with multiple topics and partitions but empty message sets' do
          frame =  "\x00\x00\x00\x02"
          frame << topic_name1
          frame << "\x00\x00\x00\x03"
          frame << "\x00\x00\x00\x09"
          frame << "\x00\x00"
          frame << "\x90\x90\x90\x90\x90\x90\x90\x90"
          frame << "\x00\x00\x00\x00"
          frame << "\x00\x00\x00\x08"
          frame << "\x00\x00"
          frame << "\x80\x80\x80\x80\x80\x80\x80\x80"
          frame << "\x00\x00\x00\x00"
          frame << "\x00\x00\x00\x07"
          frame << "\x00\x00"
          frame << "\x70\x70\x70\x70\x70\x70\x70\x70"
          frame << "\x00\x00\x00\x00"
          frame << topic_name2
          frame << "\x00\x00\x00\x02"
          frame << "\x00\x00\x00\x03"
          frame << "\x00\x00"
          frame << "\x30\x30\x30\x30\x30\x30\x30\x30"
          frame << "\x00\x00\x00\x00"
          frame << "\x00\x00\x00\x04"
          frame << "\x00\x00"
          frame << "\x40\x40\x40\x40\x40\x40\x40\x40"
          frame << "\x00\x00\x00\x00"
          response = described_class.decode(frame)
          topic1 = response.topic_fetches['topotopic']
          topic2 = response.topic_fetches['topicoco']
          expect(topic1.partition_fetches.size).to eq(3)
          expect(topic1.partition_fetches[7].hwm_offset).to eq(0x7070707070707070)
          expect(topic2.partition_fetches.size).to eq(2)
        end

        it 'decodes a frame with an error partition' do
          frame =  "\x00\x00\x00\x01"
          frame << topic_name1
          frame << "\x00\x00\x00\x02"
          frame << "\x00\x00\x00\x09"
          frame << "\x00\x03"
          frame << "\x00\x00\x00\x00\x00\x00\x00\x00"
          frame << "\x00\x00\x00\x00"
          frame << "\x00\x00\x00\x08"
          frame << "\x00\x00"
          frame << "\x00\x00\x00\x00\x00\x00\x00\x00"
          frame << "\x00\x00\x00\x00"
          response = described_class.decode(frame)
          partition_fetch8 = response.topic_fetches['topotopic'].partition_fetches[8]
          partition_fetch9 = response.topic_fetches['topotopic'].partition_fetches[9]
          expect(partition_fetch8).to_not be_error
          expect(partition_fetch9).to be_error
        end

        it 'decodes a frame with a message set' do
          frame =  "\x00\x00\x00\x01"
          frame << topic_name1
          frame << "\x00\x00\x00\x01"
          frame << "\x00\x00\x00\x09"
          frame << "\x00\x00"
          frame << "\x00\x00\x00\x00\x00\x00\x00\x00"
          frame << "\x00\x00\x00\x17"
          frame << "ich bin ein message set"
          response = described_class.decode(frame)
          partition_fetch = response.topic_fetches['topotopic'].partition_fetches[9]
          expect(partition_fetch.message_set.to_b).to eq('ich bin ein message set')
        end
      end

      describe '.merge' do
        let :response1 do
          described_class.new({
            'topotopic' => TopicFetch.new('topotopic', {
              9 => PartitionFetch.new('topotopic', 9, 9999, MessageSet.new([Message.new('k1', 'v1', 1)])),
              8 => PartitionFetch.new('topotopic', 8, 8888, MessageSet.new([Message.new('k2', 'v2', 2)])),
            }),
            'topicoco' => TopicFetch.new('topicoco', {
              5 => PartitionFetch.new('topicoco', 5, 5555, MessageSet.new([Message.new('k7', 'v7', 7)])),
              6 => PartitionFetch.new('topicoco', 6, 6666, MessageSet.new([Message.new('k8', 'v8', 8)])),
              7 => PartitionFetch.new('topicoco', 7, 7777, MessageSet.new([Message.new('k9', 'v9', 9)])),
            }),
          })
        end

        let :response2 do
          described_class.new({
            'topotopic' => TopicFetch.new('topotopic', {
              7 => PartitionFetch.new('topotopic', 7, 7777, MessageSet.new([Message.new('k3', 'v3', 3)])),
            }),
            'topicoco' => TopicFetch.new('topicoco', {
              2 => PartitionFetch.new('topicoco', 2, 2222, MessageSet.new([Message.new('k4', 'v4', 4)])),
              3 => PartitionFetch.new('topicoco', 3, 3333, MessageSet.new([Message.new('k5', 'v5', 5)])),
            }),
          })
        end

        let :response3 do
          described_class.new({
            'topicoco' => TopicFetch.new('topicoco', {
              2 => PartitionFetch.new('topicoco', 2, 2222, MessageSet.new([Message.new('k4', 'v4', 4)])),
              3 => PartitionFetch.new('topicoco', 3, 3333, MessageSet.new([Message.new('k5', 'v5', 5)])),
              4 => PartitionFetch.new('topicoco', 4, 4444, MessageSet.new([Message.new('k6', 'v6', 6)])),
            }),
            'topopoco' => TopicFetch.new('topopoco', {
              1 => PartitionFetch.new('topopoco', 1, 1111, MessageSet.new([Message.new('k10', 'v10', 10)])),
              2 => PartitionFetch.new('topopoco', 2, 2222, MessageSet.new([Message.new('k11', 'v11', 11)])),
            }),
          })
        end

        it 'returns a fetch response with the fetches from all the given fetch responses' do
          merged_response = described_class.merge(response1, response2, response3)
          expect(merged_response.topic_fetches.keys).to match_array(%w[topotopic topopoco topicoco])
          expect(merged_response.topic_fetches['topotopic'].partition_fetches.keys).to match_array([7, 8, 9])
          expect(merged_response.topic_fetches['topicoco'].partition_fetches.keys).to match_array([2, 3, 4, 5, 6, 7])
          expect(merged_response.topic_fetches['topopoco'].partition_fetches.keys).to match_array([1, 2])
        end

        context 'when the same topic/partition combination exists in more than one response' do
          it 'picks the last' do
            responseX = described_class.new('topotopic' => TopicFetch.new('topotopic', {8 => PartitionFetch.new('topotopic', 8, 8888, MessageSet.new([Message.new('k999', 'v999', 999)]))}))
            merged_response = described_class.merge(response1, responseX)
            expect(merged_response.topic_fetches['topotopic'].partition_fetches[8].message_set.first).to eq(Message.new('k999', 'v999', 999))
          end
        end

      end
    end
  end
end
