# encoding: ascii-8bit

require 'spec_helper'

module Stern
  module Protocol
    describe MetadataResponse do
      describe '.decode' do
        let :broker1 do
          "\x01\x02\x03\x04" +
          "\x00\x0fone.example.com" +
          "\x00\x00\x23\x84"
        end

        let :broker2 do
          "\x05\x06\x07\x08" +
          "\x00\x0ftwo.example.com" +
          "\x00\x00\x23\x84"
        end

        let :broker3 do
          "\x09\x0a\x0b\x0c" +
          "\x00\x11three.example.com" +
          "\x00\x00\x23\x84"
        end

        let :topic_name1 do
          "\x00\x09topotopic"
        end

        let :topic_name2 do
          "\x00\x08topicoco"
        end

        let :partition1 do
          "\x00\x00" +
          "\x01\x01\x01\x01" +
          broker1[0, 4] +
          "\x00\x00\x00\x02" +
          broker2[0, 4] +
          broker3[0, 4] +
          "\x00\x00\x00\x01" +
          broker3[0, 4]
        end

        let :partition2 do
          "\x00\x00" +
          "\x02\x02\x02\x02" +
          broker2[0, 4] +
          "\x00\x00\x00\x01" +
          broker1[0, 4] +
          "\x00\x00\x00\x01" +
          broker1[0, 4]
        end

        let :error_partition do
          "\x00\x06" +
          "\x01\x01\x01\x01" +
          "\xff\xff\xff\xff" +
          "\xff\xff\xff\xff" +
          "\xff\xff\xff\xff"
        end

        it 'decodes an empty frame' do
          frame =  "\x00\x00\x00\x00"
          frame << "\x00\x00\x00\x00"
          response = described_class.decode(frame)
          expect(response.topic_metadata).to be_empty
        end

        it 'decodes a frame with an empty topic' do
          frame =  "\x00\x00\x00\x00"
          frame << "\x00\x00\x00\x01"
          frame << "\x00\x00"
          frame << topic_name1
          frame << "\x00\x00\x00\x00"
          response = described_class.decode(frame)
          partitions = response.topic_metadata['topotopic'].partition_metadata
          expect(partitions).to be_empty
        end

        it 'decodes a frame with a single topic with two partitions' do
          frame =  "\x00\x00\x00\x03"
          frame << broker1
          frame << broker2
          frame << broker3
          frame << "\x00\x00\x00\x01"
          frame << "\x00\x00"
          frame << topic_name1
          frame << "\x00\x00\x00\x02"
          frame << partition1
          frame << partition2
          response = described_class.decode(frame)
          partitions = response.topic_metadata['topotopic'].partition_metadata
          partition1 = partitions[0x01010101]
          partition2 = partitions[0x02020202]
          expect(partition1.topic_name).to eq('topotopic')
          expect(partition1.partition_id).to eq(0x01010101)
          expect(partition2.topic_name).to eq('topotopic')
          expect(partition2.partition_id).to eq(0x02020202)
        end

        it 'decodes the list of brokers and replaces references to their node IDs with Broker objects' do
          frame =  "\x00\x00\x00\x03"
          frame << broker1
          frame << broker2
          frame << broker3
          frame << "\x00\x00\x00\x01"
          frame << "\x00\x00"
          frame << topic_name1
          frame << "\x00\x00\x00\x02"
          frame << partition1
          frame << partition2
          response = described_class.decode(frame)
          partitions = response.topic_metadata['topotopic'].partition_metadata
          partition1 = partitions[0x01010101]
          partition2 = partitions[0x02020202]
          expect(partition1.leader).to eq(Broker.new(0x01020304, 'one.example.com', 9092))
          expect(partition1.replicas).to include(Broker.new(0x05060708, 'two.example.com', 9092))
          expect(partition1.isr).to include(Broker.new(0x090a0b0c, 'three.example.com', 9092))
        end

        it 'decodes a frame with multiple topics' do
          frame =  "\x00\x00\x00\x03"
          frame << broker1
          frame << broker2
          frame << broker3
          frame << "\x00\x00\x00\x02"
          frame << "\x00\x00"
          frame << topic_name1
          frame << "\x00\x00\x00\x01"
          frame << partition1
          frame << "\x00\x00"
          frame << topic_name2
          frame << "\x00\x00\x00\x01"
          frame << partition2
          response = described_class.decode(frame)
          topic1 = response.topic_metadata['topotopic']
          topic2 = response.topic_metadata['topicoco']
          expect(topic1.partition_metadata.size).to eq(1)
          expect(topic2.partition_metadata.size).to eq(1)
        end

        it 'decodes a frame with a topic error' do
          frame =  "\x00\x00\x00\x00"
          frame << "\x00\x00\x00\x01"
          frame << "\x00\x03"
          frame << topic_name1
          frame << "\x00\x00\x00\x00"
          response = described_class.decode(frame)
          topic = response.topic_metadata['topotopic']
          expect(topic).to be_error
          expect(topic.topic_name).to eq('topotopic')
          expect(topic.error).to be_a(Errors::UnknownTopicOrPartitionError)
        end

        it 'decodes an frame with an error topic and a non-error topic' do
          frame =  "\x00\x00\x00\x03"
          frame << broker1
          frame << broker2
          frame << broker3
          frame << "\x00\x00\x00\x02"
          frame << "\x00\x03"
          frame << topic_name1
          frame << "\x00\x00\x00\x00"
          frame << "\x00\x00"
          frame << topic_name2
          frame << "\x00\x00\x00\x01"
          frame << partition1
          response = described_class.decode(frame)
          topic1 = response.topic_metadata['topotopic']
          topic2 = response.topic_metadata['topicoco']
          expect(topic1).to be_error
          expect(topic1.error).to be_a(Errors::UnknownTopicOrPartitionError)
          expect(topic2).to_not be_error
          expect(topic2.partition_metadata.size).to eq(1)
        end

        it 'skips the partitions of topics with errors' do
          frame =  "\x00\x00\x00\x01"
          frame << broker1
          frame << "\x00\x00\x00\x01"
          frame << "\x00\x03"
          frame << topic_name1
          frame << "\x00\x00\x00\x01"
          frame << partition1
          response = described_class.decode(frame)
          topic = response.topic_metadata['topotopic']
          expect(topic).to be_error
          expect(topic.partition_metadata).to be_empty
        end

        it 'decodes a frame with a partition error' do
          frame =  "\x00\x00\x00\x01"
          frame << broker1
          frame << "\x00\x00\x00\x01"
          frame << "\x00\x00"
          frame << topic_name1
          frame << "\x00\x00\x00\x02"
          frame << error_partition
          frame << partition2
          response = described_class.decode(frame)
          topic = response.topic_metadata['topotopic']
          partition1 = topic.partition_metadata[0x01010101]
          partition2 = topic.partition_metadata[0x02020202]
          expect(partition1).to be_error
          expect(partition1.topic_name).to eq('topotopic')
          expect(partition1.partition_id).to eq(0x01010101)
          expect(partition1.error).to be_a(Errors::NotLeaderForPartitionError)
          expect(partition1.leader).to be_nil
          expect(partition1.replicas).to be_empty
          expect(partition1.isr).to be_empty
          expect(partition2).to_not be_error
        end
      end
    end
  end
end
