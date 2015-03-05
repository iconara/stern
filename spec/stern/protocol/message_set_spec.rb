# encoding: ascii-8bit

require 'spec_helper'

module Stern
  module Protocol
    describe Message do
      describe '#eql?' do
        it 'is equal to another message with the same properties' do
          m1 = Message.new('key', 'value', 1)
          m2 = Message.new('key', 'value', 1)
          expect(m1).to eql(m2)
        end

        it 'is aliased as #==' do
          m1 = Message.new('key', 'value', 1)
          m2 = Message.new('key', 'value', 1)
          expect(m1).to eq(m2)
        end

        it 'is not equal to nil' do
          m1 = Message.new('key', 'value', 1)
          expect(m1).to_not eql(nil)
        end

        it 'is not equal when the key is different' do
          m1 = Message.new('key', 'value', 1)
          m2 = Message.new('yek', 'value', 1)
          expect(m1).to_not eql(m2)
        end

        it 'is not equal when the value is different' do
          m1 = Message.new('key', 'value', 1)
          m2 = Message.new('key', 'eulav', 1)
          expect(m1).to_not eql(m2)
        end

        it 'is not equal when the offset is different' do
          m1 = Message.new('key', 'value', 1)
          m2 = Message.new('key', 'value', 2)
          expect(m1).to_not eql(m2)
        end
      end

      describe '#hash' do
        it 'is the same when the properties are the same' do
          m1 = Message.new('key', 'value', 1)
          m2 = Message.new('key', 'value', 1)
          expect(m1.hash).to eq(m2.hash)
        end

        it 'is not the same when the key is different' do
          m1 = Message.new('key', 'value', 1)
          m2 = Message.new('yek', 'value', 1)
          expect(m1.hash).to_not eq(m2.hash)
        end

        it 'is not the same when the value is different' do
          m1 = Message.new('key', 'value', 1)
          m2 = Message.new('key', 'eulav', 1)
          expect(m1.hash).to_not eq(m2.hash)
        end

        it 'is not the same when the offset is different' do
          m1 = Message.new('key', 'value', 1)
          m2 = Message.new('key', 'value', 2)
          expect(m1.hash).to_not eq(m2.hash)
        end
      end
    end

    describe MessageSet do
      let :messages do
        [
          Message.new('k1', 'v1', 1),
          Message.new('k2', 'v2', 2),
          Message.new('k3', 'v3', 3),
        ]
      end

      let :message_set do
        described_class.new(messages)
      end

      describe '#to_b' do
        it 'encodes an empty message set to an empty byte array' do
          bytes = described_class.new([]).to_b
          expect(bytes).to be_empty
        end

        it 'encodes a message set with a single message' do
          bytes = described_class.new([Message.new('foo', 'bar', 123456)]).to_b
          expect(bytes).to eql(
            "\x00\x00\x00\x00\x00\x01\xe2\x40" +
            "\x00\x00\x00\x14" +
            "\xb8\xba\x5f\x57" +
            "\x00" +
            "\x00" +
            "\x00\x00\x00\x03foo" +
            "\x00\x00\x00\x03bar"
          )
        end

        it 'encodes a message set with multiple messages' do
          messages = [
            Message.new('foo', 'bar', 0x1234),
            Message.new('hello', 'world', 0x5678),
            Message.new('biff', 'boff', 0x9abc),
          ]
          bytes = described_class.new(messages).to_b
          expect(bytes).to eql(
            "\x00\x00\x00\x00\x00\x00\x12\x34" +
            "\x00\x00\x00\x14" +
            "\xb8\xba\x5f\x57" +
            "\x00" +
            "\x00" +
            "\x00\x00\x00\x03foo" +
            "\x00\x00\x00\x03bar" +
            "\x00\x00\x00\x00\x00\x00\x56\x78" +
            "\x00\x00\x00\x18" +
            "\x6b\xe8\xd0\x32" +
            "\x00" +
            "\x00" +
            "\x00\x00\x00\x05hello" +
            "\x00\x00\x00\x05world" +
            "\x00\x00\x00\x00\x00\x00\x9a\xbc" +
            "\x00\x00\x00\x16" +
            "\xb\x80\x6e\xf1" +
            "\x00" +
            "\x00" +
            "\x00\x00\x00\x04biff" +
            "\x00\x00\x00\x04boff"
          )
        end

        it 'encodes a message set with a message with a nil key' do
          bytes = described_class.new([Message.new(nil, 'bar', 123456)]).to_b
          expect(bytes).to eql(
            "\x00\x00\x00\x00\x00\x01\xe2\x40" +
            "\x00\x00\x00\x11" +
            "\x0\x7\xf2\xc7" +
            "\x00" +
            "\x00" +
            "\xff\xff\xff\xff" +
            "\x00\x00\x00\x03bar"
          )
        end

        it 'encodes a message set with a message with a nil value' do
          bytes = described_class.new([Message.new('foo', nil, 123456)]).to_b
          expect(bytes).to eql(
            "\x00\x00\x00\x00\x00\x01\xe2\x40" +
            "\x00\x00\x00\x11" +
            "\x55\xe6\x45\x4e" +
            "\x00" +
            "\x00" +
            "\x00\x00\x00\x03foo" +
            "\xff\xff\xff\xff"
          )
        end
      end

      describe '.decode' do
        context 'when the byte array is empty' do
          it 'returns an empty message set' do
            decoded_message_set = described_class.decode('')
            expect(decoded_message_set).to be_empty
          end
        end

        context 'with a byte array containing a key/value pair' do
          let :bytes do
            "\x01\x01\x01\x01\x01\x01\x01\x01" +
            "\x00\x00\x00\x1e" +
            "\x00\x00\x00\x00" + # TODO
            "\x00" +
            "\x00" +
            "\x00\x00\x00\x07" +
            "the key" +
            "\x00\x00\x00\x09" +
            "the value"
          end

          it 'returns a message set with the message' do
            message_set = described_class.decode(bytes)
            expect(message_set.messages.size).to eq(1)
            expect(message_set.messages).to eq([Message.new('the key', 'the value', 0x0101010101010101)])
          end
        end

        context 'with a byte array containing many key/value pairs' do
          let :bytes do
            "\x01\x01\x01\x01\x01\x01\x01\x01" +
            "\x00\x00\x00\x1e" +
            "\x00\x00\x00\x00" + # TODO
            "\x00" +
            "\x00" +
            "\x00\x00\x00\x07" +
            "the key" +
            "\x00\x00\x00\x09" +
            "the value" +
            "\x01\x01\x01\x01\x01\x01\x01\x02" +
            "\x00\x00\x00\x26" +
            "\x00\x00\x00\x00" + # TODO
            "\x00" +
            "\x00" +
            "\x00\x00\x00\x0b" +
            "another key" +
            "\x00\x00\x00\x0d" +
            "another value" +
            "\x01\x01\x01\x01\x01\x01\x01\x03" +
            "\x00\x00\x00\x26" +
            "\x00\x00\x00\x00" + # TODO
            "\x00" +
            "\x00" +
            "\x00\x00\x00\x0b" +
            "a third key" +
            "\x00\x00\x00\x0d" +
            "a third value"
          end

          it 'returns a message set with the messages' do
            message_set = described_class.decode(bytes)
            expect(message_set.messages.size).to eq(3)
            expect(message_set.messages).to eq([
              Message.new('the key', 'the value', 0x0101010101010101),
              Message.new('another key', 'another value', 0x0101010101010102),
              Message.new('a third key', 'a third value', 0x0101010101010103),
            ])
          end
        end

        context 'with a byte array containing a trailing partial message' do
          let :bytes do
            "\x01\x01\x01\x01\x01\x01\x01\x01" +
            "\x00\x00\x00\x1e" +
            "\x00\x00\x00\x00" + # TODO
            "\x00" +
            "\x00" +
            "\x00\x00\x00\x07" +
            "the key" +
            "\x00\x00\x00\x09" +
            "the value" +
            "\x01\x01\x01\x01\x01\x01\x01\x02" +
            "\x00\x00\x00\xff" +
            "\x00\x00\x00\x00" +
            "\x00" +
            "\x00" +
            "burk bork birk"
          end

          it 'returns a message set with the other messages' do
            message_set = described_class.decode(bytes)
            expect(message_set.messages.size).to eq(1)
            expect(message_set.messages).to eq([Message.new('the key', 'the value', 0x0101010101010101)])
          end

          it 'sets the partial flag' do
            message_set = described_class.decode(bytes)
            expect(message_set).to be_partial
          end
        end

        context 'with a byte array containing a message with a nil key' do
          let :bytes do
            "\x01\x01\x01\x01\x01\x01\x01\x01" +
            "\x00\x00\x00\x17" +
            "\x00\x00\x00\x00" + # TODO
            "\x00" +
            "\x00" +
            "\xff\xff\xff\xff" +
            "\x00\x00\x00\x09" +
            "the value"
          end

          it 'returns a message set with a message with a nil key' do
            message_set = described_class.decode(bytes)
            expect(message_set.messages).to eq([Message.new(nil, 'the value', 0x0101010101010101)])
          end
        end

        context 'with a byte array containing a message with a nil value' do
          let :bytes do
            "\x01\x01\x01\x01\x01\x01\x01\x01" +
            "\x00\x00\x00\x0e" +
            "\x00\x00\x00\x00" + # TODO
            "\x00" +
            "\x00" +
            "\x00\x00\x00\x07" +
            "the key" +
            "\xff\xff\xff\xff"
          end

          it 'returns a message set with a message with a nil key' do
            message_set = described_class.decode(bytes)
            expect(message_set.messages).to eq([Message.new('the key', nil, 0x0101010101010101)])
          end
        end

        context 'when the message set is Snappy compressed' do
          let :compressed_message_set do
            io = StringIO.new
            writer = Snappy::Writer.new(io)
            writer << "\x01\x01\x01\x01\x01\x01\x01\x01"
            writer << "\x00\x00\x00\x1e"
            writer << "\x00\x00\x00\x00" # TODO
            writer << "\x00"
            writer << "\x00"
            writer << "\x00\x00\x00\x07"
            writer << "the key"
            writer << "\x00\x00\x00\x09"
            writer << "the value"
            writer.flush
            io.string
          end

          let :bytes do
            "\x33\x33\x33\x33\x33\x33\x33\x33" +
            ([compressed_message_set.bytesize + 10]).pack('N') +
            "\x00\x00\x00\x00" + # TODO
            "\x00" +
            "\x02" +
            "\xff\xff\xff\xff" +
            [compressed_message_set.bytesize].pack('N') +
            compressed_message_set
          end

          it 'decompresses the buffer and returns the message set' do
            message_set = described_class.decode(bytes)
            expect(message_set.messages).to eq([Message.new('the key', 'the value', 0x0101010101010101)])
          end

          it 'sets the partial flag when the compressed message set contains a trailing partial frame'
        end

        context 'when the message set is Gzip compressed' do
          it 'decompresses the buffer and returns the message set'
          it 'sets the partial flag when the compressed message set contains a trailing partial frame'
        end

        it 'verifies the message set checksum'
      end

      describe '#empty?' do
        it 'returns true when the message set has no messages' do
          expect(described_class.new([])).to be_empty
        end

        it 'returns false when the message set has messages' do
          expect(message_set).to_not be_empty
        end
      end

      describe '#size' do
        it 'returns the number of messages in the message set' do
          expect(described_class.new([]).size).to eq(0)
          expect(described_class.new(messages.take(1)).size).to eq(1)
          expect(described_class.new(messages).size).to eq(3)
        end
      end

      describe '#each' do
        it 'yields each message' do
          yielded_messages = []
          message_set.each { |m| yielded_messages << m }
          expect(yielded_messages).to eq(message_set.messages)
        end

        it 'returns an enumerator' do
          keys = message_set.each.map { |m| m.key }
          expect(keys).to eq(%w[k1 k2 k3])
        end
      end

      describe '#messages' do
        it 'returns an array of the messages in the message set' do
          expect(message_set.messages).to eq(messages)
        end

        it 'is aliased as #to_a' do
          expect(message_set.to_a).to eq(message_set.messages)
        end

        it 'is aliased as #to_ary' do
          messages = *message_set
          expect(messages).to eq(message_set.messages)
        end
      end

      context 'used as an Enumerable' do
        it 'can be mapped' do
          values = message_set.map { |m| m.value }
          expect(values).to eq(%w[v1 v2 v3])
        end

        it 'can be filtered' do
          even_offsets = message_set.reject { |m| m.offset % 2 != 0 }
          expect(even_offsets).to eq([messages[1]])
        end
      end

      describe '#eql?' do
        it 'is equal to another message set with the same messages' do
          ms1 = described_class.new(messages)
          ms2 = described_class.new(messages)
          expect(ms1).to eql(ms2)
        end

        it 'is equal to another message set with the same messages, even when one had a trailing partial message' do
          ms1 = described_class.new(messages)
          ms2 = described_class.new(messages, true)
          expect(ms1).to eql(ms2)
        end

        it 'is not equal to nil' do
          ms1 = described_class.new(messages)
          expect(ms1).not_to eql(nil)
        end

        it 'is aliased as #==' do
          ms1 = described_class.new(messages)
          ms2 = described_class.new(messages)
          expect(ms1).to eq(ms2)
        end

        it 'is not equal to another message set with different messages' do
          ms1 = described_class.new(messages)
          ms2 = described_class.new(messages.take(2))
          expect(ms1).to_not eql(ms2)
        end
      end

      describe '#hash' do
        it 'is the same as another message set that has the same messages' do
          ms1 = described_class.new(messages)
          ms2 = described_class.new(messages)
          expect(ms1.hash).to eql(ms2.hash)
        end

        it 'is the same as another message set that has the same messages, even when one had a trailing partial message' do
          ms1 = described_class.new(messages)
          ms2 = described_class.new(messages, true)
          expect(ms1.hash).to eql(ms2.hash)
        end

        it 'is not the same as another message set that has different messages' do
          ms1 = described_class.new(messages)
          ms2 = described_class.new(messages.take(2))
          expect(ms1.hash).to_not eql(ms2.hash)
        end
      end
    end

    describe EncodedMessageSet do
      let :message_set_bytes do
        "\x01\x01\x01\x01\x01\x01\x01\x01" +
        "\x00\x00\x00\x1e" +
        "\x00\x00\x00\x00" + # TODO
        "\x00" +
        "\x00" +
        "\x00\x00\x00\x07" +
        "the key" +
        "\x00\x00\x00\x09" +
        "the value" +
        "\x01\x01\x01\x01\x01\x01\x01\x02" +
        "\x00\x00\x00\x26" +
        "\x00\x00\x00\x00" + # TODO
        "\x00" +
        "\x00" +
        "\x00\x00\x00\x0b" +
        "another key" +
        "\x00\x00\x00\x0d" +
        "another value" +
        "\x01\x01\x01\x01\x01\x01\x01\x03" +
        "\x00\x00\x00\x26" +
        "\x00\x00\x00\x00" + # TODO
        "\x00" +
        "\x00" +
        "\x00\x00\x00\x0b" +
        "a third key" +
        "\x00\x00\x00\x0d" +
        "a third value"
      end

      let :messages do
        [
          Message.new('the key', 'the value', 0x0101010101010101),
          Message.new('another key', 'another value', 0x0101010101010102),
          Message.new('a third key', 'a third value', 0x0101010101010103),
        ]
      end

      describe '#each' do
        it 'decodes the message set and yields each message' do
          yielded_messages = []
          message_set = described_class.new(message_set_bytes)
          message_set.each { |msg| yielded_messages << msg }
          expect(yielded_messages).to eq(messages)
        end
      end

      describe '#messages' do
        it 'decodes the message set and returns the messages' do
          message_set = described_class.new(message_set_bytes)
          expect(message_set.messages).to eq(messages)
        end
      end

      describe '#empty?' do
        it 'returns true when the byte array is empty' do
          expect(described_class.new('')).to be_empty
        end

        it 'returns true when the byte array does not contain messages' do
          bytes =  "\x01\x01\x01\x01\x01\x01\x01\x01"
          bytes << "\x00\x00\x00\x00"
          expect(described_class.new(bytes)).to be_empty
        end

        it 'returns false when the byte array contains messages' do
          expect(described_class.new(message_set_bytes)).to_not be_empty
        end
      end

      describe '#to_b' do
        it 'returns the bytes it was created with' do
          expect(described_class.new('foobar').to_b).to eq('foobar')
        end
      end

      describe '#eql?' do
        it 'is equal to another encoded message set with the same messages' do
          ms1 = described_class.new('foobar')
          ms2 = described_class.new('foobar')
          expect(ms1).to eql(ms2)
        end

        it 'is equal to another message set with the same messages' do
          ms1 = described_class.new(message_set_bytes)
          ms2 = MessageSet.new(messages)
          expect(ms1).to eql(ms2)
        end

        it 'is aliased as #==' do
          ms1 = described_class.new('foobar')
          ms2 = described_class.new('foobar')
          expect(ms1).to eq(ms2)
        end

        it 'is not equal to nil' do
          ms1 = described_class.new(message_set_bytes)
          expect(ms1).to_not eq(nil)
        end

        it 'is not equal to another encoded message set with different content' do
          ms1 = described_class.new('foo')
          ms2 = described_class.new('foobar')
          expect(ms1).to_not eq(ms2)
        end

        it 'is not equal to another message set with different content' do
          ms1 = described_class.new(message_set_bytes)
          ms2 = MessageSet.new(messages.take(2))
          expect(ms1).to_not eql(ms2)
        end
      end

      describe '#hash' do
        it 'is the same when the byte content is the same' do
          ms1 = described_class.new('foobar')
          ms2 = described_class.new('foobar')
          expect(ms1.hash).to eq(ms2.hash)
        end

        it 'is not the same when the byte contents are different' do
          ms1 = described_class.new('foo')
          ms2 = described_class.new('foobar')
          expect(ms1.hash).to_not eq(ms2.hash)
        end
      end
    end
  end
end
