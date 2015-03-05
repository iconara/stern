require 'spec_helper'

module Stern
  module Protocol
    describe KafkaByteBuffer do
      describe '#read_string' do
        it 'reads a UTF-8 encoded string with a two byte length prefix' do
          buffer = described_class.new("\x00\x03foo")
          string = buffer.read_string
          expect(string).to eq('foo')
          expect(string.encoding).to eq(::Encoding::UTF_8)
        end

        it 'reads only as many bytes as the prefixed length' do
          buffer = described_class.new("\x00\x03foobar")
          string = buffer.read_string
          expect(string).to eq('foo')
          expect(buffer.to_s).to eq('bar')
        end

        it 'returns an empty string when the length is zero' do
          buffer = described_class.new("\x00\x00")
          expect(buffer.read_string).to be_empty
        end

        it 'returns nil when the length is a negative number' do
          buffer = described_class.new("\xff\xfffoobar")
          expect(buffer.read_string).to be_nil
          buffer = described_class.new("\x80\x00foobar")
          expect(buffer.read_string).to be_nil
        end

        it 'raises RangeError when not enough bytes are available' do
          buffer = described_class.new("\x00\x03fo")
          expect { buffer.read_string }.to raise_error(RangeError)
        end
      end

      describe '#read_bytes' do
        it 'reads a byte array with a four byte length prefix' do
          buffer = described_class.new("\x00\x00\x00\x03foo")
          string = buffer.read_bytes
          expect(string).to eq('foo')
          expect(string.encoding).to eq(::Encoding::BINARY)
        end

        it 'reads only as many bytes as the prefixed length' do
          buffer = described_class.new("\x00\x00\x00\x03foobar")
          string = buffer.read_bytes
          expect(string).to eq('foo')
          expect(buffer.to_s).to eq('bar')
        end

        it 'returns an empty string when the length is zero' do
          buffer = described_class.new("\x00\x00\x00\x00")
          expect(buffer.read_bytes).to be_empty
        end

        it 'returns nil when the length is a negative number' do
          buffer = described_class.new("\xff\xff\xff\xfffoobar")
          expect(buffer.read_bytes).to be_nil
          buffer = described_class.new("\x80\x00\x00\x00foobar")
          expect(buffer.read_bytes).to be_nil
        end

        it 'raises RangeError when not enough bytes are available' do
          buffer = described_class.new("\x00\x00\x00\x03fo")
          expect { buffer.read_bytes }.to raise_error(RangeError)
        end
      end

      describe '#read_short' do
        it 'reads a two byte integer' do
          buffer = described_class.new("\x01\x00")
          expect(buffer.read_short).to eq(0x0100)
        end

        it 'reads a two byte signed integer' do
          buffer = described_class.new("\xff\xff")
          expect(buffer.read_short(true)).to eq(-1)
          buffer = described_class.new("\x81\x00")
          expect(buffer.read_short(true)).to eq(-32512)
        end

        it 'reads only two bytes' do
          buffer = described_class.new("\x00\x00foo")
          buffer.read_short
          expect(buffer.to_s).to eq('foo')
        end

        it 'raises RangeError when not enough bytes are available' do
          buffer = described_class.new("\xff")
          expect { buffer.read_short }.to raise_error(RangeError)
        end
      end

      describe '#read_int' do
        it 'reads a four byte integer' do
          buffer = described_class.new("\x01\x00\x00\x00")
          expect(buffer.read_int).to eq(0x01000000)
        end

        it 'reads a four byte signed integer' do
          buffer = described_class.new("\xff\xff\xff\xff")
          expect(buffer.read_int(true)).to eq(-1)
          buffer = described_class.new("\x81\x00\x81\x00")
          expect(buffer.read_int(true)).to eq(-2130673408)
        end

        it 'reads only four bytes' do
          buffer = described_class.new("\x00\x00\x00\x00foo")
          buffer.read_int
          expect(buffer.to_s).to eq('foo')
        end

        it 'raises RangeError when not enough bytes are available' do
          buffer = described_class.new("\xff\xff\xff")
          expect { buffer.read_int }.to raise_error(RangeError)
        end
      end

      describe '#read_long' do
        it 'reads an eight byte integer' do
          buffer = described_class.new("\x01\x00\x00\x00\x01\x00\x00\x00")
          expect(buffer.read_long).to eq(0x0100000001000000)
        end

        it 'reads an eight four byte signed integer' do
          buffer = described_class.new("\xff\xff\xff\xff\xff\xff\xff\xff")
          expect(buffer.read_long(true)).to eq(-1)
          buffer = described_class.new("\x81\x00\x81\x00\x81\x00\x81\x00")
          expect(buffer.read_long(true)).to eq(-9151172603652570880)
        end

        it 'reads only eight bytes' do
          buffer = described_class.new("\x00\x00\x00\x00\x00\x00\x00\x00foo")
          buffer.read_long
          expect(buffer.to_s).to eq('foo')
        end

        it 'raises RangeError when not enough bytes are available' do
          buffer = described_class.new("\xff\xff\xff\xff\xff")
          expect { buffer.read_long }.to raise_error(RangeError)
        end
      end

      describe '#read_array' do
        it 'reads a length prefix, calls the block that many times and returns an array of the results' do
          buffer = described_class.new("\x00\x00\x00\x03\x01\x02\x03\x04\x05\x06")
          array = buffer.read_array { buffer.read_short }
          expect(array).to eq([0x0102, 0x0304, 0x0506])
        end

        it 'reads an empty array' do
          buffer = described_class.new("\x00\x00\x00\x00")
          array = buffer.read_array { }
          expect(array).to be_empty
        end

        it 'returns nil when the length is negative' do
          buffer = described_class.new("\xff\xff\xff\xff")
          array = buffer.read_array { }
          expect(array).to be_nil
        end
      end
    end
  end
end
