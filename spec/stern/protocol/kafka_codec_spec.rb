require 'spec_helper'

module Stern
  module Protocol
    describe KafkaCodec do
      let :codec do
        described_class.new
      end

      describe '#encode' do
        let :message do
          double(:message, to_b: 'this is the message', client_id: 'the client', api_key: 9)
        end

        context 'wraps the message in a header that' do
          it 'starts with the total length of the frame' do
            bytes = codec.encode(message, 3)
            expect(bytes).to start_with("\x00\x00\x00\x27")
          end

          it 'contains the message type/API key' do
            bytes = codec.encode(message, 3)
            expect(bytes[4, 2]).to eq("\x00\x09")
          end

          it 'contains the protocol/API version' do
            bytes = codec.encode(message, 3)
            expect(bytes[6, 2]).to eq("\x00\x00")
          end

          it 'contains the channel/correlation ID' do
            bytes = codec.encode(message, 3)
            expect(bytes[8, 4]).to eq("\x00\x00\x00\x03")
          end

          it 'contains the client ID' do
            bytes = codec.encode(message, 3)
            length = bytes[12, 2].unpack('n').first
            expect(bytes[14, length]).to eq('the client')
          end

          it 'contains an empty client ID' do
            allow(message).to receive(:client_id).and_return(nil)
            bytes = codec.encode(message, 3)
            length = bytes[12, 2].unpack('n').first
            expect(bytes[14, length]).to eq('')
          end

          it 'contains the encoded message' do
            bytes = codec.encode(message, 3)
            expect(bytes).to end_with('this is the message')
          end
        end
      end

      describe '#decode' do
        context 'when given a full frame' do
          let :buffer do
            buffer = Ione::ByteBuffer.new
            buffer << "\x00\x00\x00\x0d"
            buffer << "\x00\x00\x01\x00"
            buffer << "fake body"
            buffer
          end

          it 'returns the body' do
            body, _, _ = codec.decode(buffer, nil)
            expect(body).to eq('fake body')
          end

          it 'returns the channel/correlation ID' do
            _, channel, _ = codec.decode(buffer, nil)
            expect(channel).to eq(0x0100)
          end

          it 'returns true to signal that the frame has been completely decoded' do
            _, _, status = codec.decode(buffer, nil)
            expect(status).to be_truthy
          end
        end

        context 'when given a partial frame' do
          let :buffer do
            buffer = Ione::ByteBuffer.new
            buffer << "\x00\x00\x00\xff"
            buffer << "\x00\x00\x01\x00"
            buffer << "partial body"
            buffer
          end

          it 'returns a partial state' do
            state, _, _ = codec.decode(buffer, nil)
            expect(state).not_to be_nil
          end

          it 'returns no channel/correlation ID' do
            _, channel, _ = codec.decode(buffer, nil)
            expect(channel).to be_nil
          end

          it 'returns false to signal that the frame has not been completely decoded' do
            _, _, status = codec.decode(buffer, nil)
            expect(status).to be_falsy
          end
        end

        context 'when given a full frame, in pieces' do
          let :frame_fragments do
            [
              "\x00\x00\x00\x0d" + "\x00\x00",
              "\x01\x00" + "fake ",
              "body",
            ]
          end

          let :buffer do
            Ione::ByteBuffer.new
          end

          it 'uses the state from the previous call to eventually decode the frame' do
            state = nil
            status = nil
            frame_fragments.take(2).each do |fragment|
              buffer << fragment
              state, _, status = codec.decode(buffer, state)
              expect(status).to be_falsy
            end
            buffer << frame_fragments.last
            body, _, status = codec.decode(buffer, state)
            expect(status).to be_truthy
            expect(body).to eq('fake body')
          end
        end

        context 'when given more than a single frame' do
          let :buffer do
            buffer = Ione::ByteBuffer.new
            buffer << "\x00\x00\x00\x0d"
            buffer << "\x00\x00\x01\x00"
            buffer << "fake body and some trailing data"
            buffer
          end

          it 'leaves the trailing data in the buffer' do
            body, _, _ = codec.decode(buffer, nil)
            expect(body).to eq('fake body')
            expect(buffer.to_s).to eq(' and some trailing data')
          end
        end
      end
    end
  end
end
