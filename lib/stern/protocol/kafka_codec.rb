module Stern
  module Protocol
    class KafkaCodec
      def recoding?
        false
      end

      def encode(message, channel)
        client_id = message.client_id
        client_id_size = client_id && client_id.bytesize || 0
        message_bytes = message.to_b
        components = [
          message_bytes.bytesize + 2 + 2 + 4 + 2 + client_id_size,
          message.api_key,
          api_version = 0,
          channel,
          client_id_size,
          client_id,
          message_bytes
        ]
        components.pack('NnnNna*a*')
      end

      def decode(buffer, state)
        state ||= State.new(buffer)
        if state.header_ready?
          state.read_header
        end
        if state.body_ready?
          body = state.read_body
          return body, state.channel, true
        else
          return state, nil, false
        end
      end

      class State
        attr_reader :channel

        def initialize(buffer)
          @buffer = buffer
        end

        def header_ready?
          @size.nil? && @buffer.length >= 4 + 4
        end

        def read_header
          @size = @buffer.read_int - 4
          @channel = @buffer.read_int
        end

        def body_ready?
          @size && @buffer.length >= @size
        end

        def read_body
          @buffer.read(@size)
        end
      end
    end
  end
end
