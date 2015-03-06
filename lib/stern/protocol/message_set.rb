require 'zlib'

module Stern
  module Protocol
    class MessageSet
      include Enumerable

      attr_reader :messages
      alias_method :to_a, :messages
      alias_method :to_ary, :messages

      def initialize(messages, partial=false)
        @messages = messages
        @partial = partial
      end

      def partial?
        @partial
      end

      def size
        messages.size
      end

      def empty?
        messages.empty?
      end

      def each
        if block_given?
          messages.each { |m| yield m }
        else
          messages.each
        end
      end

      def eql?(other)
        !other.nil? && messages.eql?(other.messages)
      end
      alias_method :==, :eql?

      def hash
        messages.hash
      end

      def to_b
        components = []
        format = 'Q>Na*' * messages.size
        messages.each do |message|
          bytes = message.to_b
          components << message.offset
          components << bytes.bytesize
          components << bytes
        end
        components.pack(format)
      end

      def self.decode(bytes)
        buffer = KafkaByteBuffer.new(bytes)
        messages = []
        partial = false
        until buffer.empty?
          offset = buffer.read_long
          message_size = buffer.read_int
          if message_size > 0 && message_size <= buffer.bytesize
            crc = buffer.read_int
            magic = buffer.read_byte
            attributes = buffer.read_byte
            key = buffer.read_bytes
            value = buffer.read_bytes
            if attributes & 0b11 == 0b10
              io = StringIO.new(value)
              reader = Snappy::Reader.new(io)
              message_set = decode(reader.read)
              messages.concat(message_set.messages)
              partial = message_set.partial?
            else
              messages << Message.new(key, value, offset)
            end
          elsif message_size > 0
            partial = true
            break
          end
        end
        MessageSet.new(messages, partial)
      end
    end

    class Message
      include Utils::FnvHash

      attr_reader :key, :value, :offset

      def initialize(key, value, offset=-1)
        @key = key
        @value = value
        @offset = offset
      end

      def eql?(other)
        !other.nil? && @key.eql?(other.key) && @value.eql?(other.value) && @offset.eql?(other.offset)
      end
      alias_method :==, :eql?

      def hash
        fnv_hash(@key, @value, @offset)
      end

      def to_b
        bytes = [
          0,
          0,
          @key ? @key.bytesize : -1,
          @key,
          @value ? @value.bytesize : -1,
          @value
        ].pack('ccNa*Na*')
        bytes = [Zlib.crc32(bytes)].pack('N') << bytes
        bytes
      end
    end

    class EncodedMessageSet < MessageSet
      def initialize(bytes)
        @bytes = bytes
      end

      def messages
        decode
        super
      end
      alias_method :to_a, :messages
      alias_method :to_ary, :messages

      def empty?
        @bytes.empty? || messages.empty?
      end

      def to_b
        @bytes
      end

      def eql?(other)
        if other.is_a?(self.class)
          to_b.eql?(other.to_b)
        else
          super
        end
      end
      alias_method :==, :eql?

      def hash
        @bytes.hash
      end

      private

      def decode
        unless @decoded
          ms = self.class.decode(@bytes)
          @messages = ms.messages
          @partial = ms.partial?
          @decoded = true
        end
      end
    end
  end
end
