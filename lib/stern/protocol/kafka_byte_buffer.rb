require 'stringio'
require 'snappy'
require 'ione'

module Stern
  module Protocol
    class KafkaByteBuffer < Ione::ByteBuffer
      def read_short(signed=false)
        n = super()
        if signed && n > 0x7fff
          n - 0xffff - 1
        else
          n
        end
      end

      def read_int(signed=false)
        n = super()
        if signed && n > 0x7fffffff
          n - 0xffffffff - 1
        else
          n
        end
      end

      def read_long(signed=false)
        n = read_int << 32 | read_int
        if signed && n > 0x7fffffffffffffff
          n - 0xffffffffffffffff -1
        else
          n
        end
      end

      def read_string
        if (length = read_short(true)) >= 0
          read(length).force_encoding(::Encoding::UTF_8)
        else
          nil
        end
      end

      def read_bytes
        if (length = read_int(true)) >= 0
          read(length)
        else
          nil
        end
      end

      def read_array
        if (size = read_int(true)) >= 0
          Array.new(size) { yield }
        else
          nil
        end
      end
    end
  end
end
