module Stern
  module Utils
    module FnvHash
      FNV_OFFSET_BASIS = 0xcbf29ce484222325
      FNV_PRIME = 0x100000001b3
      LONG_MASK = 0xffffffffffffffff

      def fnv_hash(*parts)
        h = parts.reduce(FNV_OFFSET_BASIS) do |h, part|
          LONG_MASK & ((h ^ part.hash) * FNV_PRIME)
        end
        0x7fffffffffffffff - h
      end
    end
  end
end
