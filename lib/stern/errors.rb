module Stern
  module Errors
    SternError = Class.new(StandardError)

    class KafkaError < SternError
      attr_reader :code

      def initialize(code, message='')
        super(message)
        @code = code
      end
    end

    class UnknownError < KafkaError
      def initialize
        super(-1)
      end
    end

    class OffsetOutOfRangeError < KafkaError
      def initialize
        super(1)
      end
    end

    class InvalidMessageError < KafkaError
      def initialize
        super(2)
      end
    end

    class UnknownTopicOrPartitionError < KafkaError
      def initialize
        super(3)
      end
    end

    class InvalidMessageSizeError < KafkaError
      def initialize
        super(4)
      end
    end

    class LeaderNotAvailableError < KafkaError
      def initialize
        super(5)
      end
    end

    class NotLeaderForPartitionError < KafkaError
      def initialize
        super(6)
      end
    end

    class RequestTimedOutError < KafkaError
      def initialize
        super(7)
      end
    end

    class BrokerNotAvailableError < KafkaError
      def initialize
        super(8)
      end
    end

    class ReplicaNotAvailableError < KafkaError
      def initialize
        super(9)
      end
    end

    class MessageSizeTooLargeError < KafkaError
      def initialize
        super(10)
      end
    end

    class StaleControllerEpochCodeError < KafkaError
      def initialize
        super(11)
      end
    end

    class OffsetMetadataTooLargeCodeError < KafkaError
      def initialize
        super(12)
      end
    end

    class OffsetsLoadInProgressCodeError < KafkaError
      def initialize
        super(14)
      end
    end

    class ConsumerCoordinatorNotAvailableCodeError < KafkaError
      def initialize
        super(15)
      end
    end

    class NotCoordinatorForConsumerCodeError < KafkaError
      def initialize
        super(16)
      end
    end

    ERRORS = {
      -1 => UnknownError,
       1 => OffsetOutOfRangeError,
       2 => InvalidMessageError,
       3 => UnknownTopicOrPartitionError,
       4 => InvalidMessageSizeError,
       5 => LeaderNotAvailableError,
       6 => NotLeaderForPartitionError,
       7 => RequestTimedOutError,
       8 => BrokerNotAvailableError,
       9 => ReplicaNotAvailableError,
      10 => MessageSizeTooLargeError,
      11 => StaleControllerEpochCodeError,
      12 => OffsetMetadataTooLargeCodeError,
      14 => OffsetsLoadInProgressCodeError,
      15 => ConsumerCoordinatorNotAvailableCodeError,
      16 => NotCoordinatorForConsumerCodeError,
    }

    def self.[](n)
      ERRORS[n]
    end
  end
end
