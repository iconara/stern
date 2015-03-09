require 'paint'

class Kafkactl
  def initialize(cluster_size=3, output=$stderr, color=:magenta)
    @cluster_size = cluster_size
    @output = output
    @color = color
    @cmd_path = File.expand_path('../../../bin/kafkactl', __FILE__)
  end

  def start
    run('start')
  end

  def stop
    run('stop')
  end

  def clear
    run('clear')
  end

  private

  def run(subcommand)
    env = {
      'KAFKA_BROKER_COUNT' => @cluster_size.to_s,
    }
    r, w = IO.pipe
    pid = Kernel.spawn(env, @cmd_path, subcommand, err: w, out: w)
    colorize_output(r)
    Process.wait(pid)
    r.close
    w.close
  end

  def colorize_output(io)
    Thread.start do
      begin
        until io.closed?
          if IO.select([io], nil, nil, 1)
            str = io.read_nonblock(1024)
            @output.print(Paint[str, @color])
          end
        end
      rescue Errno::EBADF => e
      end
    end
  end
end

if __FILE__ == $0
  Kafkactl.new(5).send(ARGV[0])
end