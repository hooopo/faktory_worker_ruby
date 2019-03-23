require 'socket'
require 'json'
require 'uri'
require 'digest'
require 'securerandom'
require 'pg'
require 'mini_sql'

module Faktory
  class CommandError < StandardError;end
  class ParseError < StandardError;end

  class Client
    @@random_process_wid = ""

    DEFAULT_TIMEOUT = 5.0

    HASHER = proc do |iter, pwd, salt|
      sha = Digest::SHA256.new
      hashing = pwd + salt
      iter.times do
        hashing = sha.digest(hashing)
      end
      Digest.hexencode(hashing)
    end


    # Called when booting the worker process to signal that this process
    # will consume jobs and send BEAT.
    def self.worker!
      @@random_process_wid = SecureRandom.hex(8)
    end

    attr_accessor :middleware
    attr_reader :worker_id
    attr_reader :db

    # Best practice is to rely on the localhost default for development
    # and configure the environment variables for non-development environments.
    #
    # PGMQ_PROVIDER=MY_FAKTORY_URL
    # MY_PGMQ_URL=postgres://:somepass@my-server.example.com:5432
    #
    # Note above, the URL can contain the password for secure installations.
    def initialize(url: uri_from_env || 'postgres://localhost:5432', debug: true, timeout: DEFAULT_TIMEOUT)
      @debug = debug
      @location = URI(url)
      @timeout = timeout
      @db = PG.connect(ENV['PGMQ_URL'])
      @db.exec 'set search_path to pgmq'
      mini_sql = MiniSql::Connection.get(@db)
      @db.type_map_for_results = mini_sql.type_map

      @db.prepare("create_worker", %Q{
        insert into workers (hostname, pid, v, labels, started_at, last_active_at)
             values ($1, $2, $3, $4, $5, $6)
        on conflict (pid)
                    do update set last_active_at = EXCLUDED.last_active_at,
                                  hostname = EXCLUDED.hostname,
                                  v = EXCLUDED.v,
                                  labels = EXCLUDED.labels,
                                  started_at = EXCLUDED.started_at
          returning *
      })

      open(@timeout)
    end

    def close
      @db
      @db = nil
    end

    # Warning: this clears all job data in Faktory
    def flush
      transaction do
        command "FLUSH"
        ok!
      end
    end

    def push(job)
      transaction do
        command "PUSH", JSON.generate(job)
        ok!
        job["jid"]
      end
    end

    def fetch(*queues)
      job = nil
      transaction do
        command("FETCH", *queues)
        job = result
      end
      JSON.parse(job) if job
    end

    def ack(jid)
      transaction do
        command("ACK", %Q[{"jid":"#{jid}"}])
        ok!
      end
    end

    def fail(jid, ex)
      transaction do
        command("FAIL", JSON.dump({ message: ex.message[0...1000],
                          errtype: ex.class.name,
                          jid: jid,
                          backtrace: ex.backtrace}))
        ok!
      end
    end

    # Sends a heartbeat to the server, in order to prove this
    # worker process is still alive.
    #
    # Return a string signal to process, legal values are "quiet" or "terminate".
    # The quiet signal is informative: the server won't allow this process to FETCH
    # any more jobs anyways.
    def beat
      "OK"
    end

    def info
      transaction do
        command("INFO")
        str = result
        JSON.parse(str) if str
      end
    end

    private

    def debug(line)
      puts line
    end

    def open(timeout = DEFAULT_TIMEOUT)
      worker = @db.exec_prepared("create_worker", [
        Socket.gethostname, 
        $$, 
        '1.0', 
        Faktory.options[:labels] || ["ruby-#{RUBY_VERSION}"], 
        Time.now, 
        Time.now]
      )[0]

      @worker_id = worker['id']
      @db
    end

    def command(*args)
      cmd = args.join(" ")
      @sock.puts(cmd)
      debug "> #{cmd}" if @debug
    end

    def transaction
      retryable = true

      # When using Faktory::Testing, you can get a client which does not actually
      # have an underlying socket.  Now if you disable testing and try to use that
      # client, it will crash without a socket.  This open() handles that case to
      # transparently open a socket.
      open(@timeout) if !@sock

      begin
        yield
      rescue Errno::EPIPE, Errno::ECONNRESET
        if retryable
          retryable = false
          open(@timeout)
          retry
        else
          raise
        end
      end
    end

    # I love pragmatic, simple protocols.  Thanks antirez!
    # https://redis.io/topics/protocol
    def result
      line = @sock.gets
      debug "< #{line}" if @debug
      raise Errno::ECONNRESET, "No response" unless line
      chr = line[0]
      if chr == '+'
        line[1..-1].strip
      elsif chr == '$'
        count = line[1..-1].strip.to_i
        return nil if count == -1
        data = @sock.read(count) if count > 0
        line = @sock.gets # read extra linefeeds
        data
      elsif chr == '-'
        raise CommandError, line[1..-1]
      else
        # this is bad, indicates we need to reset the socket
        # and start fresh
        raise ParseError, line.strip
      end
    end

    def ok!
      resp = result
      raise CommandError, resp if resp != "OK"
      true
    end

    # PGMQ_PROVIDER=MY_PGMQ_URL
    # MY_PGMQ_URL=postgres://:some-pass@some-hostname:5432
    def uri_from_env
      prov = ENV['PGMQ_PROVIDER']
      if prov
        raise(ArgumentError, <<-EOM) if prov.index(":")
  Invalid PGMQ_PROVIDER '#{prov}', it should be the name of the ENV variable that contains the URL
      PGMQ_PROVIDER=MY_PGMQ_URL
      MY_PGMQ_URL=tcp://:some-pass@some-hostname:5432
  EOM
        val = ENV[prov]
        return URI(val) if val
      end

      val = ENV['PGMQ_URL']
      return URI(val) if val
      nil
    end

  end
end

