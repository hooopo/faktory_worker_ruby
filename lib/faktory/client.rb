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
    @worker_id = 0

    DEFAULT_TIMEOUT = 5.0

    # Called when booting the worker process to signal that this process
    # will consume jobs and send BEAT.
    def self.worker!
      @worker_id = Faktory.server {|c| c.create_worker }
    end

    class << self
      attr_reader :worker_id
    end

    attr_accessor :middleware
    attr_reader :db

    # Best practice is to rely on the localhost default for development
    # and configure the environment variables for non-development environments.
    #
    # PGMQ_PROVIDER=MY_FAKTORY_URL
    # MY_PGMQ_URL=postgres://:somepass@my-server.example.com:5432
    #
    # Note above, the URL can contain the password for secure installations.
    def initialize(url: uri_from_env || 'postgres://localhost:5432', debug: true)
      @debug = debug
      @location = URI(url)
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

      @db.prepare("push_job", %Q{
        insert into jobs (jobtype, queue, args) values ($1, $2, $3) returning *
      })

      @db.prepare("beat", %Q{
        update workers set last_active_at = $1 where id = $2
      })

      @db.prepare("fetch_jobs", %Q{
        UPDATE ONLY jobs 
           SET state = 'working', enqueued_at = now()
         WHERE jid IN (
                      SELECT jid
                        FROM  ONLY jobs
                       WHERE state = 'scheduled' AND at <= now() AND queue = ANY ($1) AND retry > 0
                    ORDER BY at DESC, priority DESC
                             FOR UPDATE SKIP LOCKED
                       LIMIT $2
          )
        RETURNING *
      })

      @db.prepare("complete_jobs", %Q{
        UPDATE ONLY jobs
           SET state = 'done',
               completed_at = now(),
               worker_id = $1
         WHERE jid = $2
      })

      @db.prepare("reset_jobs", %Q{
        UPDATE ONLY jobs 
           SET state = $2,
               retry = retry - 1,
               worker_id = $3,
               failure = $4
         WHERE jid = $1
      })
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
      new_job = db.exec_prepared("push_job", job.slice('jobtype', 'queue', 'args').values)
      debug "> #{new_job[0]}" if @debug
      new_job[0]['jid'].to_s
    end

    def fetch(*queues)
      debug "> fetch #{queues.join('/')}" if @debug
      job = nil
      job = db.exec_prepared('fetch_jobs', [to_pg_array(queues), 1])[0]
      job if not job.empty?
    end

    def ack(jid)
      db.exec_prepared("complete_jobs", [Client.worker_id, jid])
    end

    def fail(job, ex)
      if job['retry'] == 1
        state = 'dead'
      else
        state = 'scheduled'
      end
      db.exec_prepared("reset_jobs", [job['jid'], state, Client.worker_id, JSON.dump({
          errtype: ex.class.name,
          message: ex.message[0...1000],
          backtrace: ex.backtrace[0...1000]
        })]
      )
    end

    # Sends a heartbeat to the server, in order to prove this
    # worker process is still alive.
    #
    # Return a string signal to process, legal values are "quiet" or "terminate".
    # The quiet signal is informative: the server won't allow this process to FETCH
    # any more jobs anyways.
    def beat
      db.exec_prepared("beat", [Time.now, Client.worker_id])
      debug "> beat" if @debug
      "OK"
    end

    def info
      transaction do
        command("INFO")
        str = result
        JSON.parse(str) if str
      end
    end

    def create_worker
      labels = to_pg_array(Faktory.options[:labels] || ["ruby-#{RUBY_VERSION}"])
      worker = @db.exec_prepared("create_worker", [
        Socket.gethostname, 
        $$, 
        '1.0', 
        labels, 
        Time.now, 
        Time.now]
      )[0]

      worker['id']
    end

    private

    def debug(line)
      puts line
    end

    def to_pg_array(array)
      PG::TextEncoder::Array.new.encode(array).force_encoding('utf-8')
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

