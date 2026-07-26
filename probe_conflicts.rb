require "bundler/inline"

gemfile do
  source "https://gem.coop"

  gem "pg-ephemeral"
  gem "concurrent-ruby", require: "concurrent"
end

require "pg_ephemeral"
require "concurrent"
require "json"
require "securerandom"

# Capture SIREAD locks across all writer connections AND list the
# write/read conflict edges PG SSI is actually tracking, mid-flight.

writers = 10
events_per_writer = 100

PgEphemeral.with_server do |server|
  obs = PG.connect(server.url)

  run_id = SecureRandom.hex(4)
  barrier = Concurrent::CyclicBarrier.new(writers + 1)
  go      = Concurrent::Event.new
  hold    = Concurrent::Event.new

  threads = writers.times.map do |writer_id|
    Thread.new do
      conn = PG.connect(server.url)
      conn.exec("SET client_min_messages TO ERROR")
      tags = { "name" => "bench_#{run_id}_#{writer_id}" }
      condition = JSON.generate(
        fail_if_events_match: [{ types: ["appended"], tags: tags }]
      )
      payload = JSON.generate(
        events_per_writer.times.map { |_| { type: "appended", data: { w: writer_id }, tags: tags } }
      )
      barrier.wait
      go.wait

      conn.exec("BEGIN ISOLATION LEVEL SERIALIZABLE")
      conn.exec_params("SELECT append_events($1::jsonb, $2::jsonb)", [payload, condition])
      # do NOT commit — wait for observer to snapshot
      hold.wait
      begin; conn.exec("COMMIT"); rescue; conn.exec("ROLLBACK"); end
      conn.close
    end
  end

  barrier.wait
  go.set
  sleep 0.25  # give writers time to run append_events

  locks = obs.exec(<<~SQL)
    SELECT
      pid, locktype, mode, page, tuple, c.relname, c.relkind
    FROM pg_locks l
    LEFT JOIN pg_class c ON c.oid = l.relation
    WHERE l.mode = 'SIReadLock'
    ORDER BY pid, c.relname, l.locktype, l.page;
  SQL

  rels = locks.group_by { |r| r["relname"] }
  puts "SIREAD locks while #{writers} writers are mid-transaction:"
  rels.each do |rel, rows|
    puts "  #{rel}: #{rows.size} locks across #{rows.map { |r| r['pid'] }.uniq.size} pids"
    by_lt = rows.group_by { |r| r["locktype"] }.transform_values(&:size)
    puts "    locktypes: #{by_lt.inspect}"
    pages = rows.map { |r| r["page"] }.compact.uniq.sort_by(&:to_i)
    puts "    distinct pages: #{pages.size}  (sample: #{pages.first(8).inspect}...)"
  end

  hold.set
  threads.each(&:join)
  obs.close
end
