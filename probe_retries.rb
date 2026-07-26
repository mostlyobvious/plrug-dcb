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

# Count how many times each writer retries (SerializationFailure) in the
# dcb-append non-conflict scenario. If retries are non-zero, SSI is producing
# false positives among non-overlapping writers; that's where we'd focus.

writers = 10
events_per_writer = 100

PgEphemeral.with_server do |server|
  total_runs = 20
  retry_counter = Concurrent::AtomicFixnum.new(0)
  fail_counter  = Concurrent::AtomicFixnum.new(0)
  ok_counter    = Concurrent::AtomicFixnum.new(0)

  total_runs.times do |run_idx|
    run_id = SecureRandom.hex(4)
    barrier = Concurrent::CyclicBarrier.new(writers + 1)

    threads = writers.times.map do |writer_id|
      Thread.new do
        conn = PG.connect(server.url)
        conn.exec("SET client_min_messages TO ERROR")
        barrier.wait

        tags = { "name" => "bench_#{run_id}_#{writer_id}" }
        condition = JSON.generate(
          fail_if_events_match: [{ types: ["appended"], tags: tags }]
        )
        payload = JSON.generate(
          events_per_writer.times.map do |_seq|
            { type: "appended", data: { writer: writer_id, run: run_id }, tags: tags }
          end
        )

        attempts = 0
        begin
          attempts += 1
          conn.exec("BEGIN ISOLATION LEVEL SERIALIZABLE")
          conn.exec_params(
            "SELECT append_events($1::jsonb, $2::jsonb)",
            [payload, condition]
          )
          conn.exec("COMMIT")
          ok_counter.increment
          retry_counter.increment(attempts - 1)
        rescue PG::TRSerializationFailure
          begin; conn.exec("ROLLBACK"); rescue; end
          fail_counter.increment if attempts >= 100
          retry if attempts < 100
        ensure
          conn.close
        end
      end
    end

    barrier.wait
    threads.each(&:join)
  end

  puts "Across #{total_runs} runs × #{writers} writers:"
  puts "  successful writers: #{ok_counter.value}"
  puts "  total retries:      #{retry_counter.value}"
  puts "  exhausted retries:  #{fail_counter.value}"
  puts "  retries / writer:   %.2f" % (retry_counter.value.to_f / [ok_counter.value, 1].max)
end
