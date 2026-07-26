require "bundler/inline"

gemfile do
  source "https://gem.coop"

  gem "pg-ephemeral"
  gem "benchmark"
  gem "concurrent-ruby", require: "concurrent"
end

require "pg_ephemeral"
require "benchmark"
require "concurrent"
require "json"
require "securerandom"

writers = 10
events_per_writer = 100
events_per_iter = writers * events_per_writer

iterations = 5
warmup = 2

measure =
  lambda do |label, &block|
    warmup.times { block.call }
    samples = Array.new(iterations) { block.call }

    sorted = samples.sort
    n = sorted.size
    min = sorted.first
    max = sorted.last
    mean = samples.sum / n.to_f
    median = sorted[n / 2]
    p95 = sorted[(n * 0.95).ceil - 1]
    stddev = Math.sqrt(samples.sum { |s| (s - mean)**2 } / n.to_f)
    mean_rate = events_per_iter / mean
    median_rate = events_per_iter / median

    printf(
      "%-30s n=%d  min=%8.2fms  median=%8.2fms  mean=%8.2fms  p95=%8.2fms  max=%8.2fms  stddev=%7.2fms  median=%7.0f ev/s  mean=%7.0f ev/s\n",
      label,
      n,
      min * 1000,
      median * 1000,
      mean * 1000,
      p95 * 1000,
      max * 1000,
      stddev * 1000,
      median_rate,
      mean_rate
    )
  end

PgEphemeral.with_server do |server|
  run_concurrent =
    lambda do |&worker|
      barrier = Concurrent::CyclicBarrier.new(writers + 1)

      threads =
        writers.times.map do |writer_id|
          Thread.new do
            conn = PG.connect(server.url)
            conn.exec("SET client_min_messages TO ERROR")
            barrier.wait
            worker.call(conn, writer_id)
          ensure
            conn&.close
          end
        end

      barrier.wait
      start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      threads.each(&:join)
      Process.clock_gettime(Process::CLOCK_MONOTONIC) - start
    end

  measure["stream-append, 10x100"] do
    run_id = SecureRandom.hex(4)

    run_concurrent.call do |conn, writer_id|
      stream_name = "bench_#{run_id}_#{writer_id}"
      payload =
        JSON.generate(
          events_per_writer.times.map do |seq|
            { type: "appended", data: { writer: writer_id, run: run_id } }
          end
        )

      conn.exec("BEGIN")
      conn.exec_params(
        "SELECT append_events($1::jsonb, $2::text, $3::bigint)",
        [payload, stream_name, 0]
      )
      conn.exec("COMMIT")
    end
  end

  measure["dcb-append,    10x100"] do
    run_id = SecureRandom.hex(4)

    run_concurrent.call do |conn, writer_id|
      tags = { "name" => "bench_#{run_id}_#{writer_id}" }
      condition =
        JSON.generate(
          fail_if_events_match: [{ types: ["appended"], tags: tags }]
        )
      payload =
        JSON.generate(
          events_per_writer.times.map do |seq|
            {
              type: "appended",
              data: {
                writer: writer_id,
                run: run_id
              },
              tags: tags
            }
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
      rescue PG::TRSerializationFailure
        begin
          conn.exec("ROLLBACK")
        rescue StandardError
          nil
        end
        retry if attempts < 100
        raise
      end
    end
  end

  measure["dcb-locked,   10x100"] do
    run_id = SecureRandom.hex(4)

    run_concurrent.call do |conn, writer_id|
      tags = { "name" => "bench_#{run_id}_#{writer_id}" }
      condition =
        JSON.generate(
          fail_if_events_match: [{ types: ["appended"], tags: tags }]
        )
      payload =
        JSON.generate(
          events_per_writer.times.map do |seq|
            {
              type: "appended",
              data: {
                writer: writer_id,
                run: run_id
              },
              tags: tags
            }
          end
        )

      conn.exec("BEGIN")
      conn.exec_params(
        "SELECT append_events_locked($1::jsonb, $2::jsonb)",
        [payload, condition]
      )
      conn.exec("COMMIT")
    end
  end

  measure["stream-append, conflict"] do
    run_id = SecureRandom.hex(4)
    stream_name = "shared_#{run_id}"

    run_concurrent.call do |conn, writer_id|
      payload =
        JSON.generate(
          events_per_writer.times.map do |seq|
            { type: "appended", data: { writer: writer_id, run: run_id } }
          end
        )

      expected = 0
      attempts = 0
      begin
        attempts += 1
        conn.exec("BEGIN")
        conn.exec_params(
          "SELECT append_events($1::jsonb, $2::text, $3::bigint)",
          [payload, stream_name, expected]
        )
        conn.exec("COMMIT")
      rescue PG::UniqueViolation
        begin
          conn.exec("ROLLBACK")
        rescue StandardError
          nil
        end
        result =
          conn.exec_params(
            "SELECT COALESCE(MAX(position), 0) AS pos FROM streams WHERE name = $1",
            [stream_name]
          )
        expected = result[0]["pos"].to_i
        retry if attempts < 11
        raise
      end
    end
  end

  measure["dcb-append,    conflict"] do
    run_id = SecureRandom.hex(4)
    tags = { "name" => "shared_#{run_id}" }

    run_concurrent.call do |conn, writer_id|
      payload =
        JSON.generate(
          events_per_writer.times.map do |seq|
            {
              type: "appended",
              data: {
                writer: writer_id,
                run: run_id
              },
              tags: tags
            }
          end
        )

      after = 0
      attempts = 0
      begin
        attempts += 1
        condition =
          JSON.generate(
            fail_if_events_match: [
              { types: ["appended"], tags: tags, after: after }
            ]
          )
        conn.exec("BEGIN ISOLATION LEVEL SERIALIZABLE")
        conn.exec_params(
          "SELECT append_events($1::jsonb, $2::jsonb)",
          [payload, condition]
        )
        conn.exec("COMMIT")
      rescue PG::RaiseException, PG::TRSerializationFailure
        begin
          conn.exec("ROLLBACK")
        rescue StandardError
          nil
        end
        result = conn.exec_params(<<~SQL, [tags.keys.first, tags.values.first])
          SELECT COALESCE(MAX(e.position), 0) AS pos
          FROM events e
          JOIN tags t ON t.event_id = e.id
          WHERE t.key = $1 AND hashtext(t.value) = hashtext($2) AND t.value = $2
        SQL
        after = result[0]["pos"].to_i
        retry if attempts < 100
        raise
      end
    end
  end

  measure["dcb-locked,   conflict"] do
    run_id = SecureRandom.hex(4)
    tags = { "name" => "shared_#{run_id}" }

    run_concurrent.call do |conn, writer_id|
      payload =
        JSON.generate(
          events_per_writer.times.map do |seq|
            {
              type: "appended",
              data: {
                writer: writer_id,
                run: run_id
              },
              tags: tags
            }
          end
        )

      after = 0
      attempts = 0
      begin
        attempts += 1
        condition =
          JSON.generate(
            fail_if_events_match: [
              { types: ["appended"], tags: tags, after: after }
            ]
          )
        conn.exec("BEGIN")
        conn.exec_params(
          "SELECT append_events_locked($1::jsonb, $2::jsonb)",
          [payload, condition]
        )
        conn.exec("COMMIT")
      rescue PG::RaiseException
        begin
          conn.exec("ROLLBACK")
        rescue StandardError
          nil
        end
        result = conn.exec_params(<<~SQL, [tags.keys.first, tags.values.first])
          SELECT COALESCE(MAX(e.position), 0) AS pos
          FROM events e
          JOIN tags t ON t.event_id = e.id
          WHERE t.key = $1 AND hashtext(t.value) = hashtext($2) AND t.value = $2
        SQL
        after = result[0]["pos"].to_i
        retry if attempts < 11
        raise
      end
    end
  end
end
