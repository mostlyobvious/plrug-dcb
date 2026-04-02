require "bundler/inline"

gemfile do
  source "https://gem.coop"

  gem "pg-ephemeral"
  gem "benchmark"
end

require "pg_ephemeral"
require "benchmark"
require "json"

variants = %w[1 10 100 1_000 10_000 100_000]
event_types = (1..10).map { |i| "type_#{i}" }

iterations = 50
warmup = 5

measure =
  lambda do |label, &block|
    warmup.times { block.call }
    samples = Array.new(iterations) { Benchmark.realtime { block.call } }

    sorted = samples.sort
    n = sorted.size
    min = sorted.first
    max = sorted.last
    mean = samples.sum / n.to_f
    median = sorted[n / 2]
    p95 = sorted[(n * 0.95).ceil - 1]
    stddev = Math.sqrt(samples.sum { |s| (s - mean)**2 } / n.to_f)

    printf(
      "%-26s n=%d  min=%8.3fms  median=%8.3fms  mean=%8.3fms  p95=%8.3fms  max=%8.3fms  stddev=%7.3fms\n",
      label,
      n,
      min * 1000,
      median * 1000,
      mean * 1000,
      p95 * 1000,
      max * 1000,
      stddev * 1000
    )
  end

PgEphemeral.with_connection do |connection|
  read_stream =
    lambda do |stream_name|
      connection.exec_params(
        "SELECT * FROM read_stream($1::text)",
        [stream_name]
      )
    end

  variants.each do |number|
    stream_name = "stream_#{number}"

    measure["stream, #{stream_name}"] { read_stream[stream_name] }
  end

  read_tags =
    lambda do |tags, types|
      text_array = PG::TextEncoder::Array.new

      connection.exec_params(
        "SELECT * FROM read_tags($1::jsonb, $2::text[])",
        [JSON.generate(tags), text_array.encode(types)]
      )
    end

  variants.each do |number|
    tag_value = "stream_#{number}"

    measure.call("tags,   #{tag_value}") do
      read_tags[{ "name" => tag_value }, event_types]
    end
  end
end
