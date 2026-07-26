require "bundler/inline"

gemfile do
  source "https://gem.coop"

  gem "pg-ephemeral"
end

require "pg_ephemeral"
require "json"

# Verify that `append_events(jsonb, jsonb)` produces fine-grained predicate
# locks (page/tuple, via index scans) rather than a relation-wide SIREAD lock
# when run inside a SERIALIZABLE transaction.
#
# Method:
#   1. Run EXPLAIN (ANALYZE, BUFFERS, VERBOSE) for the EXISTS query that lives
#      inside append_events, to confirm the chosen plan uses index scans on
#      tags + events.
#   2. Open a SERIALIZABLE transaction, execute append_events with a
#      fail_if_events_match condition (HAS to read tags+events), then query
#      pg_locks for mode = 'SIReadLock' while the transaction is still open.
#      Expectation: locktype IN ('page','tuple') on indexes of `tags` and
#      `events`, NOT locktype = 'relation' covering whole tables.

SCENARIOS = [
  {
    label: "tag with 1000 existing matches (high-volume read)",
    condition_tag: { "name" => "stream_1_000" }
  },
  {
    label: "tag with no existing matches (fresh stream — like dcb-append bench)",
    condition_tag: { "name" => "fresh_stream_does_not_exist" }
  }
]

PAYLOAD = JSON.generate(
  [
    { type: "appended", data: { hi: "there" }, tags: { "name" => "ssi_probe" } }
  ]
)

PgEphemeral.with_server do |server|
  observer = PG.connect(server.url)

  SCENARIOS.each_with_index do |scenario, idx|
    conn = PG.connect(server.url)
    condition = JSON.generate(
      fail_if_events_match: [
        {
          types: %w[type_1 type_2],
          tags: scenario[:condition_tag],
          after: 0
        }
      ]
    )
    tag_json = JSON.generate(scenario[:condition_tag])

    puts "=" * 80
    puts "Scenario #{idx + 1}: #{scenario[:label]}"
    puts "  condition_tag = #{scenario[:condition_tag].inspect}"
    puts "=" * 80

    puts "-- EXPLAIN (ANALYZE, BUFFERS) for the predicate query --"
    explain_sql = <<~SQL
      EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
      SELECT 1
      FROM (
        SELECT t.event_id
        FROM tags t
        JOIN jsonb_each_text('#{tag_json}'::jsonb) AS req(k, v)
          ON t.key = req.k AND t.value = req.v
        GROUP BY t.event_id
        HAVING count(*) = 1
      ) AS cand
      JOIN events e ON e.id = cand.event_id
      WHERE e.position > 0
        AND e.type IN ('type_1','type_2');
    SQL
    conn.exec(explain_sql).each_row { |r| puts "  " + r.first }

    puts
    puts "-- SIREAD locks held while append_events runs in SERIALIZABLE --"
    conn.exec("BEGIN ISOLATION LEVEL SERIALIZABLE")
    writer_pid = conn.exec("SELECT pg_backend_pid() AS pid")[0]["pid"].to_i
    conn.exec_params(
      "SELECT append_events($1::jsonb, $2::jsonb)",
      [PAYLOAD, condition]
    )

    locks = observer.exec_params(<<~SQL, [writer_pid])
      SELECT l.locktype, l.mode, l.page, l.tuple, c.relname, c.relkind
      FROM pg_locks l
      LEFT JOIN pg_class c ON c.oid = l.relation
      WHERE l.pid = $1 AND l.mode = 'SIReadLock'
      ORDER BY c.relname, l.locktype, l.page, l.tuple;
    SQL

    if locks.ntuples.zero?
      puts "  (none — query touched no rows worth predicate-locking)"
    else
      printf("  %-10s %-8s %-8s %-32s %-6s\n",
             "locktype", "page", "tuple", "relation", "kind")
      locks.each do |row|
        printf("  %-10s %-8s %-8s %-32s %-6s\n",
               row["locktype"],
               row["page"] || "-", row["tuple"] || "-",
               row["relname"] || "-", row["relkind"] || "-")
      end
    end

    conn.exec("ROLLBACK")

    by_type = locks.group_by { |r| r["locktype"] }.transform_values(&:count)
    by_rel  = locks.group_by { |r| r["relname"] }.transform_values(&:count)
    puts
    puts "  by locktype: #{by_type.inspect}"
    puts "  by relation: #{by_rel.inspect}"
    puts
    conn.close
  end
ensure
  observer&.close
end
