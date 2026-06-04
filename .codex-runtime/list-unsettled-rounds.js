const { Client } = require("pg");
(async () => {
  const client = new Client({ connectionString: process.env.DATABASE_URL });
  await client.connect();
  const result = await client.query(`
    SELECT r.id AS round_id, r.end_at, COUNT(p.id)::int AS open_position_count
    FROM rounds r
    JOIN positions p ON p.round_id = r.id
    WHERE p.status = 'open'
      AND r.end_at < 1780243200000
      AND r.settled_side IS NULL
    GROUP BY r.id, r.end_at
    ORDER BY r.end_at ASC, r.id ASC
  `);
  console.log(JSON.stringify(result.rows, null, 2));
  await client.end();
})().catch(async (error) => {
  console.error(error);
  process.exit(1);
});
