const { Client } = require("pg");
const updates = [
  { id: "btc-updown-5m-1779526200", side: "UP" },
  { id: "btc-updown-5m-1779526500", side: "DOWN" },
  { id: "btc-updown-5m-1779526800", side: "DOWN" },
  { id: "btc-updown-5m-1780229400", side: "UP" }
];
(async () => {
  const client = new Client({ connectionString: process.env.DATABASE_URL });
  await client.connect();
  const now = Date.now();
  await client.query("BEGIN");
  try {
    for (const item of updates) {
      await client.query(
        `
          UPDATE rounds
          SET settled_side = $2,
              settlement_price = $3,
              settlement_ts = COALESCE(settlement_ts, $4),
              settlement_source = COALESCE(settlement_source, 'manual'),
              status = 'Settled',
              accepting_orders = FALSE,
              redeem_scheduled_at = COALESCE(redeem_scheduled_at, $4),
              manual_reason = COALESCE(manual_reason, 'Manual settlement assigned during historical cleanup')
          WHERE id = $1
        `,
        [item.id, item.side, item.side === 'UP' ? 1 : 0, now]
      );
    }
    await client.query("COMMIT");
    const result = await client.query(`
      SELECT id, settled_side, settlement_price, status, redeem_scheduled_at
      FROM rounds
      WHERE id = ANY($1::text[])
      ORDER BY id ASC
    `, [updates.map((item) => item.id)]);
    console.log(JSON.stringify(result.rows, null, 2));
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    await client.end();
  }
})().catch(async (error) => {
  console.error(error);
  process.exit(1);
});
