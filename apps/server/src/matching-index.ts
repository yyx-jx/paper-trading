import { serverConfig } from "./config";
import { createMatchingServiceApp } from "./services/matching/app";

let shuttingDown = false;
let matchingRuntime: Awaited<ReturnType<typeof createMatchingServiceApp>> | undefined;

const shutdown = async () => {
  if (shuttingDown) {
    return;
  }
  shuttingDown = true;
  await matchingRuntime?.close().catch(() => undefined);
};

process.on("SIGINT", () => {
  void shutdown();
});
process.on("SIGTERM", () => {
  void shutdown();
});

async function bootstrap() {
  matchingRuntime = await createMatchingServiceApp({
    databaseUrl: serverConfig.databaseUrl,
    redisUrl: serverConfig.redisUrl,
    redisSnapshotSeconds: serverConfig.snapshotRetentionSeconds
  });

  await matchingRuntime.app.listen({
    host: "0.0.0.0",
    port: serverConfig.matchingServicePort
  });
}

bootstrap().catch(async (error) => {
  console.error(error);
  await shutdown();
  process.exit(1);
});
