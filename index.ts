import { S3Client, ListObjectsV2Command, DeleteObjectsCommand, _Object, ListObjectsV2CommandOutput } from "@aws-sdk/client-s3";
import dotenv from "dotenv";

dotenv.config();

const BUCKET = process.env.R2_BUCKET!;
const ACCESS_KEY = process.env.R2_ACCESS_KEY_ID!;
const SECRET_KEY = process.env.R2_SECRET_ACCESS_KEY!;
const ACCOUNT_ID = process.env.R2_ACCOUNT_ID!;
const REGION = "auto";

const client = new S3Client({
  region: REGION,
  endpoint: `https://${ACCOUNT_ID}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId: ACCESS_KEY,
    secretAccessKey: SECRET_KEY,
  },
});

async function listAllObjects(): Promise<string[]> {
  let objects: string[] = [];
  let continuationToken: string | undefined = undefined;

  do {
    const response: ListObjectsV2CommandOutput = await client.send(new ListObjectsV2Command({
      Bucket: BUCKET,
      ContinuationToken: continuationToken,
    }));

    const contents: _Object[] = response.Contents || [];
    objects.push(...contents.map((obj: _Object) => obj.Key!).filter(Boolean));

    continuationToken = response.NextContinuationToken;
  } while (continuationToken);

  return objects;
}

async function deleteAllObjects() {
  const objects = await listAllObjects();

  if (objects.length === 0) {
    console.log("The bucket is already empty!");
    return;
  }

  console.log(`Found ${objects.length} objects. Starting deletion...`);
  let deletedCount = 0;

  while (objects.length > 0) {
    const chunk = objects.splice(0, 1000);
    await client.send(new DeleteObjectsCommand({
      Bucket: BUCKET,
      Delete: {
        Objects: chunk.map((Key: string) => ({ Key })),
      },
    }));
    deletedCount += chunk.length;
    console.log(`Deleted ${chunk.length} objects (total: ${deletedCount})`);
  }

  console.log("Bucket cleaned successfully!");
}

deleteAllObjects().catch(err => {
  console.error("Error cleaning bucket:", err);
  process.exit(1);
});
