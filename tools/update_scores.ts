// Copyright 2023 the Deno authors. All rights reserved. MIT license.
import { kv, Item, computeZeroToOneScore } from "@/utils/db.ts";

export async function updateScores() {
    const iter = kv.list<Item>({ prefix: ["items"] });
    const items = [];
    for await (const res of iter) {
        const { score, createdAt } = res.value;
        const timestampMs = (new Date(createdAt)).getTime();
        const zscore = computeZeroToOneScore(score, timestampMs);
        items.push({ [res.key.toString()]: zscore });
    }
    items.sort((a, b) => 1 - 2 * Number(a[Object.keys(a)?.[0]] < b[Object.keys(b)?.[0]]))
    console.log(`KV: ${JSON.stringify(items, null, 2)}`)
}

if (import.meta.main) {
    await updateScores();
    await kv.close();
}
