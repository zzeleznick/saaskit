// Copyright 2023 the Deno authors. All rights reserved. MIT license.
import { AssertionError } from "https://deno.land/std@0.186.0/testing/asserts.ts";

export const kv = await Deno.openKv();

const TINYBIRD_WRITE_KEY = Deno.env.get("TINYBIRD_WRITE_KEY");
const MIN_UNIX_SEC = 1200000000; // 2008-01-10T21:20:00.000Z
const MIN_UNIX_MS = MIN_UNIX_SEC * 1000;

interface InitItem {
  userId: string;
  title: string;
  url: string;
}

export interface Item extends InitItem {
  id: string;
  createdAt: Date;
  score: number;
}

export async function createItem(initItem: InitItem) {
  const id = crypto.randomUUID();
  const itemKey = ["items", id];
  const item: Item = {
    score: 0,
    createdAt: new Date(),
    ...initItem,
    id,
  };

  const res = await kv.atomic()
    .check({ key: itemKey, versionstamp: null })
    .set(itemKey, item)
    .commit();

  if (!res.ok) {
    console.warn(`Failed to add new item with id: ${id}`);
    return
  }
  const {
    userId,
    score,
    createdAt: createdAtRaw,
    title,
    url,
  } = item
  let createdAt: number | Date = createdAtRaw;
  if (typeof createdAt === 'object' && typeof createdAt?.getTime === 'function') {
    createdAt = Math.floor(createdAt.getTime() / 1000);
  } else if (typeof createdAt === 'number') {
    if (createdAt > MIN_UNIX_MS) {
      createdAt = Math.floor(createdAt / 1000) // convert to sec
    }
  }
  // Add event to tinybird
  const resp = await fetch(
    'https://api.us-east.tinybird.co/v0/events?name=submissions',
    {
      method: 'POST',
      body: JSON.stringify({
        userId,
        id,
        createdAt,
        score,
        title,
        url,
      }),
      headers: { Authorization: `Bearer ${TINYBIRD_WRITE_KEY}` }
    }
  )
  // TODO: add retry
  if (!resp.ok) {
    console.error(`Failed to post ${id} to tinybird - status: ${resp.status}`)
  } else {
    const data = await resp.json();
    console.log(`Tinybird response: ${JSON.stringify(data)}`);
  }

  return item;
}

export async function getAllItems(options?: Deno.KvListOptions) {
  const iter = kv.list<Item>({ prefix: ["items"] }, options);
  const items = [];
  for await (const res of iter) items.push(res.value);
  return {
    items,
    cursor: iter.cursor,
  };
}

export async function getItemById(id: string) {
  const res = await kv.get<Item>(["items", id]);
  return res.value;
}

export async function getItemByUser(userId: string, itemId: string) {
  const res = await kv.get<Item>(["items_by_users", userId, itemId]);
  return res.value;
}

interface InitComment {
  userId: string;
  itemId: string;
  text: string;
}

export interface Comment extends InitComment {
  id: string;
  createdAt: Date;
}

export async function createComment(initComment: InitComment) {
  let res = { ok: false };
  while (!res.ok) {
    const id = crypto.randomUUID();
    const commentsByUserKey = ["comments_by_users", initComment.userId, id];
    const commentsByItemKey = ["comments_by_item", initComment.itemId, id];
    const comment: Comment = { ...initComment, id, createdAt: new Date() };

    res = await kv.atomic()
      .check({ key: commentsByUserKey, versionstamp: null })
      .check({ key: commentsByItemKey, versionstamp: null })
      .set(commentsByUserKey, comment)
      .set(commentsByItemKey, comment)
      .commit();

    return comment;
  }
}

export async function getCommentsByItem(
  itemId: string,
  options?: Deno.KvListOptions,
) {
  const iter = kv.list<Comment>({
    prefix: ["comments_by_item", itemId],
  }, options);
  const comments = [];
  for await (const res of iter) comments.push(res.value);
  return comments;
}

interface InitVote {
  userId: string;
  itemId: string;
}

export async function createVote(initVote: InitVote) {
  const itemKey = ["items", initVote.itemId];
  const voteByUserKey = ["votes_by_users", initVote.userId, initVote.itemId];

  let res = { ok: false };
  while (!res.ok) {
    const itemRes = await kv.get<Item>(itemKey);

    if (itemRes.value === null) throw new Error("Item does not exist");
    itemRes.value.score++;

    res = await kv.atomic()
      .check({ key: voteByUserKey, versionstamp: null })
      .check(itemRes)
      .set(itemRes.key, itemRes.value)
      .set(voteByUserKey, undefined)
      .commit();
  }
}

export async function deleteVote(initVote: InitVote) {
  const itemKey = ["items", initVote.itemId];
  const voteByUserKey = ["votes_by_users", initVote.userId, initVote.itemId];

  let res = { ok: false };
  while (!res.ok) {
    const itemRes = await kv.get<Item>(itemKey);
    const voteByUserRes = await kv.get<Item>(voteByUserKey);

    if (itemRes.value === null) throw new Error("Item does not exist");

    if (voteByUserRes.value === null) return;

    itemRes.value.score--;

    res = await kv.atomic()
      .check(itemRes)
      .check(voteByUserRes)
      .set(itemRes.key, itemRes.value)
      .delete(voteByUserKey)
      .commit();
  }
}

export async function getVotedItemIdsByUser(
  userId: string,
  options?: Deno.KvListOptions,
) {
  const iter = kv.list<undefined>({
    prefix: ["votes_by_users", userId],
  }, options);
  const voteItemIds = [];
  for await (const res of iter) voteItemIds.push(res.key.at(-1) as string);
  return voteItemIds;
}

interface InitUser {
  id: string;
  login: string;
  avatarUrl: string;
  stripeCustomerId: string;
  sessionId: string;
}

export interface User extends InitUser {
  isSubscribed: boolean;
}

export async function createUser(user: InitUser) {
  const usersKey = ["users", user.id];
  const usersByLoginKey = ["users_by_login", user.login];
  const usersBySessionKey = ["users_by_session", user.sessionId];
  const usersByStripeCustomerKey = [
    "users_by_stripe_customer",
    user.stripeCustomerId,
  ];

  user = { ...user, isSubscribed: false } as User;

  const res = await kv.atomic()
    .check({ key: usersKey, versionstamp: null })
    .check({ key: usersByLoginKey, versionstamp: null })
    .check({ key: usersBySessionKey, versionstamp: null })
    .check({ key: usersByStripeCustomerKey, versionstamp: null })
    .set(usersKey, user)
    .set(usersByLoginKey, user)
    .set(usersBySessionKey, user)
    .set(usersByStripeCustomerKey, user)
    .commit();

  if (!res.ok) {
    throw res;
  }

  return user;
}

export async function getUserById(id: string) {
  const res = await kv.get<User>(["users", id]);
  return res.value;
}

export async function getUserByLogin(login: string) {
  const res = await kv.get<User>(["users_by_login", login]);
  return res.value;
}

export async function getUserBySessionId(sessionId: string) {
  let res = await kv.get<User>(["users_by_session", sessionId], {
    consistency: "eventual",
  });
  if (!res.value) {
    res = await kv.get<User>(["users_by_session", sessionId]);
  }
  return res.value;
}

export async function getUserByStripeCustomerId(stripeCustomerId: string) {
  const res = await kv.get<User>([
    "users_by_stripe_customer",
    stripeCustomerId,
  ]);
  return res.value;
}

function isEntry<T>(entry: Deno.KvEntryMaybe<T>) {
  return entry.versionstamp !== null;
}

function assertIsEntry<T>(
  entry: Deno.KvEntryMaybe<T>,
): asserts entry is Deno.KvEntry<T> {
  if (!isEntry(entry)) {
    throw new AssertionError(`${entry.key} does not exist`);
  }
}

export async function setUserSubscription(
  user: User,
  isSubscribed: User["isSubscribed"],
) {
  const usersKey = ["users", user.id];
  const usersByLoginKey = ["users_by_login", user.login];
  const usersBySessionKey = ["users_by_session", user.sessionId];
  const usersByStripeCustomerKey = [
    "users_by_stripe_customer",
    user.stripeCustomerId,
  ];

  const [
    userRes,
    userByLoginRes,
    userBySessionRes,
    userByStripeCustomerRes,
  ] = await kv.getMany<User[]>([
    usersKey,
    usersByLoginKey,
    usersBySessionKey,
    usersByStripeCustomerKey,
  ]);

  [
    userRes,
    userByLoginRes,
    userBySessionRes,
    userByStripeCustomerRes,
  ].forEach((res) => assertIsEntry<User>(res));

  user = { ...user, isSubscribed } as User;

  const res = await kv.atomic()
    .check(userRes)
    .check(userByLoginRes)
    .check(userBySessionRes)
    .check(userByStripeCustomerRes)
    .set(usersKey, user)
    .set(usersByLoginKey, user)
    .set(usersBySessionKey, user)
    .set(usersByStripeCustomerKey, user)
    .commit();

  if (!res.ok) {
    throw res;
  }
}

/** This assumes that the previous session has been cleared */
export async function setUserSession(
  user: Omit<User, "isSubscribed">,
  sessionId: string,
) {
  const usersKey = ["users", user.id];
  const usersByLoginKey = ["users_by_login", user.login];
  const usersBySessionKey = ["users_by_session", sessionId];
  const usersByStripeCustomerKey = [
    "users_by_stripe_customer",
    user.stripeCustomerId,
  ];

  const [
    userRes,
    userByLoginRes,
    userByStripeCustomerRes,
  ] = await kv.getMany<User[]>([
    usersKey,
    usersByLoginKey,
    usersByStripeCustomerKey,
  ]);

  [
    userRes,
    userByLoginRes,
    userByStripeCustomerRes,
  ].forEach((res) => assertIsEntry<User>(res));

  user = { ...user, sessionId } as User;

  const res = await kv.atomic()
    .check(userRes)
    .check(userByLoginRes)
    .check({ key: usersBySessionKey, versionstamp: null })
    .check(userByStripeCustomerRes)
    .set(usersKey, user)
    .set(usersByLoginKey, user)
    .set(usersBySessionKey, user)
    .set(usersByStripeCustomerKey, user)
    .commit();

  if (!res.ok) {
    throw res;
  }
}

export async function deleteUser(user: User) {
  const usersKey = ["users", user.id];
  const usersByLoginKey = ["users_by_login", user.login];
  const usersBySessionKey = ["users_by_session", user.sessionId];
  const usersByStripeCustomerKey = [
    "users_by_stripe_customer",
    user.stripeCustomerId,
  ];

  const [
    userRes,
    userByLoginRes,
    userBySessionRes,
    userByStripeCustomerRes,
  ] = await kv.getMany<User[]>([
    usersKey,
    usersByLoginKey,
    usersBySessionKey,
    usersByStripeCustomerKey,
  ]);

  const res = await kv.atomic()
    .check(userRes)
    .check(userByLoginRes)
    .check(userBySessionRes)
    .check(userByStripeCustomerRes)
    .delete(usersKey)
    .delete(usersByLoginKey)
    .delete(usersBySessionKey)
    .delete(usersByStripeCustomerKey)
    .commit();

  if (!res.ok) {
    throw res;
  }
}

export async function deleteUserBySession(sessionId: string) {
  await kv.delete(["users_by_session", sessionId]);
}

export async function getUsersByIds(ids: string[]) {
  const keys = ids.map((id) => ["users", id]);
  // NOTE: limit of 10 for getMany or `TypeError: too many ranges (max 10)`
  const users: User[] = [];
  for (const batch of batchify(keys, 10)) {
    users.push(...(await kv.getMany<User[]>(batch)).map((entry) => entry.value!))
  }
  return users
}

export function* batchify<T>(arr: T[], n = 5): Generator<T[], void> {
  for (let i = 0; i < arr.length; i += n) {
    yield arr.slice(i, i + n);
  }
}

export const computeZeroToOneScore = (score: number, timestampMs: number) => {
  if (score === 0) return 0.5;
  const days = Math.max(0.01, ((new Date()).getTime() - timestampMs) / (1000 * 3600 * 24));
  const val = 0.2 * (1 / (2 ** (Math.sqrt(Math.sqrt(1 + days))))) + 0.8 * (1 + (1 / (2 * Math.log(1 + score)))) / (1 + (1 / (1 * Math.log(1 + score))));
  const bounded = Math.min(1.0, Math.max(0.1, val));
  return bounded.toFixed(18).slice(2, 18);
}