import {
  ChannelType,
  Client,
  EmbedBuilder,
  GatewayIntentBits,
  PermissionFlagsBits,
  REST,
  Routes,
  SlashCommandBuilder,
} from "discord.js";
import { MongoClient } from "mongodb";
import cron from "node-cron";
import "dotenv/config";

process.on("uncaughtException", (error) =>
  console.error("Uncaught Exception:", error)
);
process.on("unhandledRejection", (reason) =>
  console.error("Unhandled Rejection:", reason)
);

let isShuttingDown = false;

async function shutdown(signal) {
  if (isShuttingDown) {
    return;
  }

  isShuttingDown = true;
  console.warn(`Received ${signal}. Shutting down NutBot...`);

  try {
    await client.destroy();
  } catch (error) {
    console.error("Failed to destroy Discord client cleanly:", error);
  }

  try {
    await dbClient.close();
  } catch (error) {
    console.error("Failed to close MongoDB cleanly:", error);
  }

  process.exit(0);
}

process.on("SIGINT", () => {
  shutdown("SIGINT").catch((error) =>
    console.error("Shutdown error after SIGINT:", error)
  );
});

process.on("SIGTERM", () => {
  shutdown("SIGTERM").catch((error) =>
    console.error("Shutdown error after SIGTERM:", error)
  );
});

const TOKEN = process.env.TOKEN;
const MONGO_URI = process.env.MONGO_URI;
const CLIENT_ID = process.env.CLIENT_ID;
const LEGACY_GUILD_ID = process.env.GUILD_ID;
const SETUP_PASSWORD = process.env.SETUP_PASSWORD || "trixxxi3";
const BOT_TIMEZONE = process.env.BOT_TIMEZONE || "America/New_York";

const COUNT_CHANNEL_NAME = "nut-counter";
const SEASON_LENGTH_DAYS = 90;
const TOP_LIMIT = 5;
const MILESTONE_INTERVAL = 25;
const DAY_IN_MS = 24 * 60 * 60 * 1000;
const HOUR_IN_MS = 60 * 60 * 1000;
const RECONCILE_MESSAGE_LIMIT = 100;
const CARE_REMINDERS = [
  "Make sure to drink some water and use lotion!",
  "Hydrate. Stretch. Maybe take a lap before the next one.",
  "That is a busy hour. Water first, decisions second.",
];

if (!TOKEN || !MONGO_URI || !CLIENT_ID) {
  throw new Error("TOKEN, MONGO_URI, and CLIENT_ID are required.");
}

const dbClient = new MongoClient(MONGO_URI);
const rest = new REST({ version: "10" }).setToken(TOKEN);

let userCollection;
let guildCollection;
let seasonCollection;
let metaCollection;

dbClient.on("close", () => {
  console.warn("MongoDB connection closed.");
});

dbClient.on("error", (error) => {
  console.error("MongoDB error:", error);
});

const commands = [
  new SlashCommandBuilder()
    .setName("setup")
    .setDescription("Activate NutBot for this server")
    .setDMPermission(false)
    .setDefaultMemberPermissions(PermissionFlagsBits.Administrator)
    .addStringOption((option) =>
      option
        .setName("password")
        .setDescription("Activation password")
        .setRequired(true)
    ),
  new SlashCommandBuilder()
    .setName("adminstatus")
    .setDescription("Show NutBot runtime status for this server")
    .setDMPermission(false)
    .setDefaultMemberPermissions(PermissionFlagsBits.Administrator),
  new SlashCommandBuilder()
    .setName("nut")
    .setDescription("See your nut totals")
    .setDMPermission(false),
  new SlashCommandBuilder()
    .setName("count")
    .setDescription("See the current count for this server")
    .setDMPermission(false),
  new SlashCommandBuilder()
    .setName("leaderboard")
    .setDescription("See the lifetime leaderboard or a specific season")
    .setDMPermission(false)
    .addIntegerOption((option) =>
      option
        .setName("season")
        .setDescription("Optional season number for legacy results")
        .setMinValue(1)
    ),
  new SlashCommandBuilder()
    .setName("weekly")
    .setDescription("See the weekly leaderboard")
    .setDMPermission(false),
  new SlashCommandBuilder()
    .setName("season")
    .setDescription("See the current season or a legacy season")
    .setDMPermission(false)
    .addIntegerOption((option) =>
      option
        .setName("season")
        .setDescription("Optional season number for legacy results")
        .setMinValue(1)
    ),
  new SlashCommandBuilder()
    .setName("stats")
    .setDescription("See another user's stats")
    .setDMPermission(false)
    .addUserOption((option) =>
      option
        .setName("user")
        .setDescription("User to look up")
        .setRequired(true)
    )
    .addIntegerOption((option) =>
      option
        .setName("season")
        .setDescription("Optional season number for legacy results")
        .setMinValue(1)
    ),
  new SlashCommandBuilder()
    .setName("mystats")
    .setDescription("See your own stats")
    .setDMPermission(false)
    .addIntegerOption((option) =>
      option
        .setName("season")
        .setDescription("Optional season number for legacy results")
        .setMinValue(1)
    ),
  new SlashCommandBuilder()
    .setName("compare")
    .setDescription("Compare two users")
    .setDMPermission(false)
    .addUserOption((option) =>
      option
        .setName("user1")
        .setDescription("First user")
        .setRequired(true)
    )
    .addUserOption((option) =>
      option
        .setName("user2")
        .setDescription("Second user")
        .setRequired(true)
    )
    .addIntegerOption((option) =>
      option
        .setName("season")
        .setDescription("Optional season number for legacy results")
        .setMinValue(1)
    ),
  new SlashCommandBuilder()
    .setName("help")
    .setDescription("Show all NutBot commands")
    .setDMPermission(false),
].map((command) => command.toJSON());

const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent,
  ],
});

function formatNumber(value) {
  return Number(value || 0).toLocaleString("en-US");
}

function formatDate(value) {
  return new Intl.DateTimeFormat("en-US", {
    dateStyle: "long",
    timeZone: BOT_TIMEZONE,
  }).format(new Date(value));
}

function addDays(date, days) {
  return new Date(new Date(date).getTime() + days * DAY_IN_MS);
}

function subtractHours(date, hours) {
  return new Date(new Date(date).getTime() - hours * HOUR_IN_MS);
}

function getRandomCareReminder() {
  return CARE_REMINDERS[Math.floor(Math.random() * CARE_REMINDERS.length)];
}

function buildSeasonWindow(currentSeasonNumber, currentSeasonStart, targetSeason) {
  const seasonOffset = currentSeasonNumber - targetSeason;
  const startAt = addDays(currentSeasonStart, -seasonOffset * SEASON_LENGTH_DAYS);
  const endAt = addDays(startAt, SEASON_LENGTH_DAYS - 1);

  return {
    startAt,
    endAt,
    isCurrent: seasonOffset === 0,
  };
}

function buildWrongCountMessage(expectedNumber) {
  return `Hmm, that's not right. The next number should be **${formatNumber(
    expectedNumber
  )}**.`;
}

function buildMilestoneEmbed(userId, stats) {
  const elapsed = new Date() - new Date(stats.firstNutAt);
  const days = Math.floor(elapsed / DAY_IN_MS);
  const hours = Math.floor((elapsed / (60 * 60 * 1000)) % 24);
  const minutes = Math.floor((elapsed / (60 * 1000)) % 60);

  return new EmbedBuilder()
    .setTitle("Milestone Hit")
    .setDescription(
      `<@${userId}> just reached **${formatNumber(stats.nuts)}** lifetime nuts.`
    )
    .addFields({
      name: "Time Since First Recorded Nut",
      value: `${days}d ${hours}h ${minutes}m`,
    })
    .setColor("Gold")
    .setTimestamp();
}

function buildDailyCountEmbed(guildState) {
  return new EmbedBuilder()
    .setTitle("Nightly Count Update")
    .setDescription(
      `As of **${formatDate(new Date())}**, the current count is **${formatNumber(
        guildState.lastNumber
      )}**.`
    )
    .addFields({
      name: "Current Season",
      value: `Season ${guildState.seasonNumber}`,
      inline: true,
    })
    .setColor("Blue")
    .setTimestamp();
}

function buildSeasonRolloverEmbed(guildState) {
  const seasonWindow = buildSeasonWindow(
    guildState.seasonNumber,
    guildState.seasonStartedAt,
    guildState.seasonNumber
  );

  return new EmbedBuilder()
    .setTitle(`Season ${guildState.seasonNumber} Started`)
    .setDescription(
      `A new 90-day season has started for this server. The global count keeps going, but the season leaderboard resets. Legacy season results are still available with the slash commands.`
    )
    .addFields({
      name: "Season Window",
      value: `${formatDate(seasonWindow.startAt)} to ${formatDate(
        seasonWindow.endAt
      )}`,
    })
    .setColor("Green")
    .setTimestamp();
}

function buildUnauthorizedHelpEmbed() {
  return new EmbedBuilder()
    .setTitle("NutBot Setup")
    .setDescription(
      `This server is not activated yet. An admin needs to run \`/setup\` with the password before the other commands will work.`
    )
    .addFields({
      name: "Required Channel",
      value: `Create a text channel named **#${COUNT_CHANNEL_NAME}** for counting.`,
    })
    .setColor("Orange");
}

async function connectDB() {
  await dbClient.connect();

  const db = dbClient.db("nutbot");
  userCollection = db.collection("nuts");
  guildCollection = db.collection("guilds");
  seasonCollection = db.collection("seasonStats");
  metaCollection = db.collection("meta");

  await migrateLegacyData();

  await Promise.all([
    userCollection.createIndex({ guildId: 1, userId: 1 }, { unique: true }),
    userCollection.createIndex({ guildId: 1, nuts: -1 }),
    userCollection.createIndex({ guildId: 1, weeklyNuts: -1 }),
    seasonCollection.createIndex(
      { guildId: 1, seasonNumber: 1, userId: 1 },
      { unique: true }
    ),
    seasonCollection.createIndex({ guildId: 1, seasonNumber: 1, nuts: -1 }),
  ]);

  console.log("Connected to MongoDB");
}

async function migrateLegacyData() {
  if (!LEGACY_GUILD_ID) {
    return;
  }

  const now = new Date();
  const oldMeta = await metaCollection.findOne({ _id: "counter" });
  const existingGuild = await guildCollection.findOne({ _id: LEGACY_GUILD_ID });

  if (!existingGuild) {
    await guildCollection.insertOne({
      _id: LEGACY_GUILD_ID,
      authorized: true,
      channelName: COUNT_CHANNEL_NAME,
      lastNumber: oldMeta?.lastNumber || 0,
      seasonNumber: 1,
      seasonStartedAt: now,
      createdAt: now,
      activatedAt: now,
      activatedBy: "legacy-migration",
      updatedAt: now,
    });
  } else {
    const legacyLastNumber =
      typeof existingGuild.lastNumber === "number" && existingGuild.lastNumber > 0
        ? existingGuild.lastNumber
        : oldMeta?.lastNumber || 0;

    await guildCollection.updateOne(
      { _id: LEGACY_GUILD_ID },
      {
        $set: {
          authorized: true,
          channelName: existingGuild.channelName || COUNT_CHANNEL_NAME,
          lastNumber: legacyLastNumber,
          seasonNumber: existingGuild.seasonNumber || 1,
          seasonStartedAt: existingGuild.seasonStartedAt || now,
          updatedAt: now,
        },
      }
    );
  }

  const migration = await userCollection.updateMany(
    { guildId: { $exists: false } },
    { $set: { guildId: LEGACY_GUILD_ID, updatedAt: now } }
  );

  if (migration.modifiedCount > 0) {
    console.log(`Migrated ${migration.modifiedCount} user records to ${LEGACY_GUILD_ID}`);
  }
}

async function ensureGuildRecord(guildId) {
  let guildState = await guildCollection.findOne({ _id: guildId });

  if (guildState) {
    return guildState;
  }

  const now = new Date();
  const isLegacyGuild = Boolean(LEGACY_GUILD_ID) && guildId === LEGACY_GUILD_ID;

  guildState = {
    _id: guildId,
    authorized: isLegacyGuild,
    channelName: COUNT_CHANNEL_NAME,
    lastNumber: 0,
    seasonNumber: 1,
    seasonStartedAt: now,
    createdAt: now,
    activatedAt: isLegacyGuild ? now : null,
    activatedBy: isLegacyGuild ? "legacy-default" : null,
    updatedAt: now,
  };

  await guildCollection.insertOne(guildState);
  return guildState;
}

async function authorizeGuild(guildId, activatedBy) {
  const now = new Date();

  await guildCollection.updateOne(
    { _id: guildId },
    {
      $set: {
        authorized: true,
        channelName: COUNT_CHANNEL_NAME,
        seasonNumber: 1,
        seasonStartedAt: now,
        activatedAt: now,
        activatedBy,
        updatedAt: now,
      },
      $setOnInsert: {
        lastNumber: 0,
        createdAt: now,
      },
    },
    { upsert: true }
  );

  return guildCollection.findOne({ _id: guildId });
}

async function refreshGuildSeason(guildId) {
  const guildState = await ensureGuildRecord(guildId);
  let seasonNumber = guildState.seasonNumber || 1;
  let seasonStartedAt = guildState.seasonStartedAt || new Date();
  let rolledSeasons = 0;
  const now = new Date();

  while (now >= addDays(seasonStartedAt, SEASON_LENGTH_DAYS)) {
    seasonStartedAt = addDays(seasonStartedAt, SEASON_LENGTH_DAYS);
    seasonNumber += 1;
    rolledSeasons += 1;
  }

  if (rolledSeasons > 0) {
    await guildCollection.updateOne(
      { _id: guildId },
      {
        $set: {
          seasonNumber,
          seasonStartedAt,
          updatedAt: now,
        },
      }
    );
  }

  return {
    ...guildState,
    seasonNumber,
    seasonStartedAt,
    rolledSeasons,
  };
}

async function getGuildState(guildId) {
  return refreshGuildSeason(guildId);
}

async function getUser(guildId, userId) {
  return userCollection.findOne({ guildId, userId });
}

async function getSeasonUser(guildId, userId, seasonNumber) {
  return seasonCollection.findOne({ guildId, userId, seasonNumber });
}

async function claimNextCount(guildId, expectedLastNumber) {
  const nextNumber = expectedLastNumber + 1;
  const result = await guildCollection.updateOne(
    {
      _id: guildId,
      lastNumber: expectedLastNumber,
    },
    {
      $set: {
        lastNumber: nextNumber,
        updatedAt: new Date(),
      },
    }
  );

  return {
    claimed: result.modifiedCount === 1,
    nextNumber,
  };
}

async function recordRecoveredNut(guildId, userId, seasonNumber, occurredAt) {
  const currentUser = await getUser(guildId, userId);
  const now = new Date();

  if (!currentUser) {
    await userCollection.insertOne({
      guildId,
      userId,
      nuts: 1,
      weeklyNuts: 1,
      firstNutAt: occurredAt,
      recentNutTimestamps: [],
      updatedAt: now,
    });
  } else {
    await userCollection.updateOne(
      { guildId, userId },
      {
        $set: { updatedAt: now },
        $inc: { nuts: 1, weeklyNuts: 1 },
      }
    );
  }

  await seasonCollection.updateOne(
    { guildId, userId, seasonNumber },
    {
      $inc: { nuts: 1 },
      $set: { updatedAt: now },
      $setOnInsert: { firstNutAt: occurredAt },
    },
    { upsert: true }
  );
}

async function reconcileGuildCountFromChannel(guild, currentGuildState) {
  const guildState = currentGuildState || (await getGuildState(guild.id));
  const channel = await findCountChannel(guild);

  if (!channel || !guildState.authorized) {
    return guildState;
  }

  const messages = await channel.messages
    .fetch({ limit: RECONCILE_MESSAGE_LIMIT })
    .catch((error) => {
      console.error(
        `Failed to fetch recent messages for reconciliation in guild ${guild.id}:`,
        error
      );
      return null;
    });

  if (!messages || messages.size === 0) {
    return guildState;
  }

  let resolvedLastNumber = guildState.lastNumber || 0;
  const recoveredMessages = [];
  const orderedMessages = [...messages.values()].sort(
    (left, right) => left.createdTimestamp - right.createdTimestamp
  );

  for (const message of orderedMessages) {
    if (message.author.bot) {
      continue;
    }

    const content = message.content.trim();
    if (content !== String(resolvedLastNumber + 1)) {
      continue;
    }

    resolvedLastNumber += 1;

    if (resolvedLastNumber > (guildState.lastNumber || 0)) {
      recoveredMessages.push(message);
    }
  }

  if (resolvedLastNumber === (guildState.lastNumber || 0)) {
    return guildState;
  }

  console.warn(
    `Reconciling guild ${guild.id} from ${guildState.lastNumber || 0} to ${resolvedLastNumber} using recent channel history.`
  );

  for (const message of recoveredMessages) {
    await recordRecoveredNut(
      guild.id,
      message.author.id,
      guildState.seasonNumber,
      new Date(message.createdTimestamp)
    );
  }

  await guildCollection.updateOne(
    { _id: guild.id },
    {
      $set: {
        lastNumber: resolvedLastNumber,
        updatedAt: new Date(),
      },
    }
  );

  return {
    ...guildState,
    lastNumber: resolvedLastNumber,
  };
}

async function reconcileAllGuildsFromHistory() {
  const guilds = await client.guilds.fetch();

  for (const [guildId] of guilds) {
    const guild = await client.guilds.fetch(guildId).catch(() => null);

    if (!guild) {
      continue;
    }

    try {
      const guildState = await getGuildState(guild.id);
      await reconcileGuildCountFromChannel(guild, guildState);
    } catch (error) {
      console.error(`Failed to reconcile guild ${guild.id}:`, error);
    }
  }
}

async function addNut(guildId, userId, seasonNumber) {
  const now = new Date();
  const oneHourAgo = subtractHours(now, 1);
  const currentUser = await getUser(guildId, userId);

  if (!currentUser) {
    await userCollection.insertOne({
      guildId,
      userId,
      nuts: 1,
      weeklyNuts: 1,
      firstNutAt: now,
      recentNutTimestamps: [now],
      updatedAt: now,
    });

    await seasonCollection.updateOne(
      { guildId, userId, seasonNumber },
      {
        $inc: { nuts: 1 },
        $set: { updatedAt: now },
        $setOnInsert: { firstNutAt: now },
      },
      { upsert: true }
    );

    return {
      nuts: 1,
      weeklyNuts: 1,
      firstNutAt: now,
      isMilestone: false,
      recentHourCount: 1,
      shouldSendCareReminder: false,
    };
  }

  const nuts = (currentUser.nuts || 0) + 1;
  const weeklyNuts = (currentUser.weeklyNuts || 0) + 1;
  const recentNutTimestamps = Array.isArray(currentUser.recentNutTimestamps)
    ? currentUser.recentNutTimestamps
        .map((value) => new Date(value))
        .filter((value) => value >= oneHourAgo)
    : [];

  recentNutTimestamps.push(now);

  const shouldSendCareReminder =
    recentNutTimestamps.length >= 4 &&
    (!currentUser.lastCareReminderAt ||
      new Date(currentUser.lastCareReminderAt) < oneHourAgo);

  const updateFields = {
    nuts,
    weeklyNuts,
    recentNutTimestamps,
    updatedAt: now,
  };

  if (shouldSendCareReminder) {
    updateFields.lastCareReminderAt = now;
  }

  await userCollection.updateOne(
    { guildId, userId },
    {
      $set: updateFields,
    }
  );

  await seasonCollection.updateOne(
    { guildId, userId, seasonNumber },
    {
      $inc: { nuts: 1 },
      $set: { updatedAt: now },
      $setOnInsert: { firstNutAt: now },
    },
    { upsert: true }
  );

  return {
    nuts,
    weeklyNuts,
    firstNutAt: currentUser.firstNutAt,
    isMilestone: nuts % MILESTONE_INTERVAL === 0,
    recentHourCount: recentNutTimestamps.length,
    shouldSendCareReminder,
  };
}

async function getLifetimeLeaderboard(guildId) {
  return userCollection.find({ guildId }).sort({ nuts: -1 }).toArray();
}

async function getWeeklyLeaderboard(guildId) {
  return userCollection.find({ guildId }).sort({ weeklyNuts: -1 }).toArray();
}

async function getSeasonLeaderboard(guildId, seasonNumber) {
  return seasonCollection
    .find({ guildId, seasonNumber })
    .sort({ nuts: -1 })
    .toArray();
}

async function getSeasonTotal(guildId, seasonNumber) {
  const [result] = await seasonCollection
    .aggregate([
      { $match: { guildId, seasonNumber } },
      { $group: { _id: null, total: { $sum: "$nuts" } } },
    ])
    .toArray();

  return result?.total || 0;
}

async function findCountChannel(guild) {
  if (!guild) {
    return null;
  }

  await guild.channels.fetch().catch(() => null);

  return (
    guild.channels.cache.find(
      (channel) =>
        channel.type === ChannelType.GuildText &&
        channel.name === COUNT_CHANNEL_NAME
    ) || null
  );
}

async function fetchDisplayName(guild, userId) {
  const member = await guild.members.fetch(userId).catch(() => null);
  if (member) {
    return member.displayName;
  }

  const user = await client.users.fetch(userId).catch(() => null);
  return user?.username || `Unknown (${userId})`;
}

function buildSeasonLabel(guildState, seasonNumber) {
  const seasonWindow = buildSeasonWindow(
    guildState.seasonNumber,
    guildState.seasonStartedAt,
    seasonNumber
  );

  return `Season ${seasonNumber} (${formatDate(seasonWindow.startAt)} to ${formatDate(
    seasonWindow.endAt
  )})`;
}

function buildStatsEmbed({
  guildState,
  targetUser,
  lifetimeStats,
  seasonStats,
  requestedSeason,
  color,
  isSelf,
}) {
  const activeSeason = requestedSeason || guildState.seasonNumber;
  const seasonLabel = buildSeasonLabel(guildState, activeSeason);
  const embed = new EmbedBuilder()
    .setTitle(isSelf ? "Your Nut Stats" : `${targetUser.username}'s Nut Stats`)
    .setColor(color)
    .addFields(
      {
        name: "Lifetime Nuts",
        value: formatNumber(lifetimeStats?.nuts || 0),
        inline: true,
      },
      {
        name: "Weekly Nuts",
        value: formatNumber(lifetimeStats?.weeklyNuts || 0),
        inline: true,
      },
      {
        name: requestedSeason ? `Season ${activeSeason}` : "Current Season",
        value: formatNumber(seasonStats?.nuts || 0),
        inline: true,
      },
      {
        name: "Season Window",
        value: seasonLabel,
      },
      {
        name: "First Recorded Nut",
        value: lifetimeStats?.firstNutAt
          ? formatDate(lifetimeStats.firstNutAt)
          : "No record yet",
      }
    );

  return embed;
}

async function buildLeaderboardEmbed(guild, guildState, seasonNumber) {
  const isSeasonBoard = Boolean(seasonNumber);
  const results = isSeasonBoard
    ? await getSeasonLeaderboard(guild.id, seasonNumber)
    : await getLifetimeLeaderboard(guild.id);

  if (results.length === 0) {
    return null;
  }

  const title = isSeasonBoard
    ? `${buildSeasonLabel(guildState, seasonNumber)} Leaderboard`
    : "Lifetime Nut Leaderboard";

  const embed = new EmbedBuilder().setTitle(title).setColor("Gold");
  const topEntries = results.slice(0, TOP_LIMIT);
  const names = await Promise.all(
    topEntries.map((entry) => fetchDisplayName(guild, entry.userId))
  );

  topEntries.forEach((entry, index) => {
    const label = isSeasonBoard ? entry.nuts : entry.nuts;
    embed.addFields({
      name: `#${index + 1} ${names[index]}`,
      value: `**${formatNumber(label)}** nuts`,
    });
  });

  return embed;
}

async function registerCommandsForGuild(guildId) {
  console.log(`Registering slash commands for guild ${guildId}...`);

  await rest.put(Routes.applicationGuildCommands(CLIENT_ID, guildId), {
    body: commands,
  });

  console.log(`Slash commands registered for guild ${guildId}`);
}

async function registerCommandsForAllGuilds() {
  const guilds = await client.guilds.fetch();
  console.log(`Found ${guilds.size} guild(s) for command registration.`);

  for (const [guildId, guildPreview] of guilds) {
    console.log(
      `Initializing guild ${guildPreview.name || "Unknown"} (${guildId})`
    );
    await ensureGuildRecord(guildId);
    await registerCommandsForGuild(guildId);
  }
}

async function runDailyCountAnnouncement() {
  for (const guild of client.guilds.cache.values()) {
    const guildState = await refreshGuildSeason(guild.id);

    if (!guildState.authorized) {
      continue;
    }

    const channel = await findCountChannel(guild);
    if (!channel) {
      continue;
    }

    if (guildState.rolledSeasons > 0) {
      await channel.send({ embeds: [buildSeasonRolloverEmbed(guildState)] });
    }

    await channel.send({ embeds: [buildDailyCountEmbed(guildState)] });
  }
}

async function runWeeklyReset() {
  for (const guild of client.guilds.cache.values()) {
    const guildState = await getGuildState(guild.id);

    if (!guildState.authorized) {
      continue;
    }

    const leaderboard = await getWeeklyLeaderboard(guild.id);
    const winner = leaderboard.find((entry) => (entry.weeklyNuts || 0) > 0);
    const channel = await findCountChannel(guild);

    if (winner && channel) {
      const name = await fetchDisplayName(guild, winner.userId);
      const embed = new EmbedBuilder()
        .setTitle("Weekly Winner")
        .setDescription(
          `**${name}** won the week with **${formatNumber(
            winner.weeklyNuts
          )}** nuts.`
        )
        .setColor("Purple")
        .setTimestamp();

      await channel.send({ embeds: [embed] });
    }

    await userCollection.updateMany(
      { guildId: guild.id },
      { $set: { weeklyNuts: 0, updatedAt: new Date() } }
    );
  }
}

function buildHelpEmbed(isAuthorized) {
  const embed = new EmbedBuilder()
    .setTitle("NutBot Commands")
    .setColor("Aqua");

  if (!isAuthorized) {
    return buildUnauthorizedHelpEmbed();
  }

  return embed
    .setDescription(
      `Counting only works in **#${COUNT_CHANNEL_NAME}**. Seasons change every 90 days, but the global count does not reset.`
    )
    .addFields(
      { name: "/setup password", value: "Admin-only server activation." },
      { name: "/adminstatus", value: "Admin-only bot health and setup status." },
      { name: "/count", value: "Show the current count for this server." },
      { name: "/nut", value: "Show your lifetime and current season totals." },
      { name: "/mystats [season]", value: "Show your stats for now or a legacy season." },
      { name: "/stats user [season]", value: "Show someone else's stats." },
      { name: "/compare user1 user2 [season]", value: "Compare two users now or in a legacy season." },
      { name: "/leaderboard [season]", value: "Show lifetime or season leaderboard results." },
      { name: "/weekly", value: "Show the weekly leaderboard." },
      { name: "/season [season]", value: "Show current season details or a legacy season." }
    );
}

client.once("clientReady", async (readyClient) => {
  console.log(`NutBot is online as ${readyClient.user.tag}`);
  console.log(`Bot is currently in ${readyClient.guilds.cache.size} guild(s).`);

  try {
    await registerCommandsForAllGuilds();
    await reconcileAllGuildsFromHistory();
  } catch (error) {
    console.error("Failed to register slash commands:", error);
  }
});

client.on("error", (error) => {
  console.error("Discord client error:", error);
});

client.on("warn", (warning) => {
  console.warn("Discord client warning:", warning);
});

client.on("shardDisconnect", (event, shardId) => {
  console.warn(
    `Discord shard ${shardId} disconnected with code ${event.code}. Clean: ${event.wasClean}`
  );
});

client.on("shardError", (error, shardId) => {
  console.error(`Discord shard ${shardId} error:`, error);
});

client.on("shardReady", (shardId, unavailableGuilds) => {
  console.log(
    `Discord shard ${shardId} ready. Unavailable guilds: ${unavailableGuilds?.size || 0}`
  );
});

client.on("shardReconnecting", (shardId) => {
  console.warn(`Discord shard ${shardId} reconnecting...`);
});

client.on("shardResume", (shardId, replayedEvents) => {
  console.log(
    `Discord shard ${shardId} resumed after replaying ${replayedEvents} event(s).`
  );

  reconcileAllGuildsFromHistory().catch((error) => {
    console.error("Guild reconciliation after shard resume failed:", error);
  });
});

client.on("guildCreate", async (guild) => {
  try {
    console.log(`Joined new guild ${guild.name} (${guild.id})`);
    await ensureGuildRecord(guild.id);
    await registerCommandsForGuild(guild.id);
  } catch (error) {
    console.error(`Failed to initialize guild ${guild.id}:`, error);
  }
});

cron.schedule(
  "0 0 * * *",
  async () => {
    try {
      console.log("Starting nightly count announcement job...");
      await runDailyCountAnnouncement();
      console.log("Nightly count announcement job completed.");
    } catch (error) {
      console.error("Nightly count announcement job failed:", error);
    }
  },
  {
    timezone: BOT_TIMEZONE,
  }
);

cron.schedule(
  "5 0 * * 0",
  async () => {
    try {
      console.log("Starting weekly reset job...");
      await runWeeklyReset();
      console.log("Weekly reset job completed.");
    } catch (error) {
      console.error("Weekly reset job failed:", error);
    }
  },
  {
    timezone: BOT_TIMEZONE,
  }
);

client.on("messageCreate", async (message) => {
  if (message.author.bot || !message.guild) {
    return;
  }

  if (
    message.channel.type !== ChannelType.GuildText ||
    message.channel.name !== COUNT_CHANNEL_NAME
  ) {
    return;
  }

  try {
    let guildState = await refreshGuildSeason(message.guild.id);

    if (!guildState.authorized) {
      return;
    }

    const content = message.content.trim();
    if (!/^\d+$/.test(content)) {
      return;
    }

    const nextNumber = guildState.lastNumber + 1;

    if (content !== String(nextNumber)) {
      guildState = await reconcileGuildCountFromChannel(message.guild, guildState);
      const reconciledNextNumber = (guildState.lastNumber || 0) + 1;

      if (content === String(reconciledNextNumber)) {
        const claim = await claimNextCount(message.guild.id, guildState.lastNumber);

        if (!claim.claimed) {
          const freshGuildState = await getGuildState(message.guild.id);
          console.warn(
            `Count race detected in guild ${message.guild.id}. Expected ${reconciledNextNumber}, actual next is ${(freshGuildState.lastNumber || 0) + 1}.`
          );
          await message.reply(
            buildWrongCountMessage((freshGuildState.lastNumber || 0) + 1)
          );
          return;
        }

        const stats = await addNut(
          message.guild.id,
          message.author.id,
          guildState.seasonNumber
        );

        if (stats.isMilestone) {
          await message.channel.send({
            embeds: [buildMilestoneEmbed(message.author.id, stats)],
          });
        }

        if (stats.shouldSendCareReminder) {
          await message.channel.send(
            `<@${message.author.id}> ${getRandomCareReminder()}`
          );
        }

        return;
      }

      await message.reply(buildWrongCountMessage(reconciledNextNumber));
      return;
    }

    const claim = await claimNextCount(message.guild.id, guildState.lastNumber);

    if (!claim.claimed) {
      const freshGuildState = await getGuildState(message.guild.id);
      console.warn(
        `Count race detected in guild ${message.guild.id}. Expected ${nextNumber}, actual next is ${(freshGuildState.lastNumber || 0) + 1}.`
      );
      await message.reply(
        buildWrongCountMessage((freshGuildState.lastNumber || 0) + 1)
      );
      return;
    }

    const stats = await addNut(
      message.guild.id,
      message.author.id,
      guildState.seasonNumber
    );

    if (stats.isMilestone) {
      await message.channel.send({
        embeds: [buildMilestoneEmbed(message.author.id, stats)],
      });
    }

    if (stats.shouldSendCareReminder) {
      await message.channel.send(
        `<@${message.author.id}> ${getRandomCareReminder()}`
      );
    }
  } catch (error) {
    console.error("Counting error:", error);
  }
});

client.on("interactionCreate", async (interaction) => {
  if (!interaction.isChatInputCommand()) {
    return;
  }

  if (!interaction.guildId || !interaction.guild) {
    await interaction.reply({
      content: "This bot only works inside a server.",
      ephemeral: true,
    });
    return;
  }

  try {
    const commandName = interaction.commandName;

    if (commandName === "setup") {
      const password = interaction.options.getString("password", true);

      if (
        !interaction.memberPermissions?.has(PermissionFlagsBits.Administrator)
      ) {
        await interaction.reply({
          content: "Only a server admin can activate this bot.",
          ephemeral: true,
        });
        return;
      }

      if (password !== SETUP_PASSWORD) {
        await interaction.reply({
          content: "Wrong password.",
          ephemeral: true,
        });
        return;
      }

      const existingState = await ensureGuildRecord(interaction.guildId);
      const guildState = existingState.authorized
        ? existingState
        : await authorizeGuild(interaction.guildId, interaction.user.id);
      const channel = await findCountChannel(interaction.guild);

      await interaction.reply({
        embeds: [
          new EmbedBuilder()
            .setTitle("NutBot Activated")
            .setDescription(
              channel
                ? `This server is active. Counting is live in **#${COUNT_CHANNEL_NAME}**.`
                : `This server is active, but you still need to create **#${COUNT_CHANNEL_NAME}** before counting will work.`
            )
            .addFields(
              {
                name: "Current Count",
                value: formatNumber(guildState.lastNumber || 0),
                inline: true,
              },
              {
                name: "Current Season",
                value: `Season ${guildState.seasonNumber || 1}`,
                inline: true,
              }
            )
            .setColor("Green")
            .setTimestamp(),
        ],
        ephemeral: true,
      });
      return;
    }

    const guildState = await getGuildState(interaction.guildId);

    if (commandName === "adminstatus") {
      if (
        !interaction.memberPermissions?.has(PermissionFlagsBits.Administrator)
      ) {
        await interaction.reply({
          content: "Only a server admin can use this command.",
          ephemeral: true,
        });
        return;
      }

      const countChannel = await findCountChannel(interaction.guild);
      const dbState = guildState.authorized ? "Connected" : "Connected (inactive)";
      const uptimeMs = client.uptime || 0;
      const uptimeMinutes = Math.floor(uptimeMs / 60000);
      const uptimeHours = Math.floor(uptimeMinutes / 60);
      const uptimeRemainderMinutes = uptimeMinutes % 60;

      await interaction.reply({
        embeds: [
          new EmbedBuilder()
            .setTitle("NutBot Admin Status")
            .setColor("DarkGreen")
            .addFields(
              {
                name: "Server Active",
                value: guildState.authorized ? "Yes" : "No",
                inline: true,
              },
              {
                name: "Count Channel",
                value: countChannel ? `#${COUNT_CHANNEL_NAME}` : "Missing",
                inline: true,
              },
              {
                name: "Current Count",
                value: formatNumber(guildState.lastNumber || 0),
                inline: true,
              },
              {
                name: "Current Season",
                value: `Season ${guildState.seasonNumber || 1}`,
                inline: true,
              },
              {
                name: "Discord Status",
                value: client.isReady() ? "Ready" : "Not ready",
                inline: true,
              },
              {
                name: "Database Status",
                value: dbState,
                inline: true,
              },
              {
                name: "Bot Uptime",
                value: `${uptimeHours}h ${uptimeRemainderMinutes}m`,
                inline: true,
              }
            )
            .setTimestamp(),
        ],
        ephemeral: true,
      });
      return;
    }

    if (commandName === "help") {
      await interaction.reply({ embeds: [buildHelpEmbed(guildState.authorized)] });
      return;
    }

    if (!guildState.authorized) {
      await interaction.reply({
        embeds: [buildUnauthorizedHelpEmbed()],
        ephemeral: true,
      });
      return;
    }

    if (commandName === "nut") {
      const lifetimeStats = await getUser(interaction.guildId, interaction.user.id);
      const seasonStats = await getSeasonUser(
        interaction.guildId,
        interaction.user.id,
        guildState.seasonNumber
      );

      await interaction.reply({
        embeds: [
          buildStatsEmbed({
            guildState,
            targetUser: interaction.user,
            lifetimeStats,
            seasonStats,
            color: "Green",
            isSelf: true,
          }),
        ],
      });
      return;
    }

    if (commandName === "count") {
      await interaction.reply({
        embeds: [
          new EmbedBuilder()
            .setTitle("Current Count")
            .setDescription(
              `The current count for this server is **${formatNumber(
                guildState.lastNumber
              )}**.`
            )
            .addFields({
              name: "Current Season",
              value: `Season ${guildState.seasonNumber}`,
              inline: true,
            })
            .setColor("Blue"),
        ],
      });
      return;
    }

    if (commandName === "leaderboard") {
      const seasonNumber = interaction.options.getInteger("season");

      if (seasonNumber && seasonNumber > guildState.seasonNumber) {
        await interaction.reply({
          content: `Season ${seasonNumber} does not exist yet. The current season is ${guildState.seasonNumber}.`,
          ephemeral: true,
        });
        return;
      }

      const embed = await buildLeaderboardEmbed(
        interaction.guild,
        guildState,
        seasonNumber
      );

      await interaction.reply({
        embeds: [
          embed ||
            new EmbedBuilder()
              .setTitle("No Results")
              .setDescription(
                seasonNumber
                  ? `No one has recorded any nuts in Season ${seasonNumber} yet.`
                  : "Nobody has nutted yet."
              )
              .setColor("Orange"),
        ],
      });
      return;
    }

    if (commandName === "weekly") {
      const results = await getWeeklyLeaderboard(interaction.guildId);
      const topEntries = results.filter((entry) => (entry.weeklyNuts || 0) > 0);

      if (topEntries.length === 0) {
        await interaction.reply("Nobody has nutted this week.");
        return;
      }

      const embed = new EmbedBuilder()
        .setTitle("Weekly Nut Leaderboard")
        .setColor("Purple");

      const leaders = topEntries.slice(0, TOP_LIMIT);
      const names = await Promise.all(
        leaders.map((entry) => fetchDisplayName(interaction.guild, entry.userId))
      );

      leaders.forEach((entry, index) => {
        embed.addFields({
          name: `#${index + 1} ${names[index]}`,
          value: `**${formatNumber(entry.weeklyNuts)}** nuts`,
        });
      });

      await interaction.reply({ embeds: [embed] });
      return;
    }

    if (commandName === "season") {
      const seasonNumber =
        interaction.options.getInteger("season") || guildState.seasonNumber;

      if (seasonNumber > guildState.seasonNumber) {
        await interaction.reply({
          content: `Season ${seasonNumber} does not exist yet. The current season is ${guildState.seasonNumber}.`,
          ephemeral: true,
        });
        return;
      }

      const seasonWindow = buildSeasonWindow(
        guildState.seasonNumber,
        guildState.seasonStartedAt,
        seasonNumber
      );
      const seasonTotal = await getSeasonTotal(interaction.guildId, seasonNumber);

      await interaction.reply({
        embeds: [
          new EmbedBuilder()
            .setTitle(`Season ${seasonNumber}`)
            .setDescription(
              seasonNumber === guildState.seasonNumber
                ? "This is the current active season. The global count continues even when a new season starts."
                : "This is a legacy season. Season leaderboards reset, but the global count always continues."
            )
            .addFields(
              {
                name: "Season Window",
                value: `${formatDate(seasonWindow.startAt)} to ${formatDate(
                  seasonWindow.endAt
                )}`,
              },
              {
                name: "Recorded Nuts",
                value: formatNumber(seasonTotal),
                inline: true,
              },
              {
                name: "Current Count",
                value: formatNumber(guildState.lastNumber),
                inline: true,
              }
            )
            .setColor("Blue"),
        ],
      });
      return;
    }

    if (commandName === "stats" || commandName === "mystats") {
      const targetUser =
        commandName === "stats"
          ? interaction.options.getUser("user", true)
          : interaction.user;
      const seasonNumber =
        interaction.options.getInteger("season") || guildState.seasonNumber;

      if (seasonNumber > guildState.seasonNumber) {
        await interaction.reply({
          content: `Season ${seasonNumber} does not exist yet. The current season is ${guildState.seasonNumber}.`,
          ephemeral: true,
        });
        return;
      }

      const [lifetimeStats, seasonStats] = await Promise.all([
        getUser(interaction.guildId, targetUser.id),
        getSeasonUser(interaction.guildId, targetUser.id, seasonNumber),
      ]);

      await interaction.reply({
        embeds: [
          buildStatsEmbed({
            guildState,
            targetUser,
            lifetimeStats,
            seasonStats,
            requestedSeason:
              seasonNumber === guildState.seasonNumber ? null : seasonNumber,
            color: commandName === "stats" ? "Blue" : "Green",
            isSelf: commandName === "mystats",
          }),
        ],
      });
      return;
    }

    if (commandName === "compare") {
      const user1 = interaction.options.getUser("user1", true);
      const user2 = interaction.options.getUser("user2", true);
      const seasonNumber = interaction.options.getInteger("season");

      if (seasonNumber && seasonNumber > guildState.seasonNumber) {
        await interaction.reply({
          content: `Season ${seasonNumber} does not exist yet. The current season is ${guildState.seasonNumber}.`,
          ephemeral: true,
        });
        return;
      }

      const [stats1, stats2, seasonStats1, seasonStats2] = await Promise.all([
        getUser(interaction.guildId, user1.id),
        getUser(interaction.guildId, user2.id),
        getSeasonUser(
          interaction.guildId,
          user1.id,
          seasonNumber || guildState.seasonNumber
        ),
        getSeasonUser(
          interaction.guildId,
          user2.id,
          seasonNumber || guildState.seasonNumber
        ),
      ]);

      const label = seasonNumber
        ? `Season ${seasonNumber}`
        : `Current Season (${guildState.seasonNumber})`;
      const score1 = seasonStats1?.nuts || 0;
      const score2 = seasonStats2?.nuts || 0;
      const winner =
        score1 > score2
          ? `${user1.username} is ahead.`
          : score2 > score1
          ? `${user2.username} is ahead.`
          : "It is tied right now.";

      await interaction.reply({
        embeds: [
          new EmbedBuilder()
            .setTitle(`Nut Battle: ${user1.username} vs ${user2.username}`)
            .addFields(
              {
                name: user1.username,
                value: `Lifetime: **${formatNumber(
                  stats1?.nuts || 0
                )}**\n${label}: **${formatNumber(score1)}**`,
                inline: true,
              },
              {
                name: user2.username,
                value: `Lifetime: **${formatNumber(
                  stats2?.nuts || 0
                )}**\n${label}: **${formatNumber(score2)}**`,
                inline: true,
              },
              {
                name: "Status",
                value: winner,
              }
            )
            .setColor("Red"),
        ],
      });
    }
  } catch (error) {
    console.error("Interaction error:", error);

    if (interaction.deferred || interaction.replied) {
      await interaction.followUp({
        content: "Something went wrong while handling that command.",
        ephemeral: true,
      });
      return;
    }

    await interaction.reply({
      content: "Something went wrong while handling that command.",
      ephemeral: true,
    });
  }
});

(async () => {
  await connectDB();
  await client.login(TOKEN);
})();
