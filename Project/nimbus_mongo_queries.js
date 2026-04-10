// ============================================================
// NimbusAI Data Analyst Assignment — MongoDB Aggregation Pipelines
// Database: nimbus_events
// Focus: Option A — Customer Churn & Retention Analysis
// Run: mongosh nimbus_events --file nimbus_mongo_queries.js
// ============================================================

// ─── Helper: normalise inconsistent field names ──────────────
// Documents use customer_id / customerId / customerID and
// member_id / memberId / userId / userID interchangeably.
// We add a $addFields stage at the top of every pipeline to
// unify these before processing.

const NORMALIZE_FIELDS = {
  $addFields: {
    _cid: {
      $toInt: {
        $ifNull: [
          "$customer_id",
          { $ifNull: ["$customerId", { $ifNull: ["$customerID", null] }] }
        ]
      }
    },
    _mid: {
      $toInt: {
        $ifNull: [
          "$member_id",
          { $ifNull: ["$memberId", { $ifNull: ["$userId", { $ifNull: ["$userID", null] }] }] }
        ]
      }
    },
    // Normalise timestamp: parse string formats into a Date where possible
    _ts: {
      $cond: {
        if: { $eq: [{ $type: "$timestamp" }, "date"] },
        then: "$timestamp",
        else: {
          // Attempt ISO string parse; non-ISO strings (MM/DD/YYYY) will
          // remain as strings and be handled downstream in Python/R wrangling
          $dateFromString: {
            dateString: "$timestamp",
            onError: null,   // null signals bad format — downstream filters
            onNull: null
          }
        }
      }
    }
  }
};

// ─── Q1: Average Sessions per User per Week by Subscription Tier ─────────
// Methodology:
//   - A "session" is inferred from login events (event_type == "login")
//     or any event, depending on data availability.  We use all events
//     grouped by (customer, member, ISO-week).
//   - Subscription tier is stored in SQL; we join via a $lookup on a
//     view/collection called "customer_tiers" (pre-populated from SQL).
//     Without a direct SQL bridge, we group by customer_id only and note
//     the tier join must happen at the BI layer.
//   - Percentiles are computed via $percentile (MongoDB 7+) or approximated
//     with $bucketAuto for earlier versions.

print("\n=== Q1: Sessions per User per Week (by tier) ===");

db.user_activity_logs.aggregate([
  NORMALIZE_FIELDS,

  // Drop records with null timestamps or IDs (data quality filter)
  { $match: { _ts: { $ne: null }, _cid: { $ne: null }, _mid: { $ne: null } } },

  // Bucket events into ISO year-week
  {
    $addFields: {
      week_key: {
        $concat: [
          { $toString: { $isoWeekYear: "$_ts" } },
          "-W",
          { $toString: { $isoWeek: "$_ts" } }
        ]
      }
    }
  },

  // Count distinct sessions per (customer, member, week)
  {
    $group: {
      _id: { cid: "$_cid", mid: "$_mid", week: "$week_key" },
      session_count: { $sum: 1 },
      avg_session_dur_sec: { $avg: "$session_duration_sec" }
    }
  },

  // Roll up to per-(customer, member) weekly average
  {
    $group: {
      _id: { cid: "$_id.cid", mid: "$_id.mid" },
      avg_sessions_per_week: { $avg: "$session_count" },
      all_durations: { $push: "$avg_session_dur_sec" }
    }
  },

  // Approximate percentiles via sorting (MongoDB < 7 compatible)
  // NOTE: For MongoDB 7+ replace this stage with $percentile operator
  {
    $addFields: {
      sorted_durations: { $sortArray: { input: "$all_durations", sortBy: 1 } }
    }
  },
  {
    $addFields: {
      n: { $size: "$sorted_durations" },
      p25_idx: { $floor: { $multiply: [0.25, { $size: "$sorted_durations" }] } },
      p50_idx: { $floor: { $multiply: [0.50, { $size: "$sorted_durations" }] } },
      p75_idx: { $floor: { $multiply: [0.75, { $size: "$sorted_durations" }] } }
    }
  },
  {
    $addFields: {
      p25_duration_sec: { $arrayElemAt: ["$sorted_durations", "$p25_idx"] },
      p50_duration_sec: { $arrayElemAt: ["$sorted_durations", "$p50_idx"] },
      p75_duration_sec: { $arrayElemAt: ["$sorted_durations", "$p75_idx"] }
    }
  },

  // Final aggregation: group by customer_id (tier join done externally)
  {
    $group: {
      _id: "$_id.cid",
      avg_sessions_per_user_per_week: { $avg: "$avg_sessions_per_week" },
      p25_session_dur_sec: { $avg: "$p25_duration_sec" },
      p50_session_dur_sec: { $avg: "$p50_duration_sec" },
      p75_session_dur_sec: { $avg: "$p75_duration_sec" },
      user_count: { $sum: 1 }
    }
  },

  { $sort: { avg_sessions_per_user_per_week: -1 } },
  { $limit: 100 }  // top 100 customers; remove for full output
]).forEach(printjson);


// ─── Q2: DAU and 7-Day Feature Retention Rate ────────────────
// For each feature: daily active users + 7-day retention
// (users who used the feature again within 7 days of first use).

print("\n=== Q2: Feature DAU and 7-Day Retention ===");

db.user_activity_logs.aggregate([
  NORMALIZE_FIELDS,
  { $match: {
    _ts:     { $ne: null },
    _mid:    { $ne: null },
    feature: { $exists: true, $ne: null }
  }},

  // Get the first-use date per (member, feature)
  {
    $group: {
      _id: { mid: "$_mid", feature: "$feature" },
      first_use: { $min: "$_ts" },
      all_dates: { $addToSet: {
        $dateToString: { format: "%Y-%m-%d", date: "$_ts" }
      }}
    }
  },

  // Tag whether user returned within 7 days of first use
  {
    $addFields: {
      first_use_date_str: {
        $dateToString: { format: "%Y-%m-%d", date: "$first_use" }
      },
      day7_cutoff: {
        $dateToString: {
          format: "%Y-%m-%d",
          date: { $dateAdd: { startDate: "$first_use", unit: "day", amount: 7 } }
        }
      },
      returned_in_7d: {
        $gt: [
          {
            $size: {
              $filter: {
                input: "$all_dates",
                as: "d",
                cond: {
                  $and: [
                    { $gt: ["$$d", { $dateToString: { format: "%Y-%m-%d", date: "$first_use" } }] },
                    { $lte: ["$$d", {
                      $dateToString: {
                        format: "%Y-%m-%d",
                        date: { $dateAdd: { startDate: "$first_use", unit: "day", amount: 7 } }
                      }
                    }] }
                  ]
                }
              }
            }
          },
          0
        ]
      }
    }
  },

  // Aggregate per feature: unique users per day + 7d retention rate
  {
    $group: {
      _id: "$_id.feature",
      total_unique_users: { $sum: 1 },
      retained_users:     { $sum: { $cond: ["$returned_in_7d", 1, 0] } },
      // DAU approximation: spread users across distinct days they appeared
      total_person_days:  { $sum: { $size: "$all_dates" } }
    }
  },

  {
    $addFields: {
      retention_rate_7d_pct: {
        $round: [
          { $multiply: [
            { $divide: ["$retained_users", { $ifNull: ["$total_unique_users", 1] }] },
            100
          ]},
          2
        ]
      },
      avg_dau_estimate: {
        // Rough DAU = total person-days / 30 (30-day window assumption)
        $round: [{ $divide: ["$total_person_days", 30] }, 1]
      }
    }
  },

  { $sort: { retention_rate_7d_pct: -1 } }
]).forEach(printjson);


// ─── Q3: Onboarding Funnel Analysis ──────────────────────────
// Funnel: signup → first_login → workspace_created → first_project → invited_teammate
// Drop-off rates at each stage; median time between steps.

print("\n=== Q3: Onboarding Funnel ===");

// ── 3a. Stage counts (users who reached each step) ──
db.onboarding_events.aggregate([
  NORMALIZE_FIELDS,
  { $match: { _mid: { $ne: null }, completed: true } },

  // Normalise step names using a lookup map
  {
    $addFields: {
      funnel_step: {
        $switch: {
          branches: [
            { case: { $eq: ["$step", "signup"] },            then: "1_signup" },
            { case: { $eq: ["$step", "first_login"] },       then: "2_first_login" },
            { case: { $eq: ["$step", "workspace_created"] }, then: "3_workspace_created" },
            { case: { $eq: ["$step", "first_project"] },     then: "4_first_project" },
            { case: { $eq: ["$step", "invited_teammate"] },  then: "5_invited_teammate" }
          ],
          default: null
        }
      }
    }
  },
  { $match: { funnel_step: { $ne: null } } },

  // One row per (member, step) — take earliest occurrence
  {
    $group: {
      _id: { mid: "$_mid", step: "$funnel_step" },
      earliest: { $min: "$_ts" },
      duration_sec: { $first: "$duration_seconds" }
    }
  },

  // Group by step to count unique users
  {
    $group: {
      _id: "$_id.step",
      unique_users: { $sum: 1 },
      // Median approximation: sort and take middle value using $percentile (Mongo 7+)
      // For compatibility we collect all and note: sort in app layer
      durations: { $push: "$duration_sec" }
    }
  },

  { $sort: { _id: 1 } }
]).forEach(doc => {
  const sorted = (doc.durations || [])
    .filter(d => d != null)
    .sort((a, b) => a - b);
  const median = sorted.length
    ? sorted[Math.floor(sorted.length / 2)]
    : null;
  printjson({
    funnel_step:  doc._id,
    unique_users: doc.unique_users,
    median_step_duration_sec: median
  });
});

// ── 3b. Drop-off rates (computed in JS after fetching stage counts) ──
// Run this block after capturing stage counts above.
// Replace the counts array with actual values from 3a output.
print("\n--- Funnel Drop-off Rates ---");
const stageCounts = [
  { step: "1_signup",            users: null },  // fill from 3a
  { step: "2_first_login",       users: null },
  { step: "3_workspace_created", users: null },
  { step: "4_first_project",     users: null },
  { step: "5_invited_teammate",  users: null }
];
stageCounts.forEach((s, i) => {
  if (i === 0 || !stageCounts[i - 1].users || !s.users) return;
  const dropoff = ((stageCounts[i - 1].users - s.users) / stageCounts[i - 1].users * 100).toFixed(1);
  print(`${stageCounts[i - 1].step} → ${s.step}: ${dropoff}% drop-off`);
});


// ─── Q4: Top 20 Engaged Free-Tier Users (Upsell Targets) ─────
// Cross-reference SQL customer_ids for free plan accounts.
// Engagement Score = weighted sum of:
//   - session frequency (40%): distinct days active / total days
//   - feature breadth (30%): count of distinct features used
//   - session depth (30%):   average session_duration_sec / 3600
// Higher score = stronger upsell candidate.

print("\n=== Q4: Top 20 Engaged Free-Tier Users ===");

// FREE_TIER_CUSTOMER_IDS should be populated from SQL query:
//   SELECT customer_id FROM subscriptions s
//   JOIN plans p ON p.plan_id = s.plan_id
//   WHERE p.plan_tier = 'free' AND s.status = 'active';
//
// For the pipeline we demonstrate the pattern with a placeholder.
// In production: pass the array from SQL output.
const FREE_TIER_CUSTOMER_IDS = [3,7,8,9,25,39,43,44,45,46,49,50,53,54,65,66,67,69,83,84,85,92,101,106,117,135,149,155,157,163,165,166,167,169,170,172,175,178,207,212,217,226,235,243,251,260,261,264,266,268,269,271,272,281,282,288,313,315,320,321,329,330,333,340,343,348,353,357,367,371,376,381,393,399,419,420,437,441,446,449,452,457,470,488,489,494,502,513,526,531,542,557,568,574,581,582,583,592,595,596,605,608,609,622,624,631,635,638,640,653,656,667,681,686,688,689,692,694,702,720,725,727,732,741,745,750,754,755,756,759,774,782,783,796,828,830,837,856,862,873,880,881,884,885,897,899,912,914,916,922,924,925,926,930,931,932,936,939,941,954,966,972,974,992,999,1000,1008,1011,1014,1022,1024,1032,1038,1041,1043,1046,1048,1052,1058,1060,1064,1072,1078,1080,1099,1108,1109,1114,1116,1118,1127,1134,1140,1149,1156,1166,1171,1180,1195,1198];  // <-- inject from SQL

db.user_activity_logs.aggregate([
  NORMALIZE_FIELDS,
  { $match: {
    _ts:  { $ne: null },
    _mid: { $ne: null },
    // If free tier IDs are available, filter here:
    ...(FREE_TIER_CUSTOMER_IDS.length > 0
      ? { _cid: { $in: FREE_TIER_CUSTOMER_IDS } }
      : {}  // omit filter if list is empty; inject externally
    )
  }},

  // Per-user aggregations needed for engagement score
  {
    $group: {
      _id: { cid: "$_cid", mid: "$_mid" },
      distinct_active_days: {
        $addToSet: { $dateToString: { format: "%Y-%m-%d", date: "$_ts" } }
      },
      distinct_features: {
        $addToSet: {
          $cond: [{ $ifNull: ["$feature", false] }, "$feature", "$$REMOVE"]
        }
      },
      avg_session_dur_sec: { $avg: "$session_duration_sec" },
      first_seen: { $min: "$_ts" },
      last_seen:  { $max: "$_ts" },
      total_events: { $sum: 1 }
    }
  },

  // Compute engagement score
  {
    $addFields: {
      days_active: { $size: "$distinct_active_days" },
      feature_count: { $size: "$distinct_features" },
      // Normalise tenure in days for session frequency
      tenure_days: {
        $max: [
          1,
          {
            $divide: [
              { $subtract: ["$last_seen", "$first_seen"] },
              86400000  // ms → days
            ]
          }
        ]
      }
    }
  },
  {
    $addFields: {
      session_frequency: { $divide: ["$days_active", "$tenure_days"] },  // 0-1
      feature_breadth:   { $min: [{ $divide: ["$feature_count", 10] }, 1] }, // normalise to ~10 features
      session_depth:     { $min: [{ $divide: [{ $ifNull: ["$avg_session_dur_sec", 0] }, 3600] }, 1] }
    }
  },
  {
    $addFields: {
      // Weighted engagement score (weights sum to 1)
      engagement_score: {
        $round: [
          {
            $add: [
              { $multiply: ["$session_frequency", 0.40] },
              { $multiply: ["$feature_breadth",   0.30] },
              { $multiply: ["$session_depth",      0.30] }
            ]
          },
          4
        ]
      }
    }
  },

  { $sort: { engagement_score: -1 } },
  { $limit: 20 },

  // Project clean output
  {
    $project: {
      _id: 0,
      customer_id:   "$_id.cid",
      member_id:     "$_id.mid",
      engagement_score: 1,
      days_active: 1,
      feature_count: 1,
      avg_session_dur_sec: { $round: ["$avg_session_dur_sec", 0] },
      total_events: 1,
      last_seen: 1
    }
  }
]).forEach(printjson);

// ============================================================
// END OF MONGODB QUERIES
// ============================================================
