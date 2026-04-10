print("\n=== Q4: Top 20 Engaged Free-Tier Users ===");

const FREE_TIER_CUSTOMER_IDS = [1000, 1008, 101, 1011, 1014, 1022, 1024, 1032, 1038, 1041, 1043, 1046, 1048, 1052, 1058, 106, 1060, 1064, 1072, 1078, 1080, 1099, 1108, 1109, 1114, 1116, 1118, 1127, 1134, 1140, 1149, 1156, 1166, 117, 1171, 1180, 1195, 1198, 135, 149, 155, 157, 163, 165, 166, 167, 169, 170, 172, 175, 178, 207, 212, 217, 226, 235, 243, 25, 251, 260, 261, 264, 266, 268, 269, 271, 272, 281, 282, 288, 3, 313, 315, 320, 321, 329, 330, 333, 340, 343, 348, 353, 357, 367, 371, 376, 381, 39, 393, 399, 419, 420, 43, 437, 44, 441, 446, 449, 45, 452, 457, 46, 470, 488, 489, 49, 494, 50, 502, 513, 526, 53, 531, 54, 542, 557, 568, 574, 581, 582, 583, 592, 595, 596, 605, 608, 609, 622, 624, 631, 635, 638, 640, 65, 653, 656, 66, 667, 67, 681, 686, 688, 689, 69, 692, 694, 7, 702, 720, 725, 727, 732, 741, 745, 750, 754, 755, 756, 759, 774, 782, 783, 796, 8, 828, 83, 830, 837, 84, 85, 856, 862, 873, 880, 881, 884, 885, 897, 899, 9, 912, 914, 916, 92, 922, 924, 925, 926, 930, 931, 932, 936, 939, 941, 954, 966, 972, 974, 992, 999];

db.user_activity_logs.aggregate([
  // Fixed normalization — handles all field name AND type variants
  { $addFields: {
    _cid: {
      $toInt: {
        $ifNull: ["$customer_id", { $ifNull: ["$customerId", { $ifNull: ["$customerID", null] }] }]
      }
    },
    _mid: {
      $toInt: {
        $ifNull: ["$member_id", { $ifNull: ["$memberId", { $ifNull: ["$userId", { $ifNull: ["$userID", null] }] }] }]
      }
    },
    _ts: {
      $cond: {
        if: { $eq: [{ $type: "$timestamp" }, "date"] },
        then: "$timestamp",
        else: { $dateFromString: { dateString: "$timestamp", onError: null, onNull: null } }
      }
    },
    // Use event_type as feature proxy when feature field is absent
    _feature: {
      $ifNull: ["$feature", "$event_type"]
    }
  }},

  // Filter: valid timestamp, valid member, belongs to free tier
  { $match: {
    _ts:  { $ne: null },
    _mid: { $ne: null },
    _cid: { $in: FREE_TIER_CUSTOMER_IDS }
  }},

  // Per-user aggregation
  { $group: {
    _id: { cid: "$_cid", mid: "$_mid" },
    distinct_active_days: { $addToSet: { $dateToString: { format: "%Y-%m-%d", date: "$_ts" } } },
    distinct_features:    { $addToSet: "$_feature" },
    avg_session_dur_sec:  { $avg: "$session_duration_sec" },
    first_seen:           { $min: "$_ts" },
    last_seen:            { $max: "$_ts" },
    total_events:         { $sum: 1 }
  }},

  // Compute component metrics
  { $addFields: {
    days_active:   { $size: "$distinct_active_days" },
    feature_count: { $size: "$distinct_features" },
    tenure_days: {
      $max: [1, { $divide: [{ $subtract: ["$last_seen", "$first_seen"] }, 86400000] }]
    }
  }},

  // Require minimum activity to be a meaningful upsell candidate
  { $match: {
    $or: [
      { days_active:   { $gte: 2 } },
      { total_events:  { $gte: 3 } },
      { feature_count: { $gte: 2 } }
    ]
  }},

  // Normalise score components
  { $addFields: {
    session_frequency: { $divide: ["$days_active", "$tenure_days"] },
    feature_breadth:   { $min: [{ $divide: ["$feature_count", 10] }, 1] },
    session_depth:     { $min: [{ $divide: [{ $ifNull: ["$avg_session_dur_sec", 0] }, 3600] }, 1] }
  }},

  // Weighted engagement score
  { $addFields: {
    engagement_score: { $round: [{ $add: [
      { $multiply: ["$session_frequency", 0.40] },
      { $multiply: ["$feature_breadth",   0.30] },
      { $multiply: ["$session_depth",      0.30] }
    ]}, 4] }
  }},

  { $sort: { engagement_score: -1 } },
  { $limit: 20 },

  { $project: {
    _id: 0,
    customer_id:         "$_id.cid",
    member_id:           "$_id.mid",
    engagement_score:    1,
    days_active:         1,
    feature_count:       1,
    total_events:        1,
    avg_session_dur_sec: { $round: ["$avg_session_dur_sec", 0] },
    last_seen:           1
  }}
]).forEach(printjson);