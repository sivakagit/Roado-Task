use nimbus_events;

// 1. What's the actual distribution of field names used?
print("=== Field name distribution ===");
db.user_activity_logs.aggregate([
  { $project: {
    has_customer_id:   { $ifNull: ["$customer_id",  false] },
    has_customerId:    { $ifNull: ["$customerId",   false] },
    has_customerID:    { $ifNull: ["$customerID",   false] }
  }},
  { $group: {
    _id: {
      customer_id:  { $cond: [{ $ne: ["$has_customer_id", false] }, "present", "missing"] },
      customerId:   { $cond: [{ $ne: ["$has_customerId",  false] }, "present", "missing"] },
      customerID:   { $cond: [{ $ne: ["$has_customerID",  false] }, "present", "missing"] }
    },
    count: { $sum: 1 }
  }}
]).forEach(printjson);

// 2. What CIDs actually exist in the collection after normalization?
print("\n=== Sample of normalized _cid values (first 20 distinct) ===");
db.user_activity_logs.aggregate([
  { $addFields: {
    _cid: { $toInt: { $ifNull: ["$customer_id", { $ifNull: ["$customerId", { $ifNull: ["$customerID", null] }] }] } }
  }},
  { $match: { _cid: { $ne: null } } },
  { $group: { _id: "$_cid" } },
  { $sort: { _id: 1 } },
  { $limit: 20 }
]).forEach(d => print(d._id));

// 3. How many of your FREE_TIER_CUSTOMER_IDS actually have ANY events?
print("\n=== How many free-tier CIDs have events in user_activity_logs? ===");
const FREE_TIER_CUSTOMER_IDS = [1000, 1008, 101, 1011, 1014, 1022, 1024, 1032, 1038, 1041, 1043, 1046, 1048, 1052, 1058, 106, 1060, 1064, 1072, 1078, 1080, 1099, 1108, 1109, 1114, 1116, 1118, 1127, 1134, 1140, 1149, 1156, 1166, 117, 1171, 1180, 1195, 1198, 135, 149, 155, 157, 163, 165, 166, 167, 169, 170, 172, 175, 178, 207, 212, 217, 226, 235, 243, 25, 251, 260, 261, 264, 266, 268, 269, 271, 272, 281, 282, 288, 3, 313, 315, 320, 321, 329, 330, 333, 340, 343, 348, 353, 357, 367, 371, 376, 381, 39, 393, 399, 419, 420, 43, 437, 44, 441, 446, 449, 45, 452, 457, 46, 470, 488, 489, 49, 494, 50, 502, 513, 526, 53, 531, 54, 542, 557, 568, 574, 581, 582, 583, 592, 595, 596, 605, 608, 609, 622, 624, 631, 635, 638, 640, 65, 653, 656, 66, 667, 67, 681, 686, 688, 689, 69, 692, 694, 7, 702, 720, 725, 727, 732, 741, 745, 750, 754, 755, 756, 759, 774, 782, 783, 796, 8, 828, 83, 830, 837, 84, 85, 856, 862, 873, 880, 881, 884, 885, 897, 899, 9, 912, 914, 916, 92, 922, 924, 925, 926, 930, 931, 932, 936, 939, 941, 954, 966, 972, 974, 992, 999];

db.user_activity_logs.aggregate([
  { $addFields: {
    _cid: { $toInt: { $ifNull: ["$customer_id", { $ifNull: ["$customerId", { $ifNull: ["$customerID", null] }] }] } }
  }},
  { $match: { _cid: { $in: FREE_TIER_CUSTOMER_IDS } } },
  { $group: { _id: "$_cid", event_count: { $sum: 1 } } },
  { $count: "free_tier_cids_with_events" }
]).forEach(printjson);

// 4. Total events in the collection
print("\n=== Total documents in user_activity_logs ===");
print(db.user_activity_logs.countDocuments());