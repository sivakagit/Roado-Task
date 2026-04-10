// Save as diagnose.js and run it
// use nimbus_events;

// Check how customer_id is actually stored
print("=== Sample raw documents ===");
db.user_activity_logs.find({}, {customer_id: 1, customerId: 1, _id: 0}).limit(5).forEach(printjson);

// Check how many events exist for a known free-tier customer
print("\n=== Events for customer_id 167 (any format) ===");
db.user_activity_logs.find({
  $or: [
    { customer_id: 167 },
    { customer_id: "167" },
    { customerId: 167 },
    { customerId: "167" }
  ]
}).count();

// Check total distinct users in the collection
print("\n=== Total distinct member IDs in collection ===");
db.user_activity_logs.distinct("member_id").length;