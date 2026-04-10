print('=== Field presence stats ===');
print('has feature:', db.user_activity_logs.countDocuments({feature: {$exists: true}}));
print('has no feature:', db.user_activity_logs.countDocuments({feature: {$exists: false}}));

print('\n=== Sample WITH feature field ===');
printjson(db.user_activity_logs.findOne({feature: {$exists: true}}));

print('\n=== Distinct event_types ===');
printjson(db.user_activity_logs.distinct('event_type'));

print('\n=== Free tier CID 167 event count ===');
print(db.user_activity_logs.countDocuments({customer_id: 167}));

print('\n=== Users with more than 1 event (sample) ===');
db.user_activity_logs.aggregate([
  {$group: {_id: {cid: "$customer_id", mid: "$member_id"}, count: {$sum: 1}}},
  {$match: {count: {$gt: 1}}},
  {$sort: {count: -1}},
  {$limit: 5}
]).forEach(printjson);