MATCH (n)
DETACH DELETE n;

CREATE
  (c1:Customer {id: 'C1', name: 'C1'})-[:OWNS]->(a1:Account {id: 'A1', accountNo: 'ACC-001'}),
  (c1)-[:OWNS]->(a2:Account {id: 'A2', accountNo: 'ACC-002'}),

  (c2:Customer {id: 'C2', name: 'C2'})-[:OWNS]->(a3:Account {id: 'A3', accountNo: 'ACC-003'}),
  (c3:Customer {id: 'C3', name: 'C3'})-[:OWNS]->(a4:Account {id: 'A4', accountNo: 'ACC-004'}),
  (c4:Customer {id: 'C4', name: 'C4'})-[:OWNS]->(a5:Account {id: 'A5', accountNo: 'ACC-005'}),
  (c5:Customer {id: 'C5', name: 'C5'})-[:OWNS]->(a6:Account {id: 'A6', accountNo: 'ACC-006'}),

  (d1:Device {id: 'D1', deviceId: 'dev-xyz', type: 'Phone'}),
  (d2:Device {id: 'D2', deviceId: 'dev-abc', type: 'Phone'}),
  (d3:Device {id: 'D3', deviceId: 'dev-789', type: 'Tablet'}),

  (ip1:IPAddress {id: 'IP1', address: '10.0.0.10'}),
  (ip2:IPAddress {id: 'IP2', address: '10.0.0.20'}),

  // Money transfers (fraud ring pattern)
  (a1)-[:TRANSFERRED_TO {amount: 2500, currency: 'USD'}]->(a3),
  (a3)-[:TRANSFERRED_TO {amount: 2600, currency: 'USD'}]->(a4),
  (a4)-[:TRANSFERRED_TO {amount: 2700, currency: 'USD'}]->(a5),
  (a5)-[:TRANSFERRED_TO {amount: 2800, currency: 'USD'}]->(a6),
  (a2)-[:TRANSFERRED_TO {amount: 5000, currency: 'USD'}]->(a6),
  (a6)-[:TRANSFERRED_TO {amount: 2400, currency: 'USD'}]->(a1),

  (c1)-[:LOGGED_IN_WITH]->(d1),
  (c2)-[:LOGGED_IN_WITH]->(d1),
  (c3)-[:LOGGED_IN_WITH]->(d2),
  (c4)-[:LOGGED_IN_WITH]->(d2),
  (c5)-[:LOGGED_IN_WITH]->(d3),

  (c1)-[:LOGGED_IN_FROM]->(ip1),
  (c2)-[:LOGGED_IN_FROM]->(ip1),
  (c5)-[:LOGGED_IN_FROM]->(ip1),
  (c3)-[:LOGGED_IN_FROM]->(ip2),
  (c4)-[:LOGGED_IN_FROM]->(ip2);
