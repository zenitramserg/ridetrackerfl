/**
 * rsvp.js
 * Increments the Rider Count on a Rides record when someone taps "I'm Riding".
 *
 * POST /.netlify/functions/rsvp
 * Body (JSON): { recordId: "recXXXXXXXXXXXXXXX" }
 *
 * Returns: { count: <new total> }
 *
 * Uses the same AIRTABLE_TOKEN env var as the main airtable proxy.
 */

exports.handler = async (event) => {
  if (event.httpMethod !== 'POST') {
    return { statusCode: 405, body: JSON.stringify({ error: 'Method not allowed' }) };
  }

  const TOKEN   = process.env.AIRTABLE_TOKEN;
  const BASE_ID = 'appp3CTtWpqVcTn6e';
  const TABLE   = 'tbl7xURgDo5wU4z5t';
  const FIELD   = 'Rider Count';

  if (!TOKEN) {
    return { statusCode: 500, body: JSON.stringify({ error: 'AIRTABLE_TOKEN not configured' }) };
  }

  let recordId;
  try {
    ({ recordId } = JSON.parse(event.body || '{}'));
  } catch {
    return { statusCode: 400, body: JSON.stringify({ error: 'Invalid JSON body' }) };
  }

  if (!recordId || !/^rec[A-Za-z0-9]{14}$/.test(recordId)) {
    return { statusCode: 400, body: JSON.stringify({ error: 'Invalid recordId' }) };
  }

  const headers = {
    Authorization: `Bearer ${TOKEN}`,
    'Content-Type': 'application/json',
  };
  const base = `https://api.airtable.com/v0/${BASE_ID}/${TABLE}/${recordId}`;

  // 1. Fetch current count
  const getRes = await fetch(`${base}?fields[]=${encodeURIComponent(FIELD)}`, { headers });
  if (!getRes.ok) {
    return { statusCode: getRes.status, body: JSON.stringify({ error: 'Failed to fetch record' }) };
  }
  const record     = await getRes.json();
  const current    = record.fields?.[FIELD] || 0;
  const newCount   = current + 1;

  // 2. Patch with incremented count
  const patchRes = await fetch(base, {
    method: 'PATCH',
    headers,
    body: JSON.stringify({ fields: { [FIELD]: newCount } }),
  });
  if (!patchRes.ok) {
    return { statusCode: patchRes.status, body: JSON.stringify({ error: 'Failed to update record' }) };
  }

  return {
    statusCode: 200,
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ count: newCount }),
  };
};
