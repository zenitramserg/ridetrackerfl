/**
 * subscribe.js — Netlify serverless function
 * Handles email signups: validates, deduplicates, and writes to Airtable Subscribers table.
 *
 * Uses native fetch (Node 18+) — no npm packages required.
 * Reads AIRTABLE_TOKEN from Netlify environment variables (same token as airtable.js).
 */

const BASE_ID    = 'appp3CTtWpqVcTn6e';
const TABLE_NAME = 'Subscribers';

const CORS_HEADERS = {
  'Access-Control-Allow-Origin':  '*',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Content-Type':                 'application/json',
};

exports.handler = async (event) => {
  // Handle CORS preflight
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 204, headers: CORS_HEADERS, body: '' };
  }

  if (event.httpMethod !== 'POST') {
    return {
      statusCode: 405,
      headers: CORS_HEADERS,
      body: JSON.stringify({ error: 'Method not allowed' }),
    };
  }

  const TOKEN = process.env.AIRTABLE_TOKEN;
  if (!TOKEN) {
    return {
      statusCode: 500,
      headers: CORS_HEADERS,
      body: JSON.stringify({ error: 'Server configuration error' }),
    };
  }

  // Parse and validate body
  let email, source;
  try {
    const body = JSON.parse(event.body || '{}');
    email  = (body.email  || '').trim().toLowerCase();
    source = (body.source || 'website');
  } catch {
    return {
      statusCode: 400,
      headers: CORS_HEADERS,
      body: JSON.stringify({ error: 'Invalid request body' }),
    };
  }

  if (!email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
    return {
      statusCode: 400,
      headers: CORS_HEADERS,
      body: JSON.stringify({ error: 'Invalid email address' }),
    };
  }

  const airtableBase = `https://api.airtable.com/v0/${BASE_ID}/${encodeURIComponent(TABLE_NAME)}`;
  const authHeader   = { Authorization: `Bearer ${TOKEN}` };

  try {
    // ── Check for existing subscriber ──────────────────────────────
    const checkUrl = `${airtableBase}?filterByFormula=${encodeURIComponent(
      `LOWER({Email}) = "${email}"`
    )}&maxRecords=1&fields[]=Email`;

    const checkRes  = await fetch(checkUrl, { headers: authHeader });
    const checkData = await checkRes.json();

    if (!checkRes.ok) {
      console.error('Airtable check error:', checkData);
      throw new Error('Failed to check existing subscribers');
    }

    if (checkData.records && checkData.records.length > 0) {
      return {
        statusCode: 200,
        headers: CORS_HEADERS,
        body: JSON.stringify({ message: 'Already subscribed', alreadySubscribed: true }),
      };
    }

    // ── Create new subscriber ──────────────────────────────────────
    const createRes = await fetch(airtableBase, {
      method:  'POST',
      headers: { ...authHeader, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        records: [{
          fields: {
            Email:        email,
            Source:       source,
            Status:       'active',
            'Signed Up At': new Date().toISOString(),
          },
        }],
      }),
    });

    const createData = await createRes.json();

    if (!createRes.ok) {
      console.error('Airtable create error:', createData);
      throw new Error('Failed to create subscriber record');
    }

    return {
      statusCode: 200,
      headers: CORS_HEADERS,
      body: JSON.stringify({
        message: 'Subscribed successfully',
        id: createData.records[0].id,
      }),
    };

  } catch (err) {
    console.error('Subscribe function error:', err);
    return {
      statusCode: 500,
      headers: CORS_HEADERS,
      body: JSON.stringify({ error: 'Failed to subscribe. Please try again.' }),
    };
  }
};
