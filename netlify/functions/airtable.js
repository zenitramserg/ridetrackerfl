exports.handler = async (event) => {
  const TOKEN = process.env.AIRTABLE_TOKEN;
  const BASE_ID = 'appp3CTtWpqVcTn6e';

  if (!TOKEN) {
    return { statusCode: 500, body: JSON.stringify({ error: 'AIRTABLE_TOKEN not configured' }) };
  }

  const rawQuery = event.rawQuery || '';
  const params = new URLSearchParams(rawQuery);
  const tableId = params.get('tableId');
  params.delete('tableId');

  if (!tableId) {
    return { statusCode: 400, body: JSON.stringify({ error: 'tableId is required' }) };
  }

  const url = `https://api.airtable.com/v0/${BASE_ID}/${tableId}?${params.toString()}`;

  try {
    const res = await fetch(url, {
      headers: { Authorization: `Bearer ${TOKEN}` }
    });
    const data = await res.json();
    return {
      statusCode: res.status,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(data)
    };
  } catch (err) {
    return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
  }
};
