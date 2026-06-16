exports.handler = async (event) => {
  const params   = new URLSearchParams(event.rawQueryString);
  const name     = params.get('name')      || 'Ride';
  const dateStr  = params.get('date')      || '';
  const timeStr  = params.get('time')      || '';
  const location = params.get('location')  || '';
  const organizer= params.get('organizer') || '';

  // Parse time e.g. "6:30 AM"
  let hours = 6, minutes = 0;
  if (timeStr) {
    const m = timeStr.match(/(\d+):(\d+)\s*(AM|PM)/i);
    if (m) {
      hours   = parseInt(m[1], 10);
      minutes = parseInt(m[2], 10);
      if (/PM/i.test(m[3]) && hours !== 12) hours += 12;
      if (/AM/i.test(m[3]) && hours === 12) hours = 0;
    }
  }

  // Parse date
  let year = new Date().getFullYear(), month = new Date().getMonth() + 1, day = new Date().getDate();
  if (dateStr) {
    const parts = dateStr.split('-');
    if (parts.length === 3) { year = +parts[0]; month = +parts[1]; day = +parts[2]; }
  }

  const pad  = n => String(n).padStart(2, '0');
  const dtStart = `${year}${pad(month)}${pad(day)}T${pad(hours)}${pad(minutes)}00`;
  const dtEnd   = `${year}${pad(month)}${pad(day)}T${pad(hours + 3)}${pad(minutes)}00`;
  const uid     = `${dtStart}-${Date.now()}@ridetrackerfl.com`;

  const ics = [
    'BEGIN:VCALENDAR',
    'VERSION:2.0',
    'PRODID:-//RideTrackerFL//EN',
    'BEGIN:VEVENT',
    `UID:${uid}`,
    `DTSTART:${dtStart}`,
    `DTEND:${dtEnd}`,
    `SUMMARY:${name}`,
    location  ? `LOCATION:${location}`   : '',
    organizer ? `DESCRIPTION:Organized by ${organizer}. More at ridetrackerfl.com` : 'DESCRIPTION:More at ridetrackerfl.com',
    'END:VEVENT',
    'END:VCALENDAR',
  ].filter(Boolean).join('\r\n');

  const filename = name.replace(/[^a-z0-9]/gi, '_').toLowerCase() + '.ics';

  return {
    statusCode: 200,
    headers: {
      'Content-Type': 'text/calendar; charset=utf-8',
      'Content-Disposition': `attachment; filename="${filename}"`,
    },
    body: ics,
  };
};
