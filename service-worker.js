const CACHE_NAME = 'ridetrackerfl-v1';
const RUNTIME_CACHE = 'ridetrackerfl-runtime-v1';

// Assets to cache immediately on install
const STATIC_ASSETS = [
  '/',
  '/index.html',
  '/manifest.json',
  '/icons/icon-192x192.png',
  '/icons/icon-512x512.png'
];

// Install event - cache static assets
self.addEventListener('install', (event) => {
  console.log('[Service Worker] Installing...');
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then((cache) => {
        console.log('[Service Worker] Caching static assets');
        return cache.addAll(STATIC_ASSETS);
      })
      .then(() => self.skipWaiting())
  );
});

// Activate event - cleanup old caches
self.addEventListener('activate', (event) => {
  console.log('[Service Worker] Activating...');
  event.waitUntil(
    caches.keys().then((cacheNames) => {
      return Promise.all(
        cacheNames
          .filter((name) => name !== CACHE_NAME && name !== RUNTIME_CACHE)
          .map((name) => {
            console.log('[Service Worker] Deleting old cache:', name);
            return caches.delete(name);
          })
      );
    }).then(() => self.clients.claim())
  );
});

// Fetch event - Network First for API, Cache First for assets
self.addEventListener('fetch', (event) => {
  const { request } = event;
  const url = new URL(request.url);

  // Skip non-GET requests
  if (request.method !== 'GET') return;

  // API requests (Netlify functions) - Network First
  if (url.pathname.startsWith('/.netlify/functions/')) {
    event.respondWith(
      fetch(request)
        .then((response) => {
          if (response.ok) {
            const responseClone = response.clone();
            caches.open(RUNTIME_CACHE).then((cache) => {
              cache.put(request, responseClone);
            });
          }
          return response;
        })
        .catch(() => {
          return caches.match(request).then((cachedResponse) => {
            if (cachedResponse) {
              console.log('[Service Worker] Serving cached API data (offline)');
              return cachedResponse;
            }
            return new Response(
              JSON.stringify({
                rides: [],
                offline: true,
                message: 'You are offline. Showing cached data.'
              }),
              { headers: { 'Content-Type': 'application/json' } }
            );
          });
        })
    );
    return;
  }

  // Static assets - Cache First, fallback to Network
  event.respondWith(
    caches.match(request).then((cachedResponse) => {
      if (cachedResponse) {
        return cachedResponse;
      }
      return fetch(request).then((response) => {
        if (response.ok) {
          const responseClone = response.clone();
          caches.open(RUNTIME_CACHE).then((cache) => {
            cache.put(request, responseClone);
          });
        }
        return response;
      });
    })
  );
});

// Background sync for offline updates (future enhancement)
self.addEventListener('sync', (event) => {
  if (event.tag === 'sync-rides') {
    console.log('[Service Worker] Background syncing rides...');
    event.waitUntil(
      fetch('/.netlify/functions/airtable?tableId=tbl7xURgDo5wU4z5t')
        .then(response => response.json())
        .then(data => {
          return caches.open(RUNTIME_CACHE).then(cache => {
            cache.put('/.netlify/functions/airtable',
              new Response(JSON.stringify(data))
            );
          });
        })
        .catch(err => console.log('[Service Worker] Sync failed:', err))
    );
  }
});
